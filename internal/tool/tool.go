package tool

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// BindFlags are common endpoint bind flags.
type BindFlags struct {
	Bind       string
	Relay      bool
	DirectOnly bool
	// RelayURLs replaces the default relays with these. Staging selects
	// the staging relays instead. Both need relays enabled.
	RelayURLs []string
	Staging   bool
	// Key names a file holding the endpoint's secret key, in the format
	// gikey generate prints. Without it each run has a new endpoint id.
	Key string
	// QLOG names a directory for qlog traces of each connection, and KeyLog
	// a file to append TLS secrets to in NSS key log format. Both are for
	// debugging; KeyLog compromises the confidentiality of every connection.
	QLOG   string
	KeyLog string
}

// RegisterBindFlags registers common endpoint bind flags on fs.
//
// The default bind address is a loopback ephemeral port, so a ticket minted by
// default advertises a direct address only this host can dial. Reaching the
// endpoint from another host needs either a bind address on an interface that
// host can route to, such as -bind 0.0.0.0:0, or a relay. Binding to a
// wildcard address exposes the endpoint to every network the host is on, so
// the tools make that a choice rather than a default.
func RegisterBindFlags(fs *flag.FlagSet, f *BindFlags) {
	fs.StringVar(&f.Bind, "bind", "127.0.0.1:0", "local UDP address")
	fs.BoolVar(&f.Relay, "relay", true, "use the default iroh relays")
	fs.BoolVar(&f.DirectOnly, "direct-only", false, "disable relay transports (overrides -relay)")
	fs.Func("relay-url", "use this relay instead of the defaults (repeatable)", func(s string) error {
		f.RelayURLs = append(f.RelayURLs, s)
		return nil
	})
	fs.BoolVar(&f.Staging, "staging", false, "use the staging iroh relays")
	fs.StringVar(&f.Key, "key", "", "read the endpoint secret key from `file`")
	fs.StringVar(&f.QLOG, "qlog", "", "write qlog traces to `dir`")
	fs.StringVar(&f.KeyLog, "keylog", "", "append TLS secrets to `file` (debugging only)")
}

// Bind creates an iroh endpoint from f. The returned function shuts the
// endpoint down and releases anything Bind opened for it; the caller must
// call it once it is done with the endpoint.
//
// -direct-only takes precedence over -relay: it disables relay transports
// outright, while -relay=false merely declines to configure any relay. Both
// leave the endpoint reachable only over direct addresses, but only
// -direct-only says so to the endpoint, so prefer it when that is the intent.
// -relay-url and -staging choose which relays to use, so each is an error
// alongside either of those flags or the other.
func Bind(ctx context.Context, f BindFlags, opts ...iroh.Option) (*iroh.Endpoint, func(), error) {
	relayOpt, err := f.relayOption()
	if err != nil {
		return nil, nil, err
	}
	if relayOpt != nil {
		opts = append(opts, relayOpt)
	}
	if f.Bind != "" {
		addr, err := netip.ParseAddrPort(f.Bind)
		if err != nil {
			return nil, nil, fmt.Errorf("parse bind address: %w", err)
		}
		opts = append(opts, iroh.WithBindAddr(addr))
	}
	if f.Key != "" {
		sk, err := ReadSecretKeyFile(f.Key)
		if err != nil {
			return nil, nil, err
		}
		// The endpoint keeps sk, so it must not be cleared here.
		opts = append(opts, iroh.WithSecretKey(sk))
	}
	if f.QLOG != "" {
		// QLOGDir drops traces silently if it cannot create the directory,
		// so create it here, where the failure can be reported.
		if err := os.MkdirAll(f.QLOG, 0o755); err != nil {
			return nil, nil, fmt.Errorf("create qlog directory: %w", err)
		}
		opts = append(opts, iroh.WithQLOG(iroh.QLOGDir(f.QLOG)))
	}
	var keyLog *os.File
	if f.KeyLog != "" {
		keyLog, err = os.OpenFile(f.KeyLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, nil, fmt.Errorf("open key log: %w", err)
		}
		opts = append(opts, iroh.WithKeyLogWriter(keyLog))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		if keyLog != nil {
			keyLog.Close()
		}
		return nil, nil, fmt.Errorf("bind endpoint: %w", err)
	}
	cleanup := func() {
		ep.Shutdown(context.Background())
		if keyLog != nil {
			keyLog.Close()
		}
	}
	return ep, cleanup, nil
}

// relayOption validates the relay flags in f and returns the endpoint option
// they select, or nil when they select none.
func (f BindFlags) relayOption() (iroh.Option, error) {
	choosing := len(f.RelayURLs) > 0 || f.Staging
	switch {
	case len(f.RelayURLs) > 0 && f.Staging:
		return nil, errors.New("-relay-url and -staging are mutually exclusive")
	case choosing && f.DirectOnly:
		return nil, errors.New("-relay-url and -staging need relays; -direct-only disables them")
	case choosing && !f.Relay:
		return nil, errors.New("-relay-url and -staging need relays; -relay=false disables them")
	}
	switch {
	case f.DirectOnly:
		return iroh.WithoutRelayTransports(), nil
	case len(f.RelayURLs) > 0:
		urls := make([]netaddr.RelayURL, 0, len(f.RelayURLs))
		for _, s := range f.RelayURLs {
			u, err := ParseRelayURL(s)
			if err != nil {
				return nil, err
			}
			urls = append(urls, u)
		}
		return iroh.WithRelayMode(relay.ModeCustomURLs(urls...)), nil
	case f.Staging:
		return iroh.WithRelayMode(relay.ModeStaging()), nil
	case f.Relay:
		return iroh.WithRelayMode(relay.ModeDefault()), nil
	}
	return nil, nil
}

// ParseRelayURL parses s as the URL of a relay server: an http or https URL
// with a host. Any path is kept as a prefix under which the relay is served.
func ParseRelayURL(s string) (netaddr.RelayURL, error) {
	u, err := netaddr.ParseRelayURL(s)
	if err != nil {
		return netaddr.RelayURL{}, fmt.Errorf("parse relay URL: %w", err)
	}
	if uu := u.URL(); (uu.Scheme != "http" && uu.Scheme != "https") || uu.Host == "" {
		return netaddr.RelayURL{}, fmt.Errorf("parse relay URL %q: want an http or https URL with a host", s)
	}
	return u, nil
}

// ParseSecretKey reads a secret key from r in the format gikey generate
// prints: the first non-blank line, either a bare key or one labeled
// "secret ". The key may be hex or z-base-32.
func ParseSecretKey(r io.Reader) (key.SecretKey, error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "secret "); ok {
			line = strings.TrimSpace(rest)
		}
		sk, err := key.ParseSecretKey(line)
		if err != nil {
			return key.SecretKey{}, fmt.Errorf("parse secret key: %w", err)
		}
		return sk, nil
	}
	if err := scanner.Err(); err != nil {
		return key.SecretKey{}, fmt.Errorf("read secret key: %w", err)
	}
	return key.SecretKey{}, errors.New("no secret key found")
}

// ReadSecretKeyFile reads a secret key from the named file, as ParseSecretKey.
func ReadSecretKeyFile(name string) (key.SecretKey, error) {
	f, err := os.Open(name)
	if err != nil {
		return key.SecretKey{}, fmt.Errorf("read secret key: %w", err)
	}
	defer f.Close()
	sk, err := ParseSecretKey(f)
	if err != nil {
		return key.SecretKey{}, fmt.Errorf("%s: %w", name, err)
	}
	return sk, nil
}

// LocalTicket returns an endpoint ticket for ep's local address.
func LocalTicket(ep *iroh.Endpoint) string {
	return endpointticket.Encode(ep.Addr())
}

// WaitRelay waits up to ten seconds for ep to reach its home relay when f
// enables relays, whether the default, staging, or custom ones, and reports an error if it does not. Listeners call it
// before publishing a ticket so the advertised relay address is one the
// endpoint has actually reached.
//
// The wait is therefore a hard startup dependency on relay service. A listener
// that should keep working without it can pass -direct-only, which skips the
// wait entirely; direct-only endpoints have no home relay.
func WaitRelay(ctx context.Context, ep *iroh.Endpoint, f BindFlags) error {
	if f.DirectOnly || !f.Relay {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ep.Online(ctx); err != nil {
		return fmt.Errorf("connect home relay: %w", err)
	}
	return nil
}

// ParseEndpoint parses s as an endpoint ticket or as a bare endpoint id in
// either hex or z-base-32. A ticket carries transport addresses; a bare id
// carries none, so reaching it depends on discovery or a relay.
//
// A string that starts with the ticket prefix is only ever read as a ticket:
// its decoding error is reported as-is rather than being replaced by the less
// useful complaint that it is not an endpoint id.
func ParseEndpoint(s string) (netaddr.EndpointAddr, error) {
	addr, err := endpointticket.Decode(s)
	if err == nil {
		return addr, nil
	}
	if !errors.Is(err, endpointticket.ErrMissingPrefix) {
		return netaddr.EndpointAddr{}, fmt.Errorf("parse endpoint ticket: %w", err)
	}
	id, err := ParseEndpointID(s)
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	return netaddr.NewEndpointAddr(id), nil
}

// ParseEndpointID parses s as an endpoint id in hex or z-base-32. Every tool
// that accepts an endpoint id accepts both encodings, so that an id copied
// from any of their outputs can be pasted into any of their inputs.
func ParseEndpointID(s string) (key.EndpointID, error) {
	if id, err := key.ParseEndpointID(s); err == nil {
		return id, nil
	}
	id, err := key.ParseEndpointIDZ32(s)
	if err != nil {
		return key.EndpointID{}, fmt.Errorf("parse endpoint id: %w", err)
	}
	return id, nil
}

// ErrUsage reports a command-line error that the flag package has already
// described on the command's error output. Run exits with status 2 and prints
// nothing more, so a usage mistake is reported once.
var ErrUsage = errors.New("usage error")

// ParseFlags parses args with fs, which must have been created with
// flag.ContinueOnError so that parsing reports errors instead of exiting.
// It passes a help request back as flag.ErrHelp and reports any other parse
// failure as ErrUsage, leaving the exit policy to Run. A caller returns either
// unchanged, which both reports the mistake once and skips the work the flags
// were being parsed for. A runner that parses its own flags this way can be
// called from a test without terminating the test binary.
func ParseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return ErrUsage
	}
	return nil
}

// Run runs f with the command's arguments and a context cancelled by SIGINT or
// SIGTERM. A help request exits zero, ErrUsage exits with status 2, and any
// other error is printed and exits with status 1. The flag package has already
// reported both usage cases, so Run prints nothing more for them.
//
// A command interrupted by one of those signals reports the cancellation as
// success: the operator asked it to stop and it stopped. Commands that need a
// signal to mean failure, such as a probe that never completed, say so with
// their own error rather than by propagating the cancellation.
func Run(f func(context.Context, []string) error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := f(ctx, os.Args[1:])
	if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if errors.Is(err, ErrUsage) {
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
