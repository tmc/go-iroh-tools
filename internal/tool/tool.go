package tool

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"os/signal"
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
}

// Bind creates an iroh endpoint from f.
//
// -direct-only takes precedence over -relay: it disables relay transports
// outright, while -relay=false merely declines to configure any relay. Both
// leave the endpoint reachable only over direct addresses, but only
// -direct-only says so to the endpoint, so prefer it when that is the intent.
func Bind(ctx context.Context, f BindFlags, opts ...iroh.Option) (*iroh.Endpoint, error) {
	if f.Bind != "" {
		addr, err := netip.ParseAddrPort(f.Bind)
		if err != nil {
			return nil, fmt.Errorf("parse bind address: %w", err)
		}
		opts = append(opts, iroh.WithBindAddr(addr))
	}
	if f.DirectOnly {
		opts = append(opts, iroh.WithoutRelayTransports())
	} else if f.Relay {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("bind endpoint: %w", err)
	}
	return ep, nil
}

// LocalTicket returns an endpoint ticket for ep's local address.
func LocalTicket(ep *iroh.Endpoint) string {
	return endpointticket.Encode(ep.Addr())
}

// WaitRelay waits up to ten seconds for ep to reach its home relay when f
// enables relays, and reports an error if it does not. Listeners call it
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
