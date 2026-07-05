package tool

import (
	"context"
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
func RegisterBindFlags(fs *flag.FlagSet, f *BindFlags) {
	fs.StringVar(&f.Bind, "bind", "127.0.0.1:0", "local UDP address")
	fs.BoolVar(&f.Relay, "relay", true, "enable default iroh relays")
	fs.BoolVar(&f.DirectOnly, "direct-only", false, "disable relay transports")
}

// Bind creates an iroh endpoint from f.
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
	return endpointticket.Encode(LocalEndpointAddr(ep))
}

// LocalEndpointAddr returns ep's local endpoint address.
func LocalEndpointAddr(ep *iroh.Endpoint) netaddr.EndpointAddr {
	return ep.Addr()
}

// WaitRelay waits briefly for ep to connect to its home relay when f enables
// relays. Direct-only endpoints do not have a home relay.
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

// ParseEndpoint parses s as an endpoint ticket, endpoint id, or endpoint addr.
func ParseEndpoint(s string) (netaddr.EndpointAddr, error) {
	if addr, err := endpointticket.Decode(s); err == nil {
		return addr, nil
	}
	id, err := key.ParseEndpointID(s)
	if err != nil {
		return netaddr.EndpointAddr{}, fmt.Errorf("parse endpoint: %w", err)
	}
	return netaddr.NewEndpointAddr(id), nil
}

// Run runs f and exits with status 1 on error.
func Run(f func(context.Context) error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := f(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
