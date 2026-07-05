package tool

import (
	"context"
	"flag"
	"fmt"
	"net/netip"
	"os"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// BindFlags are common endpoint bind flags.
type BindFlags struct {
	Bind  string
	Relay bool
}

// RegisterBindFlags registers common endpoint bind flags on fs.
func RegisterBindFlags(fs *flag.FlagSet, f *BindFlags) {
	fs.StringVar(&f.Bind, "bind", "127.0.0.1:0", "local UDP address")
	fs.BoolVar(&f.Relay, "relay", false, "enable default iroh relays")
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
	if f.Relay {
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
	addr := netaddr.NewEndpointAddr(ep.ID())
	if local := ep.LocalAddr(); local.IsValid() {
		addr = addr.WithIP(local)
	}
	return addr
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
	if err := f(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
