package tool

import (
	"context"
	"net/netip"
	"testing"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func TestParseEndpointTicket(t *testing.T) {
	sk := key.NewSecretKey([32]byte{1})
	want := netaddr.NewEndpointAddr(sk.Public().EndpointID()).WithIP(netip.MustParseAddrPort("127.0.0.1:1234"))
	got, err := ParseEndpoint(endpointticket.Encode(want))
	if err != nil {
		t.Fatalf("ParseEndpoint: %v", err)
	}
	if !got.ID.Equal(want.ID) || got.String() != want.String() {
		t.Fatalf("addr = %s, want %s", got, want)
	}
}

func TestParseEndpointID(t *testing.T) {
	sk := key.NewSecretKey([32]byte{2})
	got, err := ParseEndpoint(sk.Public().EndpointID().String())
	if err != nil {
		t.Fatalf("ParseEndpoint: %v", err)
	}
	if !got.ID.Equal(sk.Public().EndpointID()) {
		t.Fatalf("id = %s, want %s", got.ID, sk.Public().EndpointID())
	}
	if !got.IsEmpty() {
		t.Fatalf("addr = %s, want id-only endpoint", got)
	}
}

func TestLocalTicketRoundTrip(t *testing.T) {
	ep, err := iroh.Bind(context.Background(), iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	defer ep.Shutdown(context.Background())
	got, err := endpointticket.Decode(LocalTicket(ep))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !got.ID.Equal(ep.ID()) {
		t.Fatalf("id = %s, want %s", got.ID, ep.ID())
	}
	if len(got.IPAddrs()) != 1 {
		t.Fatalf("IPAddrs = %v, want one local addr", got.IPAddrs())
	}
}
