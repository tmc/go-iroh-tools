// Package irohtest provides a loopback connection for command tests.
//
// Tests that exercise a command's wire behavior need two real endpoints, not a
// mock: the contracts worth testing are about flow control, half-close, and
// deadlines, none of which a fake stream reproduces. Pair gives a test both
// ends of one connection over loopback, with relays disabled so the test never
// depends on relay availability.
package irohtest

import (
	"context"
	"net/netip"
	"testing"

	"github.com/tmc/go-iroh/iroh"
)

// Pair returns the two ends of a connection speaking alpn. The client end is
// the dialer. Both endpoints are shut down when the test finishes.
func Pair(t *testing.T, alpn string) (client, server *iroh.Conn) {
	t.Helper()
	ctx := context.Background()
	loopback := netip.MustParseAddrPort("127.0.0.1:0")

	serverEP, err := iroh.Bind(ctx,
		iroh.WithBindAddr(loopback),
		iroh.WithoutRelayTransports(),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		t.Fatalf("bind server endpoint: %v", err)
	}
	t.Cleanup(func() { serverEP.Shutdown(context.Background()) })

	clientEP, err := iroh.Bind(ctx,
		iroh.WithBindAddr(loopback),
		iroh.WithoutRelayTransports(),
	)
	if err != nil {
		t.Fatalf("bind client endpoint: %v", err)
	}
	t.Cleanup(func() { clientEP.Shutdown(context.Background()) })

	type accepted struct {
		conn *iroh.Conn
		err  error
	}
	accepts := make(chan accepted, 1)
	go func() {
		conn, err := serverEP.Accept(ctx)
		accepts <- accepted{conn, err}
	}()

	client, err = clientEP.Connect(ctx, serverEP.Addr(), alpn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	got := <-accepts
	if got.err != nil {
		t.Fatalf("accept: %v", got.err)
	}
	t.Cleanup(func() { got.conn.Close() })
	return client, got.conn
}
