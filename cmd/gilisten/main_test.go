package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
)

const testALPN = "iroh-tools/listen-test/1"

// directBind is the flag set the tests listen with: loopback only, no relay,
// so no test depends on relay availability.
var directBind = tool.BindFlags{Bind: "127.0.0.1:0", DirectOnly: true}

func bindEndpoint(t *testing.T) *iroh.Endpoint {
	t.Helper()
	ep, err := iroh.Bind(context.Background(),
		iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")),
		iroh.WithoutRelayTransports(),
	)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	t.Cleanup(func() { ep.Shutdown(context.Background()) })
	return ep
}

// TestServeOnceCancelBeforeConnection is the regression test for a one-shot
// listener that ignored cancellation. With no client at all, cancelling the
// context must end the wait promptly; the old code waited only on a completion
// channel and stayed alive after SIGTERM.
func TestServeOnceCancelBeforeConnection(t *testing.T) {
	ep := bindEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() {
		errc <- serveOnce(ctx, ep, directBind, testALPN, true, io.Discard)
	}()
	time.AfterFunc(100*time.Millisecond, cancel)

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("serveOnce = %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveOnce ignored cancellation with no client connected")
	}
}

// TestServeOnceReadyOnTicket checks the readiness contract: a client that dials
// the instant the ticket appears finds the protocol registered. The ticket used
// to be printed before the router registered its ALPN.
func TestServeOnceReadyOnTicket(t *testing.T) {
	ep := bindEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- serveOnce(ctx, ep, directBind, testALPN, true, pw)
	}()

	ticket := readTicket(t, pr)
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		t.Fatalf("decode ticket: %v", err)
	}

	// Dial immediately, with no settling delay.
	client := bindEndpoint(t)
	dctx, dcancel := context.WithTimeout(ctx, 10*time.Second)
	defer dcancel()
	conn, err := client.Connect(dctx, addr, testALPN)
	if err != nil {
		t.Fatalf("connect immediately after ticket: %v", err)
	}
	defer conn.Close()

	if got := echoOnce(t, conn, "ready"); got != "ready" {
		t.Fatalf("echo = %q, want %q", got, "ready")
	}
	cancel()
	<-served
}

// TestServeOnceAdmitsOneConnection checks that -1 means one connection. A
// second client must not be served while the first holds the listener.
func TestServeOnceAdmitsOneConnection(t *testing.T) {
	ep := bindEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- serveOnce(ctx, ep, directBind, testALPN, true, pw)
	}()
	addr, err := endpointticket.Decode(readTicket(t, pr))
	if err != nil {
		t.Fatalf("decode ticket: %v", err)
	}

	// First client is admitted and served.
	first := bindEndpoint(t)
	firstConn, err := first.Connect(ctx, addr, testALPN)
	if err != nil {
		t.Fatalf("connect first client: %v", err)
	}
	defer firstConn.Close()
	if got := echoOnce(t, firstConn, "first"); got != "first" {
		t.Fatalf("first echo = %q, want %q", got, "first")
	}

	// Second client must not be served while the first is still connected.
	second := bindEndpoint(t)
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	defer dcancel()
	secondConn, err := second.Connect(dctx, addr, testALPN)
	if err != nil {
		return // refused outright, which also satisfies the contract
	}
	defer secondConn.Close()
	stream, err := secondConn.OpenStreamSync(dctx)
	if err != nil {
		return
	}
	if _, err := io.WriteString(stream, "second"); err != nil {
		return
	}
	stream.CloseWrite()
	stream.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := io.ReadAll(stream); err == nil && len(n) > 0 {
		t.Fatalf("second client was served %q; -1 must admit one connection", n)
	}
}

// TestServeConnCancelDuringTransfer checks that an interrupt mid-connection
// ends the listener instead of waiting for the peer.
func TestServeConnCancelDuringTransfer(t *testing.T) {
	ep := bindEndpoint(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- serveOnce(ctx, ep, directBind, testALPN, true, pw)
	}()
	addr, err := endpointticket.Decode(readTicket(t, pr))
	if err != nil {
		t.Fatalf("decode ticket: %v", err)
	}

	client := bindEndpoint(t)
	conn, err := client.Connect(ctx, addr, testALPN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	if got := echoOnce(t, conn, "hello"); got != "hello" {
		t.Fatalf("echo = %q, want %q", got, "hello")
	}

	// The connection is idle but open; cancelling must end the listener.
	cancel()
	select {
	case err := <-served:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serveOnce = %v, want nil or %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveOnce did not return after cancellation")
	}
}

// echoOnce opens a stream, sends msg, and returns what came back.
func echoOnce(t *testing.T, conn *iroh.Conn, msg string) string {
	t.Helper()
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	if _, err := io.WriteString(stream, msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if err := stream.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(got)
}

// readTicket returns the ticket line the listener announces.
func readTicket(t *testing.T, r io.Reader) string {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		scanner := bufio.NewScanner(r)
		if scanner.Scan() {
			ch <- result{strings.TrimSpace(scanner.Text()), nil}
			return
		}
		ch <- result{"", scanner.Err()}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("read ticket: %v", got.err)
		}
		if got.line == "" {
			t.Fatal("listener announced an empty ticket")
		}
		return got.line
	case <-time.After(20 * time.Second):
		t.Fatal("listener never announced a ticket")
		return ""
	}
}
