package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// listen starts a loopback endpoint that runs handle on every stream it
// accepts for alpn, and returns its ticket.
func listen(t *testing.T, alpn string, handle func(*iroh.Stream)) string {
	t.Helper()
	ep, err := iroh.Bind(context.Background(),
		iroh.WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")),
		iroh.WithoutRelayTransports(),
	)
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}
	t.Cleanup(func() { ep.Shutdown(context.Background()) })
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		for {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return nil
			}
			go handle(stream)
		}
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{alpn: handler}, nil)
	if err != nil {
		t.Fatalf("start router: %v", err)
	}
	t.Cleanup(func() { router.Shutdown(context.Background()) })
	return tool.LocalTicket(ep)
}

func echo(stream *iroh.Stream) { tool.Echo(stream) }

func TestRunValidatesBeforeBind(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want error
	}{
		{name: "unknown flag", args: []string{"-nope"}, want: tool.ErrUsage},
		{name: "help", args: []string{"-h"}, want: flag.ErrHelp},
		{name: "no operand", args: nil},
		{name: "two operands", args: []string{"a", "b"}},
		{name: "bad endpoint", args: []string{"not-a-ticket"}},
		{name: "bad ticket", args: []string{"endpointnotbase32"}},
		{name: "relay conflict", args: []string{"-direct-only", "-staging", "x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := run(ctx, tt.args, eofReader{}, io.Discard)
			if err == nil {
				t.Fatalf("run(%q) = nil, want error", tt.args)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("run(%q) = %v, want %v", tt.args, err, tt.want)
			}
		})
	}
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

// TestEcho sends a payload through an echoing peer. EOF on stdin must
// half-close the stream so the peer finishes, and the whole reply must still
// arrive. The large payload exceeds the stream's flow-control window, which
// deadlocks a bridge that uploads before it reads.
func TestEcho(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{name: "empty", size: 0},
		{name: "small", size: 13},
		{name: "large", size: 1<<20 + 512<<10},
	}
	ticket := listen(t, defaultALPN, echo)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := make([]byte, tt.size)
			for i := range payload {
				payload[i] = byte(rand.N(256))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var out bytes.Buffer
			if err := run(ctx, []string{"-direct-only", ticket}, bytes.NewReader(payload), &out); err != nil {
				t.Fatalf("run: %v", err)
			}
			if !bytes.Equal(out.Bytes(), payload) {
				t.Fatalf("echo returned %d bytes, want the %d sent", out.Len(), len(payload))
			}
		})
	}
}

// TestReplyAfterHalfClose checks that the reply is read after stdin ends: the
// peer reads to EOF before it writes anything.
func TestReplyAfterHalfClose(t *testing.T) {
	ticket := listen(t, defaultALPN, func(stream *iroh.Stream) {
		defer stream.CloseWrite()
		n, _ := io.Copy(io.Discard, stream)
		if n == 5 {
			stream.Write([]byte("done\n"))
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, []string{"-direct-only", ticket}, bytes.NewReader([]byte("hello")), &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := out.String(); got != "done\n" {
		t.Fatalf("reply = %q, want %q", got, "done\n")
	}
}

func TestALPN(t *testing.T) {
	const custom = "iroh-tools/test/1"
	ticket := listen(t, custom, echo)
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{name: "matching", args: []string{"-alpn", custom}},
		{name: "default", args: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			args := append(tt.args, "-direct-only", "-timeout", "5s", ticket)
			var out bytes.Buffer
			err := run(ctx, args, bytes.NewReader([]byte("ping")), &out)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("run(%q) = %v, want error: %t", args, err, tt.wantErr)
			}
			if !tt.wantErr && out.String() != "ping" {
				t.Fatalf("reply = %q, want %q", out.String(), "ping")
			}
		})
	}
}

// TestDialTimeout dials an address that accepts packets and never answers, so
// only -timeout can end the dial.
func TestDialTimeout(t *testing.T) {
	hole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hole.Close()
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	addr, err := netaddr.ParseTransportAddr("ip:" + hole.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	ticket := endpointticket.Encode(netaddr.NewEndpointAddr(key.EndpointID(sk.Public()), addr))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const timeout = 200 * time.Millisecond
	start := time.Now()
	err = run(ctx, []string{"-direct-only", "-timeout", timeout.String(), ticket}, eofReader{}, io.Discard)
	if err == nil {
		t.Fatal("run succeeded against a silent address, want error")
	}
	if ctx.Err() != nil {
		t.Fatalf("run ended by the test deadline, not -timeout: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("run returned after %s, want about %s", elapsed, timeout)
	}
}

// TestCancelDuringTransfer checks that cancellation ends a transfer whose
// stdin never reaches EOF and whose peer never stops sending.
func TestCancelDuringTransfer(t *testing.T) {
	ticket := listen(t, defaultALPN, echo)
	stdin, stdinw := io.Pipe()
	defer stdinw.Close()
	go func() {
		chunk := make([]byte, 4096)
		for {
			if _, err := stdinw.Write(chunk); err != nil {
				return
			}
		}
	}()
	out := &signalWriter{c: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, []string{"-direct-only", ticket}, stdin, out) }()
	select {
	case <-out.c:
	case err := <-errc:
		t.Fatalf("run returned before any data arrived: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no echoed data arrived")
	}
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run = %v, want %v", err, context.Canceled)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
}

// signalWriter discards what it is written and closes c on the first write.
type signalWriter struct {
	c    chan struct{}
	done bool
}

func (w *signalWriter) Write(p []byte) (int, error) {
	if !w.done {
		w.done = true
		close(w.c)
	}
	return len(p), nil
}
