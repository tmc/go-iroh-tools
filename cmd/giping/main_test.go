package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/irohtest"
	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

// serveWith answers streams on conn with reply, which receives the request it
// read. A reply func that returns without writing stands in for a silent peer.
func serveWith(t *testing.T, conn *iroh.Conn, reply func(stream net.Conn, req []byte)) {
	t.Helper()
	go func() {
		for {
			stream, err := conn.AcceptStreamConn(context.Background())
			if err != nil {
				return
			}
			var req [probeSize]byte
			if _, err := io.ReadFull(stream, req[:]); err != nil {
				stream.Close()
				return
			}
			reply(stream, req[:])
		}
	}()
}

func TestPingSuccess(t *testing.T) {
	client, server := irohtest.Pair(t, pingALPN)
	serveWith(t, server, func(stream net.Conn, req []byte) {
		defer stream.Close()
		stream.Write(req)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ping(ctx, client, 7, 12345); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

// TestPingRejectsBadReplies covers the replies a peer can give that are not the
// echo that was asked for.
func TestPingRejectsBadReplies(t *testing.T) {
	tests := []struct {
		name  string
		reply func(stream net.Conn, req []byte)
	}{
		{
			name: "wrong echo",
			reply: func(stream net.Conn, req []byte) {
				defer stream.Close()
				wrong := make([]byte, probeSize)
				binary.BigEndian.PutUint64(wrong[0:8], ^binary.BigEndian.Uint64(req[0:8]))
				stream.Write(wrong)
			},
		},
		{
			name: "truncated echo",
			reply: func(stream net.Conn, req []byte) {
				defer stream.Close()
				stream.Write(req[:probeSize-1])
			},
		},
		{
			name: "no reply",
			reply: func(stream net.Conn, req []byte) {
				defer stream.Close()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := irohtest.Pair(t, pingALPN)
			serveWith(t, server, tt.reply)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := ping(ctx, client, 1, 2); err == nil {
				t.Fatal("ping succeeded, want error")
			}
		})
	}
}

// TestPingDeadlineCoversRead is the regression test for a probe outliving its
// timeout. The peer reads the request and then holds the stream open without
// replying; ping must return when its context deadline passes, not when the
// peer eventually goes away.
func TestPingDeadlineCoversRead(t *testing.T) {
	client, server := irohtest.Pair(t, pingALPN)
	hold := make(chan struct{})
	defer close(hold)
	serveWith(t, server, func(stream net.Conn, req []byte) {
		<-hold
		stream.Close()
	})

	const timeout = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	errc := make(chan error, 1)
	go func() { errc <- ping(ctx, client, 1, 2) }()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("ping succeeded against a silent peer")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("ping returned after %s, want about %s", elapsed, timeout)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ping outlived its deadline")
	}
}

// TestPingCancelDuringIO checks that cancelling before the deadline also
// unblocks the probe.
func TestPingCancelDuringIO(t *testing.T) {
	client, server := irohtest.Pair(t, pingALPN)
	hold := make(chan struct{})
	defer close(hold)
	serveWith(t, server, func(stream net.Conn, req []byte) {
		<-hold
		stream.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- ping(ctx, client, 1, 2) }()
	time.AfterFunc(100*time.Millisecond, cancel)
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("ping succeeded, want cancellation error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ping did not return after cancellation")
	}
}

// echo answers every probe.
func echo(stream net.Conn, req []byte) {
	defer stream.Close()
	stream.Write(req)
}

// TestProbeExitStatus pins the exit policy: some loss is success, total loss is
// failure.
func TestProbeExitStatus(t *testing.T) {
	tests := []struct {
		name         string
		count        int
		timeout      time.Duration
		dropFirst    bool
		silent       bool
		wantReceived int
		wantErr      bool
	}{
		{name: "all answered", count: 3, timeout: 5 * time.Second, wantReceived: 3},
		{name: "none answered", count: 2, timeout: 500 * time.Millisecond, silent: true, wantErr: true},
		{name: "partial loss succeeds", count: 3, timeout: 2 * time.Second, dropFirst: true, wantReceived: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := irohtest.Pair(t, pingALPN)
			var n int
			serveWith(t, server, func(stream net.Conn, req []byte) {
				n++
				if tt.silent || tt.dropFirst && n == 1 {
					stream.Close()
					return
				}
				echo(stream, req)
			})
			var lines bytes.Buffer
			r := probe(context.Background(), client, tt.count, 0, tt.timeout, &lines)
			if r.sent != tt.count || r.received != tt.wantReceived || len(r.rtts) != tt.wantReceived {
				t.Fatalf("sent, received, rtts = %d, %d, %d; want %d, %d, %d",
					r.sent, r.received, len(r.rtts), tt.count, tt.wantReceived, tt.wantReceived)
			}
			if got := strings.Count(lines.String(), "\n"); got != tt.count {
				t.Fatalf("probe printed %d lines, want %d:\n%s", got, tt.count, lines.String())
			}
			if err := r.err(); (err != nil) != tt.wantErr {
				t.Fatalf("err() = %v, want error: %t", err, tt.wantErr)
			}
		})
	}
}

// TestProbeLines pins the per-probe text format.
func TestProbeLines(t *testing.T) {
	client, server := irohtest.Pair(t, pingALPN)
	serveWith(t, server, echo)
	var lines bytes.Buffer
	probe(context.Background(), client, 2, 0, 5*time.Second, &lines)
	re := regexp.MustCompile(`^seq=0 time=[0-9.]+[µm]?s\nseq=1 time=[0-9.]+[µm]?s\n$`)
	if !re.MatchString(lines.String()) {
		t.Fatalf("probe lines = %q, want %s", lines.String(), re)
	}
}

func TestSummary(t *testing.T) {
	tests := []struct {
		name     string
		sent     int
		received int
		want     string
		wantErr  bool
	}{
		{name: "all received", sent: 4, received: 4, want: "4 sent, 4 received, 0% loss\n"},
		{name: "partial", sent: 4, received: 1, want: "4 sent, 1 received, 75% loss\n"},
		{name: "none received", sent: 4, received: 0, want: "4 sent, 0 received, 100% loss\n", wantErr: true},
		{name: "none sent", sent: 0, received: 0, want: "0 sent, 0 received, 100% loss\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := result{remote: stringer("peer"), probes: 4, sent: tt.sent, received: tt.received}
			var out bytes.Buffer
			if err := r.writeSummary(&out); err != nil {
				t.Fatal(err)
			}
			if out.String() != tt.want {
				t.Errorf("summary = %q, want %q", out.String(), tt.want)
			}
			err := r.err()
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("result{sent: %d, received: %d}.err() = %v, want error: %t",
					tt.sent, tt.received, err, tt.wantErr)
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	tests := []struct {
		name string
		r    result
		want string
	}{
		{
			name: "answered",
			r: result{
				remote: stringer("peer"), probes: 4, sent: 4, received: 3,
				rtts: []time.Duration{3 * time.Millisecond, time.Millisecond, 2 * time.Millisecond},
			},
			want: `{"remote":"peer","probes":4,"sent":4,"received":3,"loss_pct":25,"min_ns":1000000,"avg_ns":2000000,"max_ns":3000000}` + "\n",
		},
		{
			name: "none received",
			r:    result{remote: stringer("peer"), probes: 2, sent: 2},
			want: `{"remote":"peer","probes":2,"sent":2,"received":0,"loss_pct":100}` + "\n",
		},
		{
			name: "cancelled before sending",
			r:    result{remote: stringer("peer"), probes: 3},
			want: `{"remote":"peer","probes":3,"sent":0,"received":0,"loss_pct":100}` + "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := tt.r.writeJSON(&out); err != nil {
				t.Fatal(err)
			}
			if out.String() != tt.want {
				t.Fatalf("writeJSON =\n%s\nwant\n%s", out.String(), tt.want)
			}
		})
	}
}

// listener binds a loopback endpoint that serves the ping ALPN with serve,
// and returns its ticket.
func listener(t *testing.T, serve func(stream net.Conn)) string {
	t.Helper()
	ep, closeEndpoint, err := tool.Bind(context.Background(), tool.BindFlags{Bind: "127.0.0.1:0", DirectOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeEndpoint)
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		for {
			stream, err := conn.AcceptStreamConn(ctx)
			if err != nil {
				return nil
			}
			serve(stream)
		}
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{pingALPN: handler}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { router.Shutdown(context.Background()) })
	return tool.LocalTicket(ep)
}

// TestRun runs giping end to end against a loopback listener, in text and
// -json modes, and checks both the output and the exit contract.
func TestRun(t *testing.T) {
	answer := func(stream net.Conn) { serveProbe(stream) }
	silent := func(stream net.Conn) {
		defer stream.Close()
		var buf [probeSize]byte
		io.ReadFull(stream, buf[:])
	}
	tests := []struct {
		name    string
		serve   func(net.Conn)
		json    bool
		want    string // regexp for text mode
		wantErr bool
	}{
		{
			name:  "text",
			serve: answer,
			want:  `^seq=0 time=[0-9.]+[µm]?s\nseq=1 time=[0-9.]+[µm]?s\n2 sent, 2 received, 0% loss\n$`,
		},
		{name: "json", serve: answer, json: true},
		{name: "json no reply", serve: silent, json: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticket := listener(t, tt.serve)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			args := []string{"-direct-only", "-bind", "127.0.0.1:0", "-c", "2", "-i", "0", "-timeout", "2s"}
			if tt.json {
				args = append(args, "-json")
			}
			var out bytes.Buffer
			err := run(ctx, append(args, ticket), &out)
			if (err != nil) != tt.wantErr {
				t.Fatalf("run = %v, want error: %t", err, tt.wantErr)
			}
			if !tt.json {
				if re := regexp.MustCompile(tt.want); !re.MatchString(out.String()) {
					t.Fatalf("output = %q, want %s", out.String(), re)
				}
				return
			}
			if strings.Count(out.String(), "\n") != 1 {
				t.Fatalf("-json printed %q, want one line", out.String())
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &raw); err != nil {
				t.Fatalf("decode %q: %v", out.String(), err)
			}
			var v resultJSON
			if err := json.Unmarshal(out.Bytes(), &v); err != nil {
				t.Fatal(err)
			}
			addr, err := tool.ParseEndpoint(ticket)
			if err != nil {
				t.Fatal(err)
			}
			if v.Remote != addr.ID.String() || v.Probes != 2 || v.Sent != 2 {
				t.Fatalf("result = %+v, want 2 probes sent to %s", v, addr.ID)
			}
			if tt.wantErr {
				if v.Received != 0 || v.LossPct != 100 {
					t.Fatalf("result = %+v, want none received", v)
				}
				for _, name := range []string{"min_ns", "avg_ns", "max_ns"} {
					if _, ok := raw[name]; ok {
						t.Errorf("%s present with nothing received: %s", name, out.String())
					}
				}
				return
			}
			if v.Received != 2 || v.LossPct != 0 {
				t.Fatalf("result = %+v, want 2 received", v)
			}
			if v.MinNS == nil || v.AvgNS == nil || v.MaxNS == nil || *v.MinNS <= 0 || *v.MinNS > *v.AvgNS || *v.AvgNS > *v.MaxNS {
				t.Fatalf("rtts min, avg, max = %v, %v, %v; want 0 < min <= avg <= max", v.MinNS, v.AvgNS, v.MaxNS)
			}
		})
	}
}

// TestServeProbeRejectsTruncated checks the server treats a short request as an
// error for that stream rather than echoing garbage.
func TestServeProbeRejectsTruncated(t *testing.T) {
	client, server := irohtest.Pair(t, pingALPN)
	errc := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStreamConn(context.Background())
		if err != nil {
			errc <- err
			return
		}
		errc <- serveProbe(stream)
	}()

	stream, err := client.OpenStreamConn(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamConn: %v", err)
	}
	if _, err := stream.Write(make([]byte, probeSize-1)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	select {
	case err := <-errc:
		if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("serveProbe = %v, want %v", err, io.ErrUnexpectedEOF)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveProbe did not return")
	}
}

type stringer string

func (s stringer) String() string { return string(s) }

// TestRunValidatesBeforeBind checks that run rejects bad arguments and returns
// the error instead of exiting, which is what makes the runner testable, and
// that it does so before opening an endpoint.
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
		{name: "operand with -listen", args: []string{"-listen", "a"}},
		{name: "zero count", args: []string{"-c", "0", "x"}},
		{name: "negative interval", args: []string{"-i", "-1s", "x"}},
		{name: "zero timeout", args: []string{"-timeout", "0", "-listen"}},
		{name: "json with -listen", args: []string{"-json", "-listen"}},
		{name: "bad endpoint", args: []string{"-c", "1", "not-a-ticket"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := run(ctx, tt.args, io.Discard)
			if err == nil {
				t.Fatalf("run(%q) = nil, want error", tt.args)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("run(%q) = %v, want %v", tt.args, err, tt.want)
			}
		})
	}
}
