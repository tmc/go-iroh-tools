package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/irohtest"
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

// TestProbeExitStatus pins the exit policy: some loss is success, total loss is
// failure.
func TestProbeExitStatus(t *testing.T) {
	t.Run("all answered", func(t *testing.T) {
		client, server := irohtest.Pair(t, pingALPN)
		serveWith(t, server, func(stream net.Conn, req []byte) {
			defer stream.Close()
			stream.Write(req)
		})
		if err := probe(context.Background(), client, 3, 0, 5*time.Second); err != nil {
			t.Fatalf("probe: %v", err)
		}
	})

	t.Run("none answered", func(t *testing.T) {
		client, server := irohtest.Pair(t, pingALPN)
		serveWith(t, server, func(stream net.Conn, req []byte) {
			defer stream.Close()
		})
		err := probe(context.Background(), client, 2, 0, 500*time.Millisecond)
		if err == nil {
			t.Fatal("probe succeeded with no replies, want error")
		}
	})

	t.Run("partial loss succeeds", func(t *testing.T) {
		client, server := irohtest.Pair(t, pingALPN)
		var n int
		serveWith(t, server, func(stream net.Conn, req []byte) {
			defer stream.Close()
			n++
			if n == 1 {
				return // drop the first probe
			}
			stream.Write(req)
		})
		if err := probe(context.Background(), client, 3, 0, 2*time.Second); err != nil {
			t.Fatalf("probe with partial loss = %v, want success", err)
		}
	})
}

func TestSummarize(t *testing.T) {
	tests := []struct {
		name     string
		sent     int
		received int
		wantErr  bool
	}{
		{name: "all received", sent: 4, received: 4},
		{name: "partial", sent: 4, received: 1},
		{name: "none received", sent: 4, received: 0, wantErr: true},
		{name: "none sent", sent: 0, received: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := summarize(tt.sent, tt.received, stringer("peer"))
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("summarize(%d, %d) error = %v, want error: %t",
					tt.sent, tt.received, err, tt.wantErr)
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
