package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/irohtest"
	"github.com/tmc/go-iroh/iroh"
)

func TestParseBytes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int64
	}{
		{name: "plain", in: "1024", want: 1024},
		{name: "spaces", in: "  2048  ", want: 2048},
		{name: "kib", in: "1KiB", want: 1 << 10},
		{name: "mib", in: "1MiB", want: 1 << 20},
		{name: "gib", in: "1GiB", want: 1 << 30},
		{name: "kb", in: "1kb", want: 1000},
		{name: "mb", in: "1mb", want: 1000 * 1000},
		{name: "gb", in: "1gb", want: 1000 * 1000 * 1000},
		{name: "k", in: "2k", want: 2000},
		{name: "m", in: "2m", want: 2 * 1000 * 1000},
		{name: "g", in: "2g", want: 2 * 1000 * 1000 * 1000},
		{name: "case insensitive", in: "1MIB", want: 1 << 20},
		// "kib" must win over the "k" and "b"-less suffixes that also match.
		{name: "longest suffix wins", in: "1kib", want: 1 << 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBytes(tt.in)
			if err != nil {
				t.Fatalf("parseBytes(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parseBytes(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestParseBytesRejects covers the refusals, including the multiplication that
// used to wrap silently: 17179869184GiB parsed as zero.
func TestParseBytesRejects(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "spaces only", in: "   "},
		{name: "not a number", in: "lots"},
		{name: "suffix only", in: "MiB"},
		{name: "negative", in: "-1"},
		{name: "negative with suffix", in: "-1MiB"},
		{name: "zero", in: "0"},
		{name: "zero with suffix", in: "0MiB"},
		{name: "overflow", in: "17179869184GiB"},
		{name: "max int overflow", in: "9223372036854775807KiB"},
		{name: "above limit", in: "2048GiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBytes(tt.in)
			if err == nil {
				t.Fatalf("parseBytes(%q) = %d, want error", tt.in, got)
			}
		})
	}
}

// TestTransferRoundTrip is the positive control: a completed transfer that
// every refusal test below is measured against.
func TestTransferRoundTrip(t *testing.T) {
	client, server := irohtest.Pair(t, perfALPN)
	servePerf(t, server)

	const size = 1 << 20
	if err := transfer(context.Background(), client, size, 30*time.Second); err != nil {
		t.Fatalf("transfer: %v", err)
	}
}

// TestTransferRejectsBadConfirmation checks that the client refuses to report
// success on a confirmation that does not account for every byte it sent.
func TestTransferRejectsBadConfirmation(t *testing.T) {
	tests := []struct {
		name    string
		confirm func(stream net.Conn, want int64, got int64)
	}{
		{
			name: "withheld",
			confirm: func(stream net.Conn, want, got int64) {
				// Say nothing and close, as a version 1 server would.
			},
		},
		{
			name: "short count",
			confirm: func(stream net.Conn, want, got int64) {
				writeReply(stream, statusOK, got-1)
			},
		},
		{
			name: "inflated count",
			confirm: func(stream net.Conn, want, got int64) {
				writeReply(stream, statusOK, got+1)
			},
		},
		{
			name: "error status",
			confirm: func(stream net.Conn, want, got int64) {
				writeReply(stream, statusShort, got)
			},
		},
		{
			name: "truncated reply",
			confirm: func(stream net.Conn, want, got int64) {
				var reply [replySize]byte
				binary.BigEndian.PutUint64(reply[8:16], uint64(got))
				stream.Write(reply[:replySize-1])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := irohtest.Pair(t, perfALPN)
			go func() {
				stream, err := server.AcceptStreamConn(context.Background())
				if err != nil {
					return
				}
				defer stream.Close()
				var header [headerSize]byte
				if _, err := io.ReadFull(stream, header[:]); err != nil {
					return
				}
				want := int64(binary.BigEndian.Uint64(header[:]))
				got, _ := io.CopyN(io.Discard, stream, want)
				tt.confirm(stream, want, got)
			}()

			err := transfer(context.Background(), client, 4096, 10*time.Second)
			if err == nil {
				t.Fatal("transfer succeeded on a bad confirmation, want error")
			}
		})
	}
}

// TestServeTransferRejectsOversizeHeader checks the receiver refuses a length
// it is unwilling to read rather than trying to consume it.
func TestServeTransferRejectsOversizeHeader(t *testing.T) {
	client, server := irohtest.Pair(t, perfALPN)
	errc := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStreamConn(context.Background())
		if err != nil {
			errc <- err
			return
		}
		errc <- serveTransfer(stream)
	}()

	stream, err := client.OpenStreamConn(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamConn: %v", err)
	}
	defer stream.Close()
	var header [headerSize]byte
	binary.BigEndian.PutUint64(header[:], ^uint64(0))
	if _, err := stream.Write(header[:]); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("serveTransfer accepted an oversize length")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveTransfer did not return")
	}
}

// TestServeTransferConfirmsShortPayload checks that a sender which stops early
// learns the count instead of waiting out its deadline.
func TestServeTransferConfirmsShortPayload(t *testing.T) {
	client, server := irohtest.Pair(t, perfALPN)
	go func() {
		stream, err := server.AcceptStreamConn(context.Background())
		if err != nil {
			return
		}
		serveTransfer(stream)
	}()

	stream, err := client.OpenStreamConn(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamConn: %v", err)
	}
	defer stream.Close()
	const promised, delivered = 4096, 1000
	var header [headerSize]byte
	binary.BigEndian.PutUint64(header[:], promised)
	if _, err := stream.Write(header[:]); err != nil {
		t.Fatalf("Write header: %v", err)
	}
	if _, err := stream.Write(make([]byte, delivered)); err != nil {
		t.Fatalf("Write payload: %v", err)
	}
	if err := stream.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	status, received, err := readReply(stream)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if status != statusShort {
		t.Fatalf("status = %d, want %d", status, statusShort)
	}
	if received != delivered {
		t.Fatalf("received = %d, want %d", received, delivered)
	}
}

// TestTransferMultipleStreams checks that one connection can carry several
// transfers in sequence, so serving a stream does not end the accept loop.
func TestTransferMultipleStreams(t *testing.T) {
	client, server := irohtest.Pair(t, perfALPN)
	servePerf(t, server)
	for i := range 3 {
		if err := transfer(context.Background(), client, 4096, 10*time.Second); err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}
}

// TestTransferStalledReceiver checks that a receiver which never reads does not
// hold the sender past its transfer deadline.
func TestTransferStalledReceiver(t *testing.T) {
	client, server := irohtest.Pair(t, perfALPN)
	hold := make(chan struct{})
	defer close(hold)
	go func() {
		stream, err := server.AcceptStreamConn(context.Background())
		if err != nil {
			return
		}
		defer stream.Close()
		<-hold
	}()

	errc := make(chan error, 1)
	go func() {
		// Larger than the flow-control window, so the write cannot complete
		// while the receiver refuses to read.
		errc <- transfer(context.Background(), client, 64<<20, 300*time.Millisecond)
	}()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("transfer succeeded against a stalled receiver")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("transfer outlived its deadline")
	}
}

// servePerf serves conn with the real receiver implementation.
func servePerf(t *testing.T, conn *iroh.Conn) {
	t.Helper()
	go func() {
		for {
			stream, err := conn.AcceptStreamConn(context.Background())
			if err != nil {
				return
			}
			serveTransfer(stream)
		}
	}()
}
