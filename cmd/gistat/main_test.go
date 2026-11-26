package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/irohtest"
	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

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
		{name: "json with -listen", args: []string{"-json", "-listen"}},
		{name: "zero count", args: []string{"-n", "0", "x"}},
		{name: "negative interval", args: []string{"-i", "-1s", "x"}},
		{name: "zero timeout", args: []string{"-timeout", "0", "x"}},
		{name: "bad endpoint", args: []string{"not-a-ticket"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := run(ctx, tt.args, io.Discard, io.Discard)
			if err == nil {
				t.Fatalf("run(%q) = nil, want error", tt.args)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("run(%q) = %v, want %v", tt.args, err, tt.want)
			}
		})
	}
}

// TestPathOmission checks that a field the transport did not report is left
// out of both forms, while one reported as zero is printed as zero.
func TestPathOmission(t *testing.T) {
	addr, err := netaddr.ParseTransportAddr("ip:127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		path     iroh.PathInfo
		wantText string
		wantJSON string
	}{
		{
			name:     "none reported",
			path:     iroh.PathInfo{ID: 1, Selected: true},
			wantText: "path\tid=1 selected=true validated=false relayed=false\n",
			wantJSON: `{"id":1,"selected":true,"validated":false,"relayed":false}`,
		},
		{
			name: "all reported as zero",
			path: iroh.PathInfo{
				Validated:           true,
				Addr:                addr,
				HasAddr:             true,
				HasRTT:              true,
				HasBytesInFlight:    true,
				HasBytesSent:        true,
				HasBytesReceived:    true,
				HasCongestionWindow: true,
				HasLoss:             true,
			},
			wantText: "path\tid=0 selected=false validated=true relayed=false addr=ip:127.0.0.1:9 rtt=0s sent=0 received=0 in-flight=0 cwnd=0 lost-packets=0 lost-bytes=0\n",
			wantJSON: `{"id":0,"selected":false,"validated":true,"relayed":false,"addr":"ip:127.0.0.1:9","rtt_ns":0,"bytes_sent":0,"bytes_received":0,"bytes_in_flight":0,"congestion_window":0,"lost_packets":0,"lost_bytes":0}`,
		},
		{
			name: "values ignored without has",
			path: iroh.PathInfo{
				ID:               2,
				Relayed:          true,
				RTT:              time.Millisecond,
				BytesSent:        10,
				CongestionWindow: 20,
				LostPackets:      3,
			},
			wantText: "path\tid=2 selected=false validated=false relayed=true\n",
			wantJSON: `{"id":2,"selected":false,"validated":false,"relayed":true}`,
		},
		{
			name: "values reported",
			path: iroh.PathInfo{
				ID:                  3,
				Selected:            true,
				Validated:           true,
				RTT:                 1500 * time.Microsecond,
				HasRTT:              true,
				BytesSent:           100,
				HasBytesSent:        true,
				BytesReceived:       200,
				HasBytesReceived:    true,
				BytesInFlight:       5,
				HasBytesInFlight:    true,
				CongestionWindow:    12000,
				HasCongestionWindow: true,
				LostPackets:         1,
				LostBytes:           1200,
				HasLoss:             true,
			},
			wantText: "path\tid=3 selected=true validated=true relayed=false rtt=1.5ms sent=100 received=200 in-flight=5 cwnd=12000 lost-packets=1 lost-bytes=1200\n",
			wantJSON: `{"id":3,"selected":true,"validated":true,"relayed":false,"rtt_ns":1500000,"bytes_sent":100,"bytes_received":200,"bytes_in_flight":5,"congestion_window":12000,"lost_packets":1,"lost_bytes":1200}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var text bytes.Buffer
			if err := printPath(&text, tt.path); err != nil {
				t.Fatal(err)
			}
			if got := text.String(); got != tt.wantText {
				t.Errorf("printPath:\ngot  %q\nwant %q", got, tt.wantText)
			}
			got, err := json.Marshal(newPathsJSON([]iroh.PathInfo{tt.path}))
			if err != nil {
				t.Fatal(err)
			}
			if want := "[" + tt.wantJSON + "]"; string(got) != want {
				t.Errorf("json:\ngot  %s\nwant %s", got, want)
			}
		})
	}
}

func TestPathsJSONNeverNull(t *testing.T) {
	got, err := json.Marshal(connJSON{Paths: newPathsJSON(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"paths":[]`)) {
		t.Fatalf("json = %s, want \"paths\":[]", got)
	}
}

func TestStatsJSON(t *testing.T) {
	s := iroh.ConnStats{
		MinRTT:          time.Millisecond,
		LatestRTT:       2 * time.Millisecond,
		SmoothedRTT:     3 * time.Millisecond,
		MeanDeviation:   4 * time.Millisecond,
		BytesSent:       5,
		PacketsSent:     6,
		BytesReceived:   7,
		PacketsReceived: 8,
		BytesLost:       9,
		PacketsLost:     10,
		PTOs:            11,
		SpuriousLosses:  12,
	}
	got, err := json.Marshal(newStatsJSON(s))
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"rtt_min_ns":1000000,"rtt_latest_ns":2000000,"rtt_smoothed_ns":3000000,"rtt_mean_deviation_ns":4000000,"bytes_sent":5,"bytes_received":7,"packets_sent":6,"packets_received":8,"bytes_lost":9,"packets_lost":10,"ptos":11,"spurious_losses":12}`
	if string(got) != want {
		t.Fatalf("json:\ngot  %s\nwant %s", got, want)
	}
}

// listen runs gistat -listen and returns its ticket and a channel that
// receives run's result once ctx is done.
func listen(ctx context.Context, t *testing.T) (string, <-chan error) {
	t.Helper()
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := run(ctx, []string{"-listen", "-direct-only"}, io.Discard, pw)
		pw.CloseWithError(err)
		errc <- err
	}()
	ticket, err := bufio.NewReader(pr).ReadString('\n')
	if err != nil {
		t.Fatalf("read ticket: %v", err)
	}
	go io.Copy(io.Discard, pr)
	return strings.TrimSpace(ticket), errc
}

func TestListenCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, errc := listen(ctx, t)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run -listen = %v, want %v", err, context.Canceled)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run -listen did not return after cancellation")
	}
}

// textFields is the order of the fixed lines in a text record. Path lines
// follow them.
var textFields = []string{
	"remote", "alpn", "local", "remote-addr", "multipath",
	"rtt-min", "rtt-latest", "rtt-smoothed",
	"bytes-sent", "bytes-received", "packets-sent", "packets-received",
	"bytes-lost", "packets-lost",
}

func TestSampleText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ticket, _ := listen(ctx, t)
	addr, err := tool.ParseEndpoint(ticket)
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := run(ctx, []string{"-direct-only", "-n", "2", "-i", "10ms", ticket}, &out, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	s := out.String()
	if !strings.HasSuffix(s, "\n") || strings.HasSuffix(s, "\n\n") {
		t.Fatalf("output must end in exactly one newline:\n%s", s)
	}
	records := strings.Split(strings.TrimSuffix(s, "\n"), "\n\n")
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2:\n%s", len(records), s)
	}
	for i, rec := range records {
		lines := strings.Split(rec, "\n")
		if len(lines) < len(textFields) {
			t.Fatalf("record %d has %d lines, want at least %d:\n%s", i, len(lines), len(textFields), rec)
		}
		values := map[string]string{}
		for j, line := range lines {
			name, value, ok := strings.Cut(line, "\t")
			if !ok {
				t.Fatalf("record %d line %q is not name<TAB>value", i, line)
			}
			want := "path"
			if j < len(textFields) {
				want = textFields[j]
			}
			if name != want {
				t.Fatalf("record %d line %d is %q, want %q", i, j, name, want)
			}
			values[name] = value
		}
		if got, want := values["remote"], addr.ID.String(); got != want {
			t.Errorf("record %d remote = %q, want %q", i, got, want)
		}
		if got := values["alpn"]; got != statALPN {
			t.Errorf("record %d alpn = %q, want %q", i, got, statALPN)
		}
	}
}

func TestSampleJSON(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ticket, _ := listen(ctx, t)
	addr, err := tool.ParseEndpoint(ticket)
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := run(ctx, []string{"-direct-only", "-json", "-n", "2", "-i", "10ms", ticket}, &out, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	lines := strings.SplitAfter(out.String(), "\n")
	if len(lines) != 3 || lines[2] != "" {
		t.Fatalf("got %q, want two newline-terminated lines", out.String())
	}
	for i, line := range lines[:2] {
		if !strings.Contains(line, `"paths":[`) {
			t.Errorf("line %d has no paths array: %s", i, line)
		}
		dec := json.NewDecoder(strings.NewReader(line))
		dec.DisallowUnknownFields()
		var c connJSON
		if err := dec.Decode(&c); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if c.Remote != addr.ID.String() {
			t.Errorf("line %d remote = %q, want %q", i, c.Remote, addr.ID.String())
		}
		if c.ALPN != statALPN {
			t.Errorf("line %d alpn = %q, want %q", i, c.ALPN, statALPN)
		}
		if c.Local == "" || c.RemoteAddr == "" {
			t.Errorf("line %d local = %q, remote_addr = %q, want both set", i, c.Local, c.RemoteAddr)
		}
	}
}

// TestSampleStopsWhenPeerCloses checks that sample ends without error when
// the peer goes away, after the records it has already printed. The interval
// is long enough that only the closing connection can end the run.
func TestSampleStopsWhenPeerCloses(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		name := "text"
		if asJSON {
			name = "json"
		}
		t.Run(name, func(t *testing.T) {
			client, server := irohtest.Pair(t, statALPN)
			out := &signalBuffer{c: make(chan struct{})}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errc := make(chan error, 1)
			go func() { errc <- sample(ctx, out, client, 100, time.Hour, asJSON) }()
			select {
			case <-out.c:
			case err := <-errc:
				t.Fatalf("sample returned before printing: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("sample printed nothing")
			}
			server.Close()
			select {
			case err := <-errc:
				if err != nil {
					t.Fatalf("sample = %v, want nil", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("sample did not stop after the peer closed")
			}
			s := out.String()
			if asJSON {
				if n := strings.Count(s, "\n"); n != 1 {
					t.Fatalf("got %d JSON records, want 1:\n%s", n, s)
				}
			} else if strings.Contains(s, "\n\n") {
				t.Fatalf("got more than one text record:\n%s", s)
			}
		})
	}
}

// signalBuffer is a bytes.Buffer safe for concurrent use that closes c on its
// first write.
type signalBuffer struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	c    chan struct{}
	once sync.Once
}

func (b *signalBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.once.Do(func() { close(b.c) })
	return b.buf.Write(p)
}

func (b *signalBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
