package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/netaddr"
)

// direct are the bind flags every test runs with: loopback only, no relay.
var direct = []string{"-bind", "127.0.0.1:0", "-direct-only"}

// lineWriter collects writes and delivers the first complete line on first.
type lineWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	sent  bool
	first chan string
}

func newLineWriter() *lineWriter { return &lineWriter{first: make(chan string, 1)} }

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	if line, _, ok := strings.Cut(w.buf.String(), "\n"); ok && !w.sent {
		w.sent = true
		w.first <- line
	}
	return len(p), nil
}

// server is a running giblob -serve.
type server struct {
	ticket string
	cancel context.CancelFunc
	errc   chan error
}

// startServer runs giblob -serve with args and waits for its ticket. The
// server is cancelled and drained when the test ends.
func startServer(t *testing.T, stdin io.Reader, args ...string) *server {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	stderr := newLineWriter()
	s := &server{cancel: cancel, errc: make(chan error, 1)}
	go func() {
		s.errc <- run(ctx, append(append([]string{"-serve"}, direct...), args...), stdin, io.Discard, stderr)
	}()
	t.Cleanup(func() {
		cancel()
		<-s.errc
	})
	select {
	case s.ticket = <-stderr.first:
	case err := <-s.errc:
		s.errc <- err
		t.Fatalf("server exited before printing a ticket: %v", err)
	case <-ctx.Done():
		t.Fatal("server printed no ticket")
	}
	return s
}

// wait returns the server's exit error, failing if it does not exit.
func (s *server) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.errc:
		s.errc <- err
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("server did not exit")
		return nil
	}
}

// runFetch runs giblob without -serve, writing the blob to stdout.
func runFetch(t *testing.T, stdout io.Writer, args ...string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return run(ctx, append(append([]string{}, direct...), args...), strings.NewReader(""), stdout, io.Discard)
}

// payload returns n non-uniform bytes.
func payload(n int) []byte {
	b := make([]byte, n)
	x := uint32(2463534242)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

func TestTransfer(t *testing.T) {
	tests := []struct {
		name   string
		size   int
		stdin  bool // serve standard input instead of a file
		store  bool // pass -store
		toFile bool // fetch with -o
	}{
		{name: "file to stdout", size: 1000},
		{name: "file to -o", size: 1000, toFile: true},
		{name: "stdin to stdout", size: 1000, stdin: true},
		{name: "stdin to -o", size: 1000, stdin: true, toFile: true},
		{name: "file with -store", size: 5000, store: true},
		{name: "stdin with -store", size: 5000, stdin: true, store: true},
		{name: "large file", size: 3<<20 + 17, toFile: true},
		{name: "large stdin", size: 3<<20 + 17, stdin: true},
		{name: "empty", size: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := payload(tt.size)
			dir := t.TempDir()
			var args []string
			if tt.store {
				args = append(args, "-store", filepath.Join(dir, "store"))
			}
			var stdin io.Reader = strings.NewReader("")
			if tt.stdin {
				stdin = bytes.NewReader(data)
			} else {
				src := filepath.Join(dir, "src")
				if err := os.WriteFile(src, data, 0o644); err != nil {
					t.Fatal(err)
				}
				args = append(args, src)
			}
			srv := startServer(t, stdin, args...)

			var got []byte
			if tt.toFile {
				outDir := t.TempDir()
				out := filepath.Join(outDir, "out")
				if err := runFetch(t, io.Discard, "-o", out, srv.ticket); err != nil {
					t.Fatalf("fetch: %v", err)
				}
				var err error
				if got, err = os.ReadFile(out); err != nil {
					t.Fatal(err)
				}
				if names := dirNames(t, outDir); len(names) != 1 {
					t.Errorf("output directory holds %q, want only out", names)
				}
			} else {
				var buf bytes.Buffer
				if err := runFetch(t, &buf, srv.ticket); err != nil {
					t.Fatalf("fetch: %v", err)
				}
				got = buf.Bytes()
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("fetched %d bytes, want %d identical bytes", len(got), len(data))
			}
		})
	}
}

func TestServeOnce(t *testing.T) {
	data := payload(4096)
	srv := startServer(t, bytes.NewReader(data), "-1")
	var buf bytes.Buffer
	if err := runFetch(t, &buf, srv.ticket); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("fetched data differs")
	}
	if err := srv.wait(t); err != nil {
		t.Fatalf("server after one fetch = %v, want nil", err)
	}
}

func TestServeMany(t *testing.T) {
	data := payload(4096)
	srv := startServer(t, bytes.NewReader(data))
	for i := range 2 {
		var buf bytes.Buffer
		if err := runFetch(t, &buf, srv.ticket); err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		if !bytes.Equal(buf.Bytes(), data) {
			t.Fatalf("fetch %d: data differs", i)
		}
	}
	srv.cancel()
	if err := srv.wait(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("server after cancel = %v, want %v", err, context.Canceled)
	}
}

func TestFetchErrors(t *testing.T) {
	srv := startServer(t, bytes.NewReader(payload(100)))
	good, err := blobs.ParseTicket(srv.ticket)
	if err != nil {
		t.Fatal(err)
	}
	other := blobs.NewHash([]byte("not served"))
	tests := []struct {
		name   string
		ticket string
		want   string
	}{
		{name: "garbage", ticket: "blobnotaticket", want: "parse blob ticket"},
		{name: "endpoint ticket", ticket: endpointticket.Encode(good.Addr()), want: "parse blob ticket"},
		{name: "hash sequence", ticket: blobs.NewTicket(good.Addr(), good.Hash(), blobs.HashSeq).String(), want: "only raw blobs"},
		{name: "wrong hash", ticket: blobs.NewTicket(good.Addr(), other, blobs.Raw).String(), want: "fetch " + other.String()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			err := runFetch(t, io.Discard, "-timeout", "5s", "-o", filepath.Join(dir, "out"), tt.ticket)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("fetch = %v, want error containing %q", err, tt.want)
			}
			if names := dirNames(t, dir); len(names) != 0 {
				t.Errorf("failed fetch left %q", names)
			}
		})
	}
}

// TestFetchTimeout dials a socket that never answers and checks that
// -timeout bounds the dial and leaves no output file behind.
func TestFetchTimeout(t *testing.T) {
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	id := startServer(t, bytes.NewReader(payload(10))).ticket
	good, err := blobs.ParseTicket(id)
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(good.Addr().ID).WithIP(silent.LocalAddr().(*net.UDPAddr).AddrPort())
	ticket := blobs.NewTicket(addr, good.Hash(), blobs.Raw).String()

	dir := t.TempDir()
	start := time.Now()
	err = runFetch(t, io.Discard, "-timeout", "300ms", "-o", filepath.Join(dir, "out"), ticket)
	if err == nil || !strings.Contains(err.Error(), "timed out after 300ms") {
		t.Fatalf("fetch = %v, want timeout error", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("fetch took %s to time out", d)
	}
	if names := dirNames(t, dir); len(names) != 0 {
		t.Errorf("timed-out fetch left %q", names)
	}
}

func TestUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "-1 without -serve", args: []string{"-1", "t"}, want: "-1 applies only with -serve"},
		{name: "-short without -serve", args: []string{"-short", "t"}, want: "-short applies only with -serve"},
		{name: "-store without -serve", args: []string{"-store", "d", "t"}, want: "-store applies only with -serve"},
		{name: "-o with -serve", args: []string{"-serve", "-o", "x"}, want: "-o applies only when fetching"},
		{name: "-short without relay", args: []string{"-serve", "-short", "-direct-only"}, want: "needs a relay"},
		{name: "two files", args: []string{"-serve", "a", "b"}, want: `unexpected argument "b"`},
		{name: "no ticket", args: nil, want: "usage: giblob"},
		{name: "bad timeout", args: []string{"-timeout", "0s", "t"}, want: "-timeout must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args, strings.NewReader(""), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run(%q) = %v, want error containing %q", tt.args, err, tt.want)
			}
		})
	}
	err := run(context.Background(), []string{"-nosuchflag"}, strings.NewReader(""), io.Discard, io.Discard)
	if !errors.Is(err, tool.ErrUsage) {
		t.Fatalf("unknown flag = %v, want %v", err, tool.ErrUsage)
	}
}

func TestServeMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	err := run(context.Background(), append(append([]string{"-serve"}, direct...), missing), strings.NewReader(""), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "import "+missing) {
		t.Fatalf("serve missing file = %v, want import error", err)
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
