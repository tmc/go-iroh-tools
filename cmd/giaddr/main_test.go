package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/key"
)

// offline keeps every test on loopback with no relay.
var offline = []string{"-direct-only", "-bind", "127.0.0.1:0"}

// runOut runs giaddr offline with args and returns its standard output.
func runOut(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, append(append([]string{}, offline...), args...), &out); err != nil {
		t.Fatalf("run(%q): %v", args, err)
	}
	return out.String()
}

// writeKey writes a fixed secret key to a file and returns the file name and
// the endpoint id it names.
func writeKey(t *testing.T) (string, string) {
	t.Helper()
	sk := key.NewSecretKey([32]byte{7})
	seed := sk.Bytes()
	name := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(name, []byte("secret "+hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name, sk.Public().EndpointID().String()
}

func TestRunValidates(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want error
	}{
		{name: "unknown flag", args: []string{"-nope"}, want: tool.ErrUsage},
		{name: "help", args: []string{"-h"}, want: flag.ErrHelp},
		{name: "operand", args: []string{"x"}},
		{name: "unknown field", args: []string{"-f", "nope"}},
		{name: "field with json", args: []string{"-f", "id", "-json"}},
		{name: "negative wait", args: []string{"-wait", "-1s"}},
		{name: "missing key file", args: []string{"-direct-only", "-key", filepath.Join(t.TempDir(), "absent")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var out bytes.Buffer
			err := run(ctx, tt.args, &out)
			if err == nil {
				t.Fatalf("run(%q) = nil, want error", tt.args)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("run(%q) = %v, want %v", tt.args, err, tt.want)
			}
			if out.Len() != 0 {
				t.Fatalf("run(%q) wrote %q, want nothing", tt.args, out.String())
			}
		})
	}
}

func TestAllFields(t *testing.T) {
	keyFile, id := writeKey(t)
	out := runOut(t, "-key", keyFile)
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != len(fields) {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), len(fields), out)
	}
	values := make(map[string]string)
	for i, line := range lines {
		name, value, ok := strings.Cut(line, "\t")
		if !ok || name != fields[i] || value == "" {
			t.Fatalf("line %d = %q, want %q, a tab, and a value", i, line, fields[i])
		}
		values[name] = value
	}
	if values["id"] != id {
		t.Errorf("id = %s, want %s", values["id"], id)
	}
	if !strings.HasPrefix(values["local"], "127.0.0.1:") {
		t.Errorf("local = %s, want a 127.0.0.1 address", values["local"])
	}
	addr, err := tool.ParseEndpoint(values["ticket"])
	if err != nil {
		t.Fatalf("parse ticket: %v", err)
	}
	if addr.ID.String() != id {
		t.Errorf("ticket id = %s, want %s", addr.ID, id)
	}
}

func TestSingleField(t *testing.T) {
	keyFile, id := writeKey(t)
	tests := []struct {
		field string
		want  string
	}{
		{field: "id", want: id + "\n"},
		{field: "z32", want: mustID(t, id).Z32() + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			if got := runOut(t, "-key", keyFile, "-f", tt.field); got != tt.want {
				t.Fatalf("-f %s = %q, want %q", tt.field, got, tt.want)
			}
		})
	}
	if got := runOut(t, "-f", "local"); !strings.HasPrefix(got, "127.0.0.1:") || strings.Count(got, "\n") != 1 {
		t.Fatalf("-f local = %q, want one 127.0.0.1 address line", got)
	}
}

func TestJSON(t *testing.T) {
	keyFile, id := writeKey(t)
	out := runOut(t, "-key", keyFile, "-json")
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("-json printed %d lines, want 1:\n%s", strings.Count(out, "\n"), out)
	}
	// Decode loosely first, to check the slices are arrays and not null.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(out), &raw); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for _, name := range []string{"id", "z32", "local", "addr", "ip_addrs", "relay_urls", "ticket"} {
		if _, ok := raw[name]; !ok {
			t.Errorf("missing field %q in %s", name, out)
		}
	}
	if got := string(raw["relay_urls"]); got != "[]" {
		t.Errorf("relay_urls = %s, want [] with -direct-only", got)
	}
	var v endpointJSON
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatal(err)
	}
	if v.ID != id || v.Z32 != mustID(t, id).Z32() {
		t.Errorf("id, z32 = %s, %s; want %s", v.ID, v.Z32, id)
	}
	if len(v.IPAddrs) == 0 || v.IPAddrs[0] != v.Local {
		t.Errorf("ip_addrs = %q, want it to start with local %s", v.IPAddrs, v.Local)
	}
	addr, err := tool.ParseEndpoint(v.Ticket)
	if err != nil {
		t.Fatalf("parse ticket: %v", err)
	}
	if addr.ID.String() != id {
		t.Errorf("ticket id = %s, want %s", addr.ID, id)
	}
}

// TestKeyPersistence checks that -key gives the same identity on every run,
// and that without it each run gets a new one.
func TestKeyPersistence(t *testing.T) {
	keyFile, id := writeKey(t)
	first := runOut(t, "-key", keyFile, "-f", "id")
	second := runOut(t, "-key", keyFile, "-f", "id")
	if first != second || first != id+"\n" {
		t.Fatalf("ids with -key = %q, %q; want %q twice", first, second, id)
	}
	if a, b := runOut(t, "-f", "id"), runOut(t, "-f", "id"); a == b {
		t.Fatalf("two runs without -key both had id %q", a)
	}
}

// TestWaitCancel checks that a signal during -wait ends the run promptly with
// the cancellation, after the endpoint was printed.
func TestWaitCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &notifyWriter{written: make(chan struct{})}
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, append(append([]string{}, offline...), "-f", "id", "-wait", "1h"), out)
	}()
	select {
	case <-out.written:
	case err := <-errc:
		t.Fatalf("run returned %v before -wait", err)
	case <-time.After(10 * time.Second):
		t.Fatal("giaddr printed nothing before -wait")
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

// notifyWriter closes written on its first write.
type notifyWriter struct {
	once    sync.Once
	written chan struct{}
}

func (w *notifyWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.written) })
	return len(p), nil
}

func mustID(t *testing.T, s string) key.EndpointID {
	t.Helper()
	id, err := tool.ParseEndpointID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
