package tool

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
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

// TestParseFlags pins the policy that lets a runner parse flags in a test
// without terminating the test binary: a mistake is ErrUsage, a help request is
// flag.ErrHelp, and both stop the caller before it does any work.
func TestParseFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want error
	}{
		{name: "ok", args: []string{"-n", "3", "operand"}, want: nil},
		{name: "no args", args: nil, want: nil},
		{name: "unknown flag", args: []string{"-nope"}, want: ErrUsage},
		{name: "bad value", args: []string{"-n", "many"}, want: ErrUsage},
		{name: "help", args: []string{"-h"}, want: flag.ErrHelp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			fs.Int("n", 0, "count")
			if err := ParseFlags(fs, tt.args); !errors.Is(err, tt.want) {
				t.Fatalf("ParseFlags(%q) = %v, want %v", tt.args, err, tt.want)
			}
		})
	}
}

func TestParseSecretKey(t *testing.T) {
	sk := key.NewSecretKey([32]byte{3})
	seed := sk.Bytes()
	hexKey := hex.EncodeToString(seed[:])
	tests := []struct {
		name string
		in   string
	}{
		{name: "bare", in: hexKey + "\n"},
		{name: "labeled", in: "secret " + hexKey + "\n"},
		{name: "leading blank lines", in: "\n\n  secret " + hexKey + "  \npublic ignored\n"},
		{name: "no newline", in: hexKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSecretKey(strings.NewReader(tt.in))
			if err != nil {
				t.Fatalf("ParseSecretKey: %v", err)
			}
			if !got.Public().Equal(sk.Public()) {
				t.Fatalf("public = %s, want %s", got.Public(), sk.Public())
			}
		})
	}
	for _, in := range []string{"", "\n\n", "secret nothex\n"} {
		if _, err := ParseSecretKey(strings.NewReader(in)); err == nil {
			t.Errorf("ParseSecretKey(%q) succeeded, want error", in)
		}
	}
}

// TestBindKey checks that -key gives an endpoint the same id on every run.
func TestBindKey(t *testing.T) {
	sk := key.NewSecretKey([32]byte{4})
	seed := sk.Bytes()
	name := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(name, []byte("secret "+hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		ep, closeEndpoint, err := Bind(context.Background(), BindFlags{Bind: "127.0.0.1:0", DirectOnly: true, Key: name})
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		id := ep.ID()
		closeEndpoint()
		if !id.Equal(sk.Public().EndpointID()) {
			t.Fatalf("id = %s, want %s", id, sk.Public().EndpointID())
		}
	}
	if _, _, err := Bind(context.Background(), BindFlags{Bind: "127.0.0.1:0", DirectOnly: true, Key: name + ".missing"}); err == nil {
		t.Fatal("Bind with missing key file succeeded")
	}
}

// TestBindRejectsRelayConflicts checks the relay flag combinations that
// cannot all be honored, and relay URLs that name no relay server.
func TestBindRejectsRelayConflicts(t *testing.T) {
	tests := []struct {
		name string
		f    BindFlags
	}{
		{name: "url and staging", f: BindFlags{Relay: true, RelayURLs: []string{"https://relay.example./"}, Staging: true}},
		{name: "url and direct-only", f: BindFlags{Relay: true, DirectOnly: true, RelayURLs: []string{"https://relay.example./"}}},
		{name: "staging and direct-only", f: BindFlags{Relay: true, DirectOnly: true, Staging: true}},
		{name: "url and relay=false", f: BindFlags{RelayURLs: []string{"https://relay.example./"}}},
		{name: "staging and relay=false", f: BindFlags{Staging: true}},
		{name: "not a URL", f: BindFlags{Relay: true, RelayURLs: []string{"::"}}},
		{name: "wrong scheme", f: BindFlags{Relay: true, RelayURLs: []string{"ftp://relay.example./"}}},
		{name: "no host", f: BindFlags{Relay: true, RelayURLs: []string{"https:///prefix"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.f.Bind = "127.0.0.1:0"
			ep, closeEndpoint, err := Bind(context.Background(), tt.f)
			if err == nil {
				closeEndpoint()
				t.Fatalf("Bind(%+v) = %s, want error", tt.f, ep.ID())
			}
		})
	}
}

func TestParseRelayURL(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"https://relay.example.com", "https://relay.example.com/"},
		{"http://relay.example.com:8080/prefix", "http://relay.example.com:8080/prefix/"},
	}
	for _, tt := range tests {
		u, err := ParseRelayURL(tt.in)
		if err != nil {
			t.Errorf("ParseRelayURL(%q): %v", tt.in, err)
			continue
		}
		if got := u.String(); got != tt.want {
			t.Errorf("ParseRelayURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestBindDebugFiles checks that -qlog and -keylog create their targets
// before the endpoint exists, and that the cleanup releases the key log.
func TestBindDebugFiles(t *testing.T) {
	dir := t.TempDir()
	f := BindFlags{
		Bind:       "127.0.0.1:0",
		DirectOnly: true,
		QLOG:       filepath.Join(dir, "qlog", "nested"),
		KeyLog:     filepath.Join(dir, "keylog"),
	}
	_, closeEndpoint, err := Bind(context.Background(), f)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	closeEndpoint()
	if fi, err := os.Stat(f.QLOG); err != nil || !fi.IsDir() {
		t.Fatalf("qlog dir: %v", err)
	}
	fi, err := os.Stat(f.KeyLog)
	if err != nil {
		t.Fatalf("key log: %v", err)
	}
	if mode := fi.Mode().Perm(); mode != 0o600 {
		t.Fatalf("key log mode = %v, want 0600", mode)
	}

	f.QLOG = filepath.Join(f.KeyLog, "under-a-file")
	if _, _, err := Bind(context.Background(), f); err == nil {
		t.Fatal("Bind with uncreatable qlog dir succeeded")
	}
}
