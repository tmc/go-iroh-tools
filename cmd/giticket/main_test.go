package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func TestRunBuildAndInspect(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = run(&out, nil, sk.Public().EndpointID().String(), []string{"ip:127.0.0.1:1234"}, false, "")
	if err != nil {
		t.Fatal(err)
	}
	ticket := strings.TrimSpace(out.String())
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(addr.Addrs()), 1; got != want {
		t.Fatalf("len(addr.Addrs()) = %d, want %d", got, want)
	}

	out.Reset()
	if err := run(&out, []string{ticket}, "", nil, false, ""); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"id " + sk.Public().EndpointID().String(),
		"z32 " + sk.Public().EndpointID().Z32(),
		"addr ip:127.0.0.1:1234",
		"ticket " + ticket,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q:\n%s", want, text)
		}
	}
}

func TestRunShortDropsIPAddrs(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		t.Fatal(err)
	}
	ipAddr, err := netaddr.ParseTransportAddr("ip:127.0.0.1:1234")
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(sk.Public().EndpointID(), netaddr.RelayAddr{URL: relayURL}, ipAddr)
	ticket := endpointticket.Encode(addr)

	var out bytes.Buffer
	if err := run(&out, []string{ticket}, "", nil, true, ""); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if strings.Contains(text, "ip:127.0.0.1:1234") {
		t.Fatalf("short output contains direct address:\n%s", text)
	}
	if !strings.Contains(text, "relay:https://relay.example.com/") {
		t.Fatalf("short output missing relay address:\n%s", text)
	}
}

// TestRunKeepsRelayPathPrefix checks that a relay URL with a path prefix
// survives a build and inspect round trip, normalized with a trailing slash.
func TestRunKeepsRelayPathPrefix(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = run(&out, nil, sk.Public().EndpointID().String(), []string{"relay:https://relay.example.com/prefix"}, false, "")
	if err != nil {
		t.Fatal(err)
	}
	ticket := strings.TrimSpace(out.String())

	out.Reset()
	if err := run(&out, []string{ticket}, "", nil, false, "addr"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(out.String()), "relay:https://relay.example.com/prefix/"; got != want {
		t.Fatalf("addr = %q, want %q", got, want)
	}
}

// TestRunFieldSelection covers the output selection the docs point at: a
// shortened ticket must come back as a bare ticket another command can take.
func TestRunFieldSelection(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		t.Fatal(err)
	}
	ipAddr, err := netaddr.ParseTransportAddr("ip:127.0.0.1:1234")
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(sk.Public().EndpointID(), netaddr.RelayAddr{URL: relayURL}, ipAddr)
	ticket := endpointticket.Encode(addr)

	var out bytes.Buffer
	if err := run(&out, []string{ticket}, "", nil, true, "ticket"); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(out.String())
	if strings.ContainsAny(got, " \t\n") {
		t.Fatalf("field output = %q, want a bare ticket", got)
	}
	decoded, err := endpointticket.Decode(got)
	if err != nil {
		t.Fatalf("decode field output: %v", err)
	}
	if !decoded.ID.Equal(addr.ID) {
		t.Fatalf("id = %s, want %s", decoded.ID, addr.ID)
	}
	if len(decoded.IPAddrs()) != 0 {
		t.Fatalf("IPAddrs = %v, want none in a short ticket", decoded.IPAddrs())
	}

	out.Reset()
	if err := run(&out, []string{ticket}, "", nil, false, "z32"); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(out.String()), addr.ID.Z32(); got != want {
		t.Fatalf("z32 field = %q, want %q", got, want)
	}
}

func TestRunRejectsBadInvocations(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	id := sk.Public().EndpointID().String()
	tests := []struct {
		name   string
		args   []string
		idText string
		addrs  []string
		field  string
	}{
		{name: "unknown field", args: []string{"x"}, field: "nope"},
		{name: "addr without id", args: []string{"x"}, addrs: []string{"ip:127.0.0.1:1"}},
		{name: "operand with id", args: []string{"x"}, idText: id},
		{name: "no operand", args: nil},
		{name: "two operands", args: []string{"a", "b"}},
		{name: "bad ticket", args: []string{"endpointnotvalid"}},
		{name: "bad id", args: nil, idText: "not-an-id"},
		{name: "bad addr", idText: id, addrs: []string{"nonsense"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(&bytes.Buffer{}, tt.args, tt.idText, tt.addrs, false, tt.field)
			if err == nil {
				t.Fatal("run succeeded, want error")
			}
		})
	}
}

// TestRunAcceptsZ32ID checks that an id encoding printed by these tools is
// accepted back by them.
func TestRunAcceptsZ32ID(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(&out, nil, sk.Public().EndpointID().Z32(), nil, false, ""); err != nil {
		t.Fatalf("run with z32 id: %v", err)
	}
	addr, err := endpointticket.Decode(strings.TrimSpace(out.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !addr.ID.Equal(sk.Public().EndpointID()) {
		t.Fatalf("id = %s, want %s", addr.ID, sk.Public().EndpointID())
	}
}
