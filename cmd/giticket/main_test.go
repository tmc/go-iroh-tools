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
	err = run(&out, nil, sk.Public().EndpointID().String(), []string{"ip:127.0.0.1:1234"}, false)
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
	if err := run(&out, []string{ticket}, "", nil, false); err != nil {
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
	if err := run(&out, []string{ticket}, "", nil, true); err != nil {
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
