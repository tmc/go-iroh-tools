package main

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/tmc/go-iroh/key"
)

func TestPublicAndInspect(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	seed := sk.Bytes()

	var out bytes.Buffer
	if err := run(&out, []string{"public", hex.EncodeToString(seed[:])}); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"public " + sk.Public().String(),
		"endpoint " + sk.Public().EndpointID().String(),
		"z32 " + sk.Public().EndpointID().Z32(),
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q:\n%s", want, text)
		}
	}

	out.Reset()
	if err := run(&out, []string{"inspect", sk.Public().EndpointID().Z32()}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "endpoint "+sk.Public().EndpointID().String()) {
		t.Fatalf("inspect output missing endpoint:\n%s", out.String())
	}
}

func TestParseEndpointRejectsBadInput(t *testing.T) {
	if _, err := parseEndpoint("not-an-endpoint"); err == nil {
		t.Fatal("parseEndpoint succeeded on bad input")
	}
}
