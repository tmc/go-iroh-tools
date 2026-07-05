package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestDefaultAndStagingMaps(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out, nil, false); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(strings.TrimSpace(out.String()), "\n") + 1; got < 4 {
		t.Fatalf("default relay count = %d, want at least 4\n%s", got, out.String())
	}
	if !strings.Contains(out.String(), "https://") {
		t.Fatalf("default output missing URL:\n%s", out.String())
	}

	out.Reset()
	if err := run(&out, nil, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "staging") {
		t.Fatalf("staging output missing staging relay:\n%s", out.String())
	}
}

func TestNormalizeRelayURL(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out, []string{"https://relay.example.com"}, false); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(out.String()), "https://relay.example.com/"; got != want {
		t.Fatalf("relay URL = %q, want %q", got, want)
	}
}
