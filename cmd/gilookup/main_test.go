package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

const testOrigin = "test.example."

// testEndpoint is a published endpoint with a relay, a direct address, and
// user data.
type testEndpoint struct {
	sk   key.SecretKey
	id   key.EndpointID
	info dns.EndpointInfo
}

func newTestEndpoint(t *testing.T) testEndpoint {
	t.Helper()
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	relayURL, err := netaddr.ParseRelayURL("https://relay.example.com")
	if err != nil {
		t.Fatal(err)
	}
	ud, err := dns.NewUserData("hello")
	if err != nil {
		t.Fatal(err)
	}
	data := dns.NewEndpointData(netaddr.RelayAddr{URL: relayURL}).
		WithIPAddrs(netip.MustParseAddrPort("127.0.0.1:1234")).
		WithUserData(&ud)
	id := sk.Public().EndpointID()
	return testEndpoint{sk: sk, id: id, info: dns.EndpointInfo{ID: id, Data: data}}
}

// pkarrRelay serves e's signed packet at GET /<z32> and 404 for anything else,
// as a pkarr relay does. A request for block waits until the client gives up.
func pkarrRelay(t *testing.T, e testEndpoint, block key.EndpointID) *httptest.Server {
	t.Helper()
	packet, err := e.info.ToSignedPacket(e.sk, 30)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/") {
		case e.id.Z32():
			w.Header().Set("Content-Type", "application/x-pkarr-signed-packet")
			w.Write(packet.RelayPayload())
		case block.Z32():
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// txtLookuper answers TXT queries for e under testOrigin and fails others.
func txtLookuper(e testEndpoint) dns.TXTLookuper {
	name := "_iroh." + e.id.Z32() + "." + testOrigin
	return dns.TXTLookuperFunc(func(ctx context.Context, n string) ([]string, error) {
		if n != name {
			return nil, errors.New("no such host")
		}
		return e.info.ToTXTStrings(), nil
	})
}

func otherID(t *testing.T) key.EndpointID {
	t.Helper()
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	return sk.Public().EndpointID()
}

func TestFound(t *testing.T) {
	e := newTestEndpoint(t)
	srv := pkarrRelay(t, e, key.EndpointID{})
	ticket := endpointticket.Encode(e.info.Addr())
	want := func(provenance string) string {
		return "id\t" + e.id.String() + "\n" +
			"z32\t" + e.id.Z32() + "\n" +
			"provenance\t" + provenance + "\n" +
			"relay\thttps://relay.example.com/\n" +
			"addr\t127.0.0.1:1234\n" +
			"user-data\thello\n" +
			"ticket\t" + ticket + "\n"
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"pkarr", []string{"-pkarr-url", srv.URL, e.id.String()}, want("pkarr")},
		{"pkarr z32", []string{"-pkarr-url", srv.URL, e.id.Z32()}, want("pkarr")},
		{"pkarr ticket", []string{"-pkarr-url", srv.URL, endpointticket.Encode(netaddr.NewEndpointAddr(e.id))}, want("pkarr")},
		{"dns", []string{"-source", "dns", "-origin", testOrigin, e.id.String()}, want("dns")},
		{"dns origin without dot", []string{"-source", "dns", "-origin", strings.TrimSuffix(testOrigin, "."), e.id.String()}, want("dns")},
		{"all", []string{"-source", "all", "-pkarr-url", srv.URL, "-origin", testOrigin, e.id.String()}, want("pkarr") + "\n" + want("dns")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := run(context.Background(), tt.args, &stdout, &stderr, txtLookuper(e)); err != nil {
				t.Fatal(err)
			}
			if got := stdout.String(); got != tt.want {
				t.Errorf("stdout:\n%s\nwant:\n%s", got, tt.want)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr: %s", stderr.String())
			}
		})
	}
}

func TestNotFound(t *testing.T) {
	e := newTestEndpoint(t)
	block := otherID(t)
	srv := pkarrRelay(t, e, block)
	missing := otherID(t).String()
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"pkarr 404", []string{"-pkarr-url", srv.URL, missing}, "status 404"},
		{"pkarr timeout", []string{"-pkarr-url", srv.URL, "-timeout", "50ms", block.String()}, "no record within 50ms"},
		// A failing DNS lookup is retried for about three seconds, longer
		// than this timeout, so this covers the timeout path.
		{"dns", []string{"-source", "dns", "-origin", testOrigin, "-timeout", "50ms", missing}, "no record within 50ms"},
		{"all", []string{"-source", "all", "-pkarr-url", srv.URL, "-origin", testOrigin, "-timeout", "50ms", missing}, "via dns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(context.Background(), tt.args, &stdout, &stderr, txtLookuper(e))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run = %v, want error containing %q", err, tt.wantErr)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout: %s", stdout.String())
			}
		})
	}
}

func TestEmptyRecordNotFound(t *testing.T) {
	e := newTestEndpoint(t)
	e.info.Data = dns.NewEndpointData()
	srv := pkarrRelay(t, e, key.EndpointID{})
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"-pkarr-url", srv.URL, e.id.String()}, &stdout, &stderr, nil)
	if err == nil || !strings.Contains(err.Error(), "record is empty") {
		t.Fatalf("run = %v, want empty record error", err)
	}
}

// TestPartialFailure checks that -source all prints the record one source
// found and reports the other's failure on stderr.
func TestPartialFailure(t *testing.T) {
	a, b := newTestEndpoint(t), newTestEndpoint(t)
	srv := pkarrRelay(t, a, key.EndpointID{})
	var stdout, stderr bytes.Buffer
	args := []string{"-source", "all", "-pkarr-url", srv.URL, "-origin", testOrigin, "-f", "provenance", b.id.String()}
	if err := run(context.Background(), args, &stdout, &stderr, txtLookuper(b)); err != nil {
		t.Fatal(err)
	}
	if got, want := stdout.String(), "dns\n"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if !strings.Contains(stderr.String(), "via pkarr") {
		t.Errorf("stderr = %q, want pkarr failure", stderr.String())
	}
}

func TestField(t *testing.T) {
	e := newTestEndpoint(t)
	srv := pkarrRelay(t, e, key.EndpointID{})
	tests := []struct {
		field string
		want  string
	}{
		{"id", e.id.String() + "\n"},
		{"z32", e.id.Z32() + "\n"},
		{"provenance", "pkarr\n"},
		{"updated", ""},
		{"relay", "https://relay.example.com/\n"},
		{"addr", "127.0.0.1:1234\n"},
		{"user-data", "hello\n"},
		{"ticket", endpointticket.Encode(e.info.Addr()) + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			// -source all also finds the dns record; -f prints only the first.
			args := []string{"-source", "all", "-pkarr-url", srv.URL, "-origin", testOrigin, "-f", tt.field, e.id.String()}
			var stdout, stderr bytes.Buffer
			if err := run(context.Background(), args, &stdout, &stderr, txtLookuper(e)); err != nil {
				t.Fatal(err)
			}
			if got := stdout.String(); got != tt.want {
				t.Errorf("stdout = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFieldTicketDialable checks that -f ticket prints a ticket the other
// tools accept, carrying the published addresses.
func TestFieldTicketDialable(t *testing.T) {
	e := newTestEndpoint(t)
	srv := pkarrRelay(t, e, key.EndpointID{})
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"-pkarr-url", srv.URL, "-f", "ticket", e.id.Z32()}, &stdout, &stderr, nil); err != nil {
		t.Fatal(err)
	}
	addr, err := tool.ParseEndpoint(strings.TrimSpace(stdout.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !addr.ID.Equal(e.id) {
		t.Errorf("ticket id = %s, want %s", addr.ID, e.id)
	}
	if urls := addr.RelayURLs(); len(urls) != 1 || urls[0].String() != "https://relay.example.com/" {
		t.Errorf("ticket relay URLs = %v", urls)
	}
	if ips := addr.IPAddrs(); len(ips) != 1 || ips[0].String() != "127.0.0.1:1234" {
		t.Errorf("ticket IP addrs = %v", ips)
	}
}

func TestJSON(t *testing.T) {
	e := newTestEndpoint(t)
	srv := pkarrRelay(t, e, key.EndpointID{})
	args := []string{"-source", "all", "-pkarr-url", srv.URL, "-origin", testOrigin, "-json", e.id.String()}
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), args, &stdout, &stderr, txtLookuper(e)); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&stdout)
	for _, provenance := range []string{"pkarr", "dns"} {
		var got recordJSON
		if err := dec.Decode(&got); err != nil {
			t.Fatal(err)
		}
		want := recordJSON{
			ID:         e.id.String(),
			Z32:        e.id.Z32(),
			Provenance: provenance,
			RelayURLs:  []string{"https://relay.example.com/"},
			IPAddrs:    []string{"127.0.0.1:1234"},
			UserData:   "hello",
			Ticket:     endpointticket.Encode(e.info.Addr()),
		}
		if got.ID != want.ID || got.Z32 != want.Z32 || got.Provenance != want.Provenance ||
			strings.Join(got.RelayURLs, ",") != strings.Join(want.RelayURLs, ",") ||
			strings.Join(got.IPAddrs, ",") != strings.Join(want.IPAddrs, ",") ||
			got.UserData != want.UserData || got.Ticket != want.Ticket || got.Updated != "" {
			t.Errorf("record = %+v, want %+v", got, want)
		}
	}
	if dec.More() {
		t.Error("more than two records")
	}
}

// TestJSONEmptyArrays checks that absent addresses encode as [] not null.
func TestJSONEmptyArrays(t *testing.T) {
	e := newTestEndpoint(t)
	ud, err := dns.NewUserData("only")
	if err != nil {
		t.Fatal(err)
	}
	e.info.Data = dns.NewEndpointData().WithUserData(&ud)
	srv := pkarrRelay(t, e, key.EndpointID{})
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"-pkarr-url", srv.URL, "-json", e.id.String()}, &stdout, &stderr, nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"relay_urls":[]`, `"ip_addrs":[]`, `"user_data":"only"`} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("output missing %s: %s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "updated") {
		t.Errorf("output has updated: %s", stdout.String())
	}
}

func TestFlagErrors(t *testing.T) {
	id := otherID(t).String()
	tests := []struct {
		name    string
		args    []string
		wantErr string // empty means a usage error
	}{
		{"bad flag", []string{"-nope", id}, ""},
		{"bad timeout syntax", []string{"-timeout", "soon", id}, ""},
		{"no operand", nil, "usage: gilookup"},
		{"two operands", []string{id, id}, "usage: gilookup"},
		{"bad id", []string{"nope"}, "parse endpoint id"},
		{"bad source", []string{"-source", "mdns", id}, "unknown source"},
		{"bad field", []string{"-f", "nope", id}, "unknown field"},
		{"f and json", []string{"-f", "id", "-json", id}, "mutually exclusive"},
		{"zero timeout", []string{"-timeout", "0", id}, "-timeout must be positive"},
		{"staging and pkarr-url", []string{"-staging", "-pkarr-url", "http://127.0.0.1:1", id}, "-staging and -pkarr-url"},
		{"staging and origin", []string{"-source", "dns", "-staging", "-origin", testOrigin, id}, "-staging and -origin"},
		{"pkarr-url with dns", []string{"-source", "dns", "-pkarr-url", "http://127.0.0.1:1", id}, "-pkarr-url applies only"},
		{"origin with pkarr", []string{"-origin", testOrigin, id}, "-origin applies only"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := run(ctx, tt.args, &stdout, &stderr, nil)
			if tt.wantErr == "" {
				if !errors.Is(err, tool.ErrUsage) {
					t.Fatalf("run = %v, want ErrUsage", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"-h"}, &stdout, &stderr, nil); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("run = %v, want flag.ErrHelp", err)
	}
}
