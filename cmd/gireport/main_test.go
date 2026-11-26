package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func relayURL(t *testing.T, s string) netaddr.RelayURL {
	t.Helper()
	u, err := netaddr.ParseRelayURL(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func ptr(b bool) *bool { return &b }

// fullReport returns a report with every field set. Its relays are listed
// out of order so that sorting is exercised.
func fullReport(t *testing.T) iroh.NetReport {
	a := relayURL(t, "https://a.relay.example./")
	b := relayURL(t, "https://b.relay.example./")
	return iroh.NetReport{
		UDPv4:                 true,
		UDPv6:                 true,
		MappingVariesByDestV4: ptr(false),
		MappingVariesByDestV6: ptr(true),
		PreferredRelay:        a,
		RelayLatencies: map[netaddr.RelayURL]time.Duration{
			b: 41*time.Millisecond + 300*time.Nanosecond,
			a: 23100 * time.Microsecond,
		},
		GlobalV4:      netip.MustParseAddrPort("203.0.113.7:41641"),
		GlobalV6:      netip.MustParseAddrPort("[2001:db8::7]:41641"),
		CaptivePortal: ptr(false),
	}
}

func ipv4Report(t *testing.T) iroh.NetReport {
	a := relayURL(t, "https://a.relay.example./")
	return iroh.NetReport{
		UDPv4:                 true,
		MappingVariesByDestV4: ptr(false),
		PreferredRelay:        a,
		RelayLatencies:        map[netaddr.RelayURL]time.Duration{a: 5 * time.Millisecond},
		GlobalV4:              netip.MustParseAddrPort("203.0.113.7:41641"),
	}
}

func TestWriteText(t *testing.T) {
	tests := []struct {
		name   string
		report func(*testing.T) iroh.NetReport
		want   string
	}{
		{
			name:   "full",
			report: fullReport,
			want: "udp-v4\ttrue\n" +
				"udp-v6\ttrue\n" +
				"global-v4\t203.0.113.7:41641\n" +
				"global-v6\t[2001:db8::7]:41641\n" +
				"mapping-varies-v4\tfalse\n" +
				"mapping-varies-v6\ttrue\n" +
				"captive-portal\tfalse\n" +
				"preferred-relay\thttps://a.relay.example./\n" +
				"relay\thttps://a.relay.example./\t23.1ms\n" +
				"relay\thttps://b.relay.example./\t41ms\n",
		},
		{
			name:   "zero",
			report: func(*testing.T) iroh.NetReport { return iroh.NetReport{} },
			want:   "udp-v4\tfalse\nudp-v6\tfalse\n",
		},
		{
			name:   "ipv4 only",
			report: ipv4Report,
			want: "udp-v4\ttrue\n" +
				"udp-v6\tfalse\n" +
				"global-v4\t203.0.113.7:41641\n" +
				"mapping-varies-v4\tfalse\n" +
				"preferred-relay\thttps://a.relay.example./\n" +
				"relay\thttps://a.relay.example./\t5ms\n",
		},
		{
			name: "empty latencies",
			report: func(t *testing.T) iroh.NetReport {
				r := ipv4Report(t)
				r.PreferredRelay = netaddr.RelayURL{}
				r.RelayLatencies = map[netaddr.RelayURL]time.Duration{}
				return r
			},
			want: "udp-v4\ttrue\n" +
				"udp-v6\tfalse\n" +
				"global-v4\t203.0.113.7:41641\n" +
				"mapping-varies-v4\tfalse\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeText(&out, tt.report(t)); err != nil {
				t.Fatal(err)
			}
			if got := out.String(); got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

// TestFieldsCoverOutput checks that every name the text output can print is
// one -f accepts, so the two lists cannot drift apart.
func TestFieldsCoverOutput(t *testing.T) {
	var names []string
	for _, l := range lines(fullReport(t)) {
		if len(names) == 0 || names[len(names)-1] != l.name {
			names = append(names, l.name)
		}
	}
	if !slices.Equal(names, fields) {
		t.Fatalf("output names = %q, want %q", names, fields)
	}
}

func TestWriteField(t *testing.T) {
	tests := []struct {
		field   string
		report  func(*testing.T) iroh.NetReport
		want    string
		wantErr bool
	}{
		{field: "udp-v4", report: fullReport, want: "true\n"},
		{field: "udp-v6", report: ipv4Report, want: "false\n"},
		{field: "global-v4", report: fullReport, want: "203.0.113.7:41641\n"},
		{field: "mapping-varies-v6", report: fullReport, want: "true\n"},
		{field: "preferred-relay", report: fullReport, want: "https://a.relay.example./\n"},
		{
			field:  "relay",
			report: fullReport,
			want:   "https://a.relay.example./\t23.1ms\nhttps://b.relay.example./\t41ms\n",
		},
		{field: "global-v6", report: ipv4Report, wantErr: true},
		{field: "captive-portal", report: ipv4Report, wantErr: true},
		{field: "relay", report: func(*testing.T) iroh.NetReport { return iroh.NetReport{} }, wantErr: true},
	}
	for _, tt := range tests {
		var out bytes.Buffer
		err := writeField(&out, tt.report(t), tt.field)
		if tt.wantErr {
			if err == nil {
				t.Errorf("-f %s: got %q, want error", tt.field, out.String())
			}
			continue
		}
		if err != nil {
			t.Errorf("-f %s: %v", tt.field, err)
			continue
		}
		if got := out.String(); got != tt.want {
			t.Errorf("-f %s = %q, want %q", tt.field, got, tt.want)
		}
	}
}

func TestWriteJSON(t *testing.T) {
	tests := []struct {
		name   string
		report func(*testing.T) iroh.NetReport
		want   string
	}{
		{
			name:   "full",
			report: fullReport,
			want: `{"udp_v4":true,"udp_v6":true,` +
				`"global_v4":"203.0.113.7:41641","global_v6":"[2001:db8::7]:41641",` +
				`"mapping_varies_v4":false,"mapping_varies_v6":true,"captive_portal":false,` +
				`"preferred_relay":"https://a.relay.example./",` +
				`"relays":[{"url":"https://a.relay.example./","rtt_ns":23100000},` +
				`{"url":"https://b.relay.example./","rtt_ns":41000300}]}` + "\n",
		},
		{
			name:   "ipv4 only",
			report: ipv4Report,
			want: `{"udp_v4":true,"udp_v6":false,"global_v4":"203.0.113.7:41641",` +
				`"mapping_varies_v4":false,"preferred_relay":"https://a.relay.example./",` +
				`"relays":[{"url":"https://a.relay.example./","rtt_ns":5000000}]}` + "\n",
		},
		{
			name:   "zero",
			report: func(*testing.T) iroh.NetReport { return iroh.NetReport{} },
			want:   `{"udp_v4":false,"udp_v6":false}` + "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeJSON(&out, tt.report(t)); err != nil {
				t.Fatal(err)
			}
			if got := out.String(); got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
			if !json.Valid(out.Bytes()) {
				t.Errorf("output is not valid JSON")
			}
		})
	}
}

func TestWaitReport(t *testing.T) {
	// The report arrives on the third poll.
	polls := 0
	get := func() (iroh.NetReport, bool) {
		polls++
		return iroh.NetReport{UDPv4: true}, polls == 3
	}
	r, err := waitReport(context.Background(), get, time.Minute)
	if err != nil || !r.UDPv4 {
		t.Fatalf("waitReport = %+v, %v; want a report", r, err)
	}

	never := func() (iroh.NetReport, bool) { return iroh.NetReport{}, false }
	_, err = waitReport(context.Background(), never, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "no network report within") {
		t.Fatalf("timeout: waitReport error = %v", err)
	}

	// An interrupt must not surface as ctx's error, which tool.Run would
	// report as success.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = waitReport(ctx, never, time.Minute)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("interrupt: waitReport error = %v, want a non-cancellation error", err)
	}
}

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want config
	}{
		{
			name: "defaults",
			want: config{bind: tool.BindFlags{Relay: true}, timeout: 10 * time.Second},
		},
		{
			name: "staging",
			args: []string{"-staging", "-bind", "0.0.0.0:0", "-timeout", "3s", "-json"},
			want: config{
				bind:    tool.BindFlags{Bind: "0.0.0.0:0", Relay: true, Staging: true},
				timeout: 3 * time.Second,
				json:    true,
			},
		},
		{
			name: "relay urls",
			args: []string{"-f", "relay", "https://a.relay.example./", "http://localhost:3340/prefix"},
			want: config{
				bind: tool.BindFlags{
					Relay:     true,
					RelayURLs: []string{"https://a.relay.example./", "http://localhost:3340/prefix"},
				},
				timeout: 10 * time.Second,
				field:   "relay",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if err != nil {
				t.Fatal(err)
			}
			if got.bind.Bind != tt.want.bind.Bind ||
				got.bind.Relay != tt.want.bind.Relay ||
				got.bind.DirectOnly ||
				got.bind.Staging != tt.want.bind.Staging ||
				!slices.Equal(got.bind.RelayURLs, tt.want.bind.RelayURLs) ||
				got.timeout != tt.want.timeout ||
				got.field != tt.want.field ||
				got.json != tt.want.json {
				t.Fatalf("parseArgs(%q) = %+v, want %+v", tt.args, got, tt.want)
			}
		})
	}
}

func TestParseArgsErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string // substring of the error; empty means tool.ErrUsage
	}{
		{"direct-only", []string{"-direct-only"}, ""},
		{"relay=false", []string{"-relay=false"}, ""},
		{"bad timeout", []string{"-timeout", "0s"}, "-timeout must be positive"},
		{"f and json", []string{"-f", "udp-v4", "-json"}, "mutually exclusive"},
		{"unknown field", []string{"-f", "hairpinning"}, "unknown field"},
		{"bad bind", []string{"-bind", "localhost"}, "parse bind address"},
		{"staging and urls", []string{"-staging", "https://a.relay.example./"}, "mutually exclusive"},
		{"bad scheme", []string{"ftp://relay.example./"}, "parse relay URL"},
		{"no host", []string{"https:///path"}, "parse relay URL"},
		{"not a url", []string{"::"}, "parse relay URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseArgs(tt.args)
			if tt.want == "" {
				if !errors.Is(err, tool.ErrUsage) {
					t.Fatalf("parseArgs(%q) = %v, want %v", tt.args, err, tool.ErrUsage)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseArgs(%q) = %v, want error containing %q", tt.args, err, tt.want)
			}
		})
	}
}
