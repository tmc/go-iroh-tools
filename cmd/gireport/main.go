// Gireport prints a network report: whether this host reaches iroh relays
// over UDP, its public addresses as relays see them, and the latency to
// each relay.
//
// Usage:
//
//	gireport [-staging] [-bind addr] [-timeout d] [-f field | -json] [relay-url...]
//
// Gireport binds an endpoint, using the default relays, the staging relays
// with -staging, or the relays named as operands, and waits for the
// endpoint's first network report. The default bind address is the wildcard
// address, so that probes can leave the host.
//
// The report is printed as tab-separated lines on standard output:
//
//	udp-v4	true
//	udp-v6	false
//	global-v4	203.0.113.7:41641
//	mapping-varies-v4	false
//	captive-portal	false
//	preferred-relay	https://relay.example./
//	relay	https://relay.example./	23.1ms
//
// udp-v4 and udp-v6 are always printed. The other lines appear only when
// the report has a value for them, and relay lines are sorted by URL.
// -f prints only the named field's value, one per line for relay, and
// -json prints the report as a single JSON object with durations in
// nanoseconds.
//
// Gireport exits 0 after printing a report and 2 if the flags cannot be
// parsed. It exits 1 if no report completes within -timeout or before an
// interrupt, if -f names a field the report has no value for, or if the
// flags conflict or a relay URL is malformed; those last two are reported
// before any endpoint is bound.
// Gireport always probes relays, so it has no -direct-only or -relay flag.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

// fields lists the names -f accepts, in output order.
var fields = []string{
	"udp-v4",
	"udp-v6",
	"global-v4",
	"global-v6",
	"mapping-varies-v4",
	"mapping-varies-v6",
	"captive-portal",
	"preferred-relay",
	"relay",
}

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, os.Stdout, args)
	})
}

// config is a validated command line.
type config struct {
	bind    tool.BindFlags
	timeout time.Duration
	field   string
	json    bool
}

func parseArgs(args []string) (config, error) {
	fs := flag.NewFlagSet("gireport", flag.ContinueOnError)
	var c config
	fs.StringVar(&c.bind.Bind, "bind", "", "local UDP `address` (default: all interfaces)")
	fs.BoolVar(&c.bind.Staging, "staging", false, "use the staging iroh relays")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "how long to wait for a report")
	fs.StringVar(&c.field, "f", "", "print only this `field`")
	fs.BoolVar(&c.json, "json", false, "print the report as JSON")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: gireport [-staging] [-bind addr] [-timeout d] [-f field | -json] [relay-url...]\n")
		fs.PrintDefaults()
	}
	if err := tool.ParseFlags(fs, args); err != nil {
		return config{}, err
	}
	if c.timeout <= 0 {
		return config{}, fmt.Errorf("-timeout must be positive, got %s", c.timeout)
	}
	if c.field != "" && c.json {
		return config{}, errors.New("-f and -json are mutually exclusive")
	}
	if c.field != "" && !slices.Contains(fields, c.field) {
		return config{}, fmt.Errorf("unknown field %q", c.field)
	}
	if c.bind.Bind != "" {
		if _, err := netip.ParseAddrPort(c.bind.Bind); err != nil {
			return config{}, fmt.Errorf("parse bind address: %w", err)
		}
	}
	if fs.NArg() > 0 && c.bind.Staging {
		return config{}, errors.New("-staging and relay URL operands are mutually exclusive")
	}
	for _, s := range fs.Args() {
		if _, err := tool.ParseRelayURL(s); err != nil {
			return config{}, err
		}
	}
	c.bind.Relay = true
	c.bind.RelayURLs = fs.Args()
	return c, nil
}

func run(ctx context.Context, w io.Writer, args []string) error {
	c, err := parseArgs(args)
	if err != nil {
		return err
	}
	r, err := report(ctx, c.bind, c.timeout)
	if err != nil {
		return err
	}
	switch {
	case c.json:
		return writeJSON(w, r)
	case c.field != "":
		return writeField(w, r, c.field)
	}
	return writeText(w, r)
}

// report binds an endpoint and waits up to timeout for its first network
// report.
func report(ctx context.Context, bind tool.BindFlags, timeout time.Duration) (iroh.NetReport, error) {
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return iroh.NetReport{}, err
	}
	defer closeEndpoint()
	return waitReport(ctx, ep.NetReport, timeout)
}

// waitReport polls get until it returns a report or timeout passes.
//
// An interrupt is reported as an error rather than as ctx's cancellation,
// which tool.Run would treat as success: no report was printed.
func waitReport(ctx context.Context, get func() (iroh.NetReport, bool), timeout time.Duration) (iroh.NetReport, error) {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		if r, ok := get(); ok {
			return r, nil
		}
		select {
		case <-tctx.Done():
			if ctx.Err() != nil {
				return iroh.NetReport{}, errors.New("interrupted before a network report completed")
			}
			return iroh.NetReport{}, fmt.Errorf("no network report within %s", timeout)
		case <-t.C:
		}
	}
}

// line is one output line: a field name and its value. A relay line's value
// is the relay URL and its latency, separated by a tab.
type line struct {
	name, value string
}

// lines returns r's fields in output order, leaving out those r has no
// value for.
func lines(r iroh.NetReport) []line {
	ls := []line{
		{"udp-v4", strconv.FormatBool(r.UDPv4)},
		{"udp-v6", strconv.FormatBool(r.UDPv6)},
	}
	if r.GlobalV4.IsValid() {
		ls = append(ls, line{"global-v4", r.GlobalV4.String()})
	}
	if r.GlobalV6.IsValid() {
		ls = append(ls, line{"global-v6", r.GlobalV6.String()})
	}
	if r.MappingVariesByDestV4 != nil {
		ls = append(ls, line{"mapping-varies-v4", strconv.FormatBool(*r.MappingVariesByDestV4)})
	}
	if r.MappingVariesByDestV6 != nil {
		ls = append(ls, line{"mapping-varies-v6", strconv.FormatBool(*r.MappingVariesByDestV6)})
	}
	if r.CaptivePortal != nil {
		ls = append(ls, line{"captive-portal", strconv.FormatBool(*r.CaptivePortal)})
	}
	if !r.PreferredRelay.IsZero() {
		ls = append(ls, line{"preferred-relay", r.PreferredRelay.String()})
	}
	for _, u := range sortedRelays(r) {
		rtt := r.RelayLatencies[u].Round(time.Microsecond)
		ls = append(ls, line{"relay", u.String() + "\t" + rtt.String()})
	}
	return ls
}

func sortedRelays(r iroh.NetReport) []netaddr.RelayURL {
	urls := make([]netaddr.RelayURL, 0, len(r.RelayLatencies))
	for u := range r.RelayLatencies {
		urls = append(urls, u)
	}
	slices.SortFunc(urls, func(a, b netaddr.RelayURL) int {
		return a.Compare(b)
	})
	return urls
}

func writeText(w io.Writer, r iroh.NetReport) error {
	for _, l := range lines(r) {
		if _, err := fmt.Fprintf(w, "%s\t%s\n", l.name, l.value); err != nil {
			return err
		}
	}
	return nil
}

// writeField prints the value of the named field, or reports an error if r
// has no value for it.
func writeField(w io.Writer, r iroh.NetReport, field string) error {
	found := false
	for _, l := range lines(r) {
		if l.name != field {
			continue
		}
		found = true
		if _, err := fmt.Fprintln(w, l.value); err != nil {
			return err
		}
	}
	if !found {
		return fmt.Errorf("report has no %s", field)
	}
	return nil
}

type jsonReport struct {
	UDPv4                 bool        `json:"udp_v4"`
	UDPv6                 bool        `json:"udp_v6"`
	GlobalV4              string      `json:"global_v4,omitempty"`
	GlobalV6              string      `json:"global_v6,omitempty"`
	MappingVariesByDestV4 *bool       `json:"mapping_varies_v4,omitempty"`
	MappingVariesByDestV6 *bool       `json:"mapping_varies_v6,omitempty"`
	CaptivePortal         *bool       `json:"captive_portal,omitempty"`
	PreferredRelay        string      `json:"preferred_relay,omitempty"`
	Relays                []jsonRelay `json:"relays,omitempty"`
}

type jsonRelay struct {
	URL   string `json:"url"`
	RTTNs int64  `json:"rtt_ns"`
}

func writeJSON(w io.Writer, r iroh.NetReport) error {
	j := jsonReport{
		UDPv4:                 r.UDPv4,
		UDPv6:                 r.UDPv6,
		MappingVariesByDestV4: r.MappingVariesByDestV4,
		MappingVariesByDestV6: r.MappingVariesByDestV6,
		CaptivePortal:         r.CaptivePortal,
	}
	if r.GlobalV4.IsValid() {
		j.GlobalV4 = r.GlobalV4.String()
	}
	if r.GlobalV6.IsValid() {
		j.GlobalV6 = r.GlobalV6.String()
	}
	if !r.PreferredRelay.IsZero() {
		j.PreferredRelay = r.PreferredRelay.String()
	}
	for _, u := range sortedRelays(r) {
		j.Relays = append(j.Relays, jsonRelay{URL: u.String(), RTTNs: int64(r.RelayLatencies[u])})
	}
	enc := json.NewEncoder(w)
	return enc.Encode(j)
}
