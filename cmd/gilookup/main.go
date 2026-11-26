// Gilookup looks up the addresses an iroh endpoint has published, the way
// dig(1) looks up a DNS name.
//
// Usage:
//
//	gilookup [-source pkarr|dns|all] [-staging] [-pkarr-url url] [-origin domain]
//	         [-timeout 10s] [-f field] [-json] <endpoint-id-or-ticket>
//
// The operand is an endpoint id, in hex or z-base-32, or a ticket, whose
// addresses are ignored: only its id is looked up.
//
// -source pkarr, the default, fetches the endpoint's signed packet from the
// number0 pkarr relay, or from the relay at -pkarr-url. -source dns queries
// the TXT records under _iroh.<z32-id>.<origin> in DNS, where the origin is
// number0's discovery domain unless -origin names another. -source all asks
// both at once. -staging selects number0's staging relay and origin instead
// of the production ones, so it excludes -pkarr-url and -origin.
//
// Each record found is printed as name and tab-separated value lines, with a
// blank line between records:
//
//	id          endpoint id in hex
//	z32         endpoint id in z-base-32
//	provenance  the source that answered: pkarr or dns
//	updated     when the source last updated the record, if it says
//	relay       a relay URL, one line each
//	addr        a direct IP address, one line each
//	user-data   the endpoint's user data, if any
//	ticket      a ticket holding the id and every address found
//
// Use -f to print one field's value alone, one line per value, from the first
// record found (pkarr before dns), so that it can be substituted into another
// command:
//
//	ginc "$(gilookup -f ticket <id>)"
//
// Use -json to print each record as a JSON object on its own line, with the
// fields id, z32, provenance, updated, relay_urls, ip_addrs, user_data, and
// ticket; relay_urls and ip_addrs are arrays, empty rather than null when
// there are none, and updated and user_data are omitted when unset. -f and
// -json are exclusive.
//
// A record with neither addresses nor user data counts as not found. With
// -source all, a source that fails is reported on standard error while the
// other's record is still printed.
//
// Gilookup exits zero if at least one record was found, 1 if none was found
// within -timeout or on any other error, and 2 if the flags cannot be parsed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/key"
)

// fields are the printable fields of a record, in output order.
var fields = []string{"id", "z32", "provenance", "updated", "relay", "addr", "user-data", "ticket"}

// sources are the lookup sources, in the order their records are printed.
var sources = []string{iroh.PkarrProvenance, iroh.DNSProvenance}

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stdout, os.Stderr, nil)
	})
}

// run looks up the endpoint named in args and prints what it finds to stdout,
// reporting the failure of one of several sources to stderr. Lookups in DNS use
// lookuper, or the system resolver if lookuper is nil.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, lookuper dns.TXTLookuper) error {
	fs := flag.NewFlagSet("gilookup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	source := fs.String("source", iroh.PkarrProvenance, "where to look: pkarr, dns, or all")
	staging := fs.Bool("staging", false, "use number0's staging pkarr relay and DNS origin")
	pkarrURL := fs.String("pkarr-url", "", "fetch from the pkarr relay at `url`")
	origin := fs.String("origin", "", "query DNS under `domain`")
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this long")
	field := fs.String("f", "", "print only this field's value ("+strings.Join(fields, ", ")+")")
	asJSON := fs.Bool("json", false, "print each record as a JSON object")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: gilookup [flags] <endpoint-id-or-ticket>\n")
		fs.PrintDefaults()
	}
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	var names []string
	switch *source {
	case "all":
		names = sources
	case iroh.PkarrProvenance, iroh.DNSProvenance:
		names = []string{*source}
	default:
		return fmt.Errorf("unknown source %q; want pkarr, dns, or all", *source)
	}
	switch {
	case *field != "" && !slices.Contains(fields, *field):
		return fmt.Errorf("unknown field %q; want one of %s", *field, strings.Join(fields, ", "))
	case *field != "" && *asJSON:
		return errors.New("-f and -json are mutually exclusive")
	case *timeout <= 0:
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	case *staging && *pkarrURL != "":
		return errors.New("-staging and -pkarr-url are mutually exclusive")
	case *staging && *origin != "":
		return errors.New("-staging and -origin are mutually exclusive")
	case *pkarrURL != "" && !slices.Contains(names, iroh.PkarrProvenance):
		return errors.New("-pkarr-url applies only to -source pkarr or all")
	case *origin != "" && !slices.Contains(names, iroh.DNSProvenance):
		return errors.New("-origin applies only to -source dns or all")
	}
	if fs.NArg() != 1 {
		return errors.New("usage: gilookup [flags] <endpoint-id-or-ticket>")
	}
	addr, err := tool.ParseEndpoint(fs.Arg(0))
	if err != nil {
		return err
	}

	relayURL, dnsOrigin := iroh.N0DNSPkarrRelayProd, dns.N0DNSEndpointOriginProd
	if *staging {
		relayURL, dnsOrigin = iroh.N0DNSPkarrRelayStaging, dns.N0DNSEndpointOriginStaging
	}
	if *pkarrURL != "" {
		relayURL = *pkarrURL
	}
	if *origin != "" {
		dnsOrigin = *origin
	}
	resolvers := make([]resolver, len(names))
	for i, name := range names {
		switch name {
		case iroh.PkarrProvenance:
			r, err := iroh.NewPkarrResolver(relayURL, nil)
			if err != nil {
				return err
			}
			resolvers[i] = r
		case iroh.DNSProvenance:
			resolvers[i] = iroh.NewDNSAddressLookup(dnsOrigin, &dns.Resolver{Lookuper: lookuper})
		}
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	results := make([]chan result, len(resolvers))
	for i, r := range resolvers {
		results[i] = make(chan result, 1)
		go func() {
			results[i] <- lookup(ctx, r, addr.ID, *timeout)
		}()
	}
	var items []iroh.Item
	var errs []error
	for i, ch := range results {
		res := <-ch
		if res.err != nil {
			errs = append(errs, fmt.Errorf("lookup %s via %s: %w", addr.ID, names[i], res.err))
			continue
		}
		items = append(items, res.item)
	}
	if len(items) == 0 {
		return errors.Join(errs...)
	}
	for _, err := range errs {
		fmt.Fprintf(stderr, "gilookup: %v\n", err)
	}
	switch {
	case *field != "":
		return printField(stdout, items[0], *field)
	case *asJSON:
		return printJSON(stdout, items)
	}
	return printItems(stdout, items)
}

// resolver is the part of iroh's address lookup services gilookup uses.
type resolver interface {
	Resolve(context.Context, key.EndpointID) iter.Seq2[iroh.Item, error]
}

type result struct {
	item iroh.Item
	err  error
}

// lookup returns the first record r finds for id. The resolvers yield nothing
// once ctx is done, so an empty result after ctx expires is a timeout.
func lookup(ctx context.Context, r resolver, id key.EndpointID, timeout time.Duration) result {
	for item, err := range r.Resolve(ctx, id) {
		if err != nil {
			return result{err: err}
		}
		data := item.EndpointInfo().Data
		if !data.HasAddrs() && data.UserData() == nil {
			return result{err: errors.New("record is empty")}
		}
		return result{item: item}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return result{err: fmt.Errorf("no record within %s", timeout)}
	}
	if ctx.Err() != nil {
		return result{err: errors.New("interrupted")}
	}
	return result{err: errors.New("no record found")}
}

// values returns item's values for each field. Fields without a value are
// absent; relay and addr may have several.
func values(item iroh.Item) map[string][]string {
	id := item.EndpointID()
	v := map[string][]string{
		"id":         {id.String()},
		"z32":        {id.Z32()},
		"provenance": {item.Provenance()},
		"ticket":     {endpointticket.Encode(item.Addr())},
	}
	if t, ok := item.LastUpdatedTime(); ok {
		v["updated"] = []string{t.UTC().Format(time.RFC3339Nano)}
	}
	data := item.EndpointInfo().Data
	for _, u := range data.RelayURLs() {
		v["relay"] = append(v["relay"], u.String())
	}
	for _, ap := range data.IPAddrs() {
		v["addr"] = append(v["addr"], ap.String())
	}
	if u, ok := item.UserData(); ok {
		v["user-data"] = []string{u.String()}
	}
	return v
}

// printField writes one field's values from item, unlabeled, one per line.
func printField(w io.Writer, item iroh.Item, field string) error {
	for _, s := range values(item)[field] {
		if _, err := fmt.Fprintln(w, s); err != nil {
			return err
		}
	}
	return nil
}

// printItems writes each item's fields as names and tab-separated values,
// with a blank line between items.
func printItems(w io.Writer, items []iroh.Item) error {
	for i, item := range items {
		if i > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		v := values(item)
		for _, name := range fields {
			for _, s := range v[name] {
				if _, err := fmt.Fprintf(w, "%s\t%s\n", name, s); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// recordJSON is the -json output for one record.
type recordJSON struct {
	ID         string   `json:"id"`
	Z32        string   `json:"z32"`
	Provenance string   `json:"provenance"`
	Updated    string   `json:"updated,omitempty"`
	RelayURLs  []string `json:"relay_urls"`
	IPAddrs    []string `json:"ip_addrs"`
	UserData   string   `json:"user_data,omitempty"`
	Ticket     string   `json:"ticket"`
}

// printJSON writes each item as a JSON object on its own line.
func printJSON(w io.Writer, items []iroh.Item) error {
	enc := json.NewEncoder(w)
	for _, item := range items {
		v := values(item)
		rec := recordJSON{
			ID:         v["id"][0],
			Z32:        v["z32"][0],
			Provenance: v["provenance"][0],
			RelayURLs:  append([]string{}, v["relay"]...),
			IPAddrs:    append([]string{}, v["addr"]...),
			Ticket:     v["ticket"][0],
		}
		if s := v["updated"]; len(s) > 0 {
			rec.Updated = s[0]
		}
		if s := v["user-data"]; len(s) > 0 {
			rec.UserData = s[0]
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}
