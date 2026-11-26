// Giaddr prints the local endpoint's identity and dialable address.
//
// Each run binds an endpoint, prints it, and exits. Without -key the run
// generates a fresh keypair, so the identity and the ticket last only as long
// as the process: they name this invocation, not a service another host can
// come back to later. With -key file the identity is read from file (as
// written by gikey generate) and is the same on every run; the addresses in
// the ticket still describe only this run. Use -wait to hold the endpoint open
// while something dials it.
//
// Usage:
//
//	giaddr [flags]
//
// By default every field is printed as a name and a tab-separated value. Use
// -f to print one field's value alone, for substitution into another command.
// Use -json to print a single JSON object with the fields id, z32, local,
// addr, ip_addrs, relay_urls, and ticket; ip_addrs and relay_urls are arrays,
// empty rather than null when there are none. -f and -json are exclusive.
//
// Giaddr exits zero once it has printed the endpoint, or when -wait is cut
// short by a signal, and nonzero if the endpoint cannot be bound.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

// fields are the printable fields, in output order.
var fields = []string{"id", "z32", "local", "addr", "ticket"}

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stdout)
	})
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("giaddr", flag.ContinueOnError)
	var bind tool.BindFlags
	wait := fs.Duration("wait", 0, "keep the endpoint alive this long after printing")
	field := fs.String("f", "", "print only this field's value ("+strings.Join(fields, ", ")+")")
	asJSON := fs.Bool("json", false, "print a single JSON object")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q; giaddr takes no operands", fs.Arg(0))
	}
	if *field != "" && !slices.Contains(fields, *field) {
		return fmt.Errorf("unknown field %q; want one of %s", *field, strings.Join(fields, ", "))
	}
	if *field != "" && *asJSON {
		return errors.New("-f and -json are mutually exclusive")
	}
	if *wait < 0 {
		return fmt.Errorf("-wait must not be negative, got %s", *wait)
	}
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	if *asJSON {
		err = printJSON(stdout, ep)
	} else {
		err = printEndpoint(stdout, ep, *field)
	}
	if err != nil {
		return err
	}
	if *wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*wait):
		}
	}
	return nil
}

// printEndpoint writes ep's fields to w, or just the named one.
func printEndpoint(w io.Writer, ep *iroh.Endpoint, field string) error {
	values := map[string]string{
		"id":     ep.ID().String(),
		"z32":    ep.ID().Z32(),
		"local":  ep.LocalAddr().String(),
		"addr":   ep.Addr().String(),
		"ticket": tool.LocalTicket(ep),
	}
	if field != "" {
		_, err := fmt.Fprintln(w, values[field])
		return err
	}
	for _, name := range fields {
		if _, err := fmt.Fprintf(w, "%s\t%s\n", name, values[name]); err != nil {
			return err
		}
	}
	return nil
}

// endpointJSON is the -json output.
type endpointJSON struct {
	ID        string   `json:"id"`
	Z32       string   `json:"z32"`
	Local     string   `json:"local"`
	Addr      string   `json:"addr"`
	IPAddrs   []string `json:"ip_addrs"`
	RelayURLs []string `json:"relay_urls"`
	Ticket    string   `json:"ticket"`
}

// printJSON writes ep as one JSON object to w.
func printJSON(w io.Writer, ep *iroh.Endpoint) error {
	addr := ep.Addr()
	v := endpointJSON{
		ID:        ep.ID().String(),
		Z32:       ep.ID().Z32(),
		Local:     ep.LocalAddr().String(),
		Addr:      addr.String(),
		IPAddrs:   []string{},
		RelayURLs: []string{},
		Ticket:    tool.LocalTicket(ep),
	}
	for _, ap := range addr.IPAddrs() {
		v.IPAddrs = append(v.IPAddrs, ap.String())
	}
	for _, u := range addr.RelayURLs() {
		v.RelayURLs = append(v.RelayURLs, u.String())
	}
	return json.NewEncoder(w).Encode(v)
}
