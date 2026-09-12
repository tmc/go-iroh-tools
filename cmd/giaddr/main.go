// Giaddr prints the local endpoint's identity and dialable address.
//
// Each run generates a fresh keypair, binds an endpoint, prints it, and exits.
// The identity and the ticket therefore last only as long as the process: they
// name this invocation, not a service another host can come back to later. Use
// -wait to hold the endpoint open while something dials it.
//
// Usage:
//
//	giaddr [flags]
//
// By default every field is printed as a name and a tab-separated value. Use
// -f to print one field's value alone, for substitution into another command.
package main

import (
	"context"
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
	tool.Run(run)
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("giaddr", flag.ContinueOnError)
	var bind tool.BindFlags
	wait := fs.Duration("wait", 0, "keep the endpoint alive this long after printing")
	field := fs.String("f", "", "print only this field's value ("+strings.Join(fields, ", ")+")")
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
	if *wait < 0 {
		return fmt.Errorf("-wait must not be negative, got %s", *wait)
	}
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	if err := printEndpoint(os.Stdout, ep, *field); err != nil {
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
