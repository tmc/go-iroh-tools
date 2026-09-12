// Giticket builds and inspects endpoint tickets.
//
// Usage:
//
//	giticket [-short] [-f field] <ticket>
//	giticket [-short] -id <endpoint-id> [-addr <transport-addr>]...
//
// Inspecting a ticket prints every field as a name and a value.
// Use -f to print one field's value alone, which is how a shortened ticket is
// obtained in a form another command can take directly:
//
//	giticket -short -f ticket "$TICKET"
//
// A short ticket keeps only relay and discovery addresses, dropping direct IP
// and custom addresses, so it stays valid when the endpoint's direct addresses
// change.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/netaddr"
)

// fields are the printable fields of a ticket, in output order.
var fields = []string{"id", "z32", "addr", "ticket", "short"}

type addrFlags []string

func (f *addrFlags) String() string { return strings.Join(*f, ",") }

func (f *addrFlags) Set(s string) error {
	*f = append(*f, s)
	return nil
}

func main() {
	var idText string
	var addrs addrFlags
	var short bool
	var field string
	fs := flag.NewFlagSet("giticket", flag.ExitOnError)
	fs.StringVar(&idText, "id", "", "endpoint id for a new ticket")
	fs.Var(&addrs, "addr", "transport address to include, such as ip:127.0.0.1:1234 or relay:https://relay.example./")
	fs.BoolVar(&short, "short", false, "drop direct IP and custom addresses")
	fs.StringVar(&field, "f", "", "print only this field's value ("+strings.Join(fields, ", ")+")")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: giticket [-short] [-f field] <ticket>\n")
		fmt.Fprintf(fs.Output(), "       giticket [-short] -id <endpoint-id> [-addr <transport-addr>]...\n")
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	tool.Run(func(context.Context) error {
		return run(os.Stdout, fs.Args(), idText, addrs, short, field)
	})
}

func run(w io.Writer, args []string, idText string, addrTexts []string, short bool, field string) error {
	if field != "" && !slices.Contains(fields, field) {
		return fmt.Errorf("unknown field %q; want one of %s", field, strings.Join(fields, ", "))
	}
	if idText != "" {
		if len(args) != 0 {
			return fmt.Errorf("unexpected argument %q; -id builds a ticket from flags alone", args[0])
		}
		addr, err := buildAddr(idText, addrTexts)
		if err != nil {
			return err
		}
		if short {
			addr = endpointticket.ShortAddr(addr)
		}
		if field != "" {
			return printField(w, addr, field)
		}
		_, err = fmt.Fprintln(w, endpointticket.Encode(addr))
		return err
	}
	// Inspect mode builds nothing, so -addr would be silently discarded.
	if len(addrTexts) != 0 {
		return errors.New("-addr applies only with -id")
	}
	if len(args) != 1 {
		return errors.New("usage: giticket [-short] [-f field] <ticket>")
	}
	addr, err := endpointticket.Decode(args[0])
	if err != nil {
		return fmt.Errorf("decode ticket: %w", err)
	}
	if short {
		addr = endpointticket.ShortAddr(addr)
	}
	if field != "" {
		return printField(w, addr, field)
	}
	return printAddr(w, addr)
}

func buildAddr(idText string, addrTexts []string) (netaddr.EndpointAddr, error) {
	id, err := tool.ParseEndpointID(idText)
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	addrs := make([]netaddr.TransportAddr, 0, len(addrTexts))
	for _, text := range addrTexts {
		addr, err := netaddr.ParseTransportAddr(text)
		if err != nil {
			return netaddr.EndpointAddr{}, fmt.Errorf("parse transport address: %w", err)
		}
		addrs = append(addrs, addr)
	}
	return netaddr.NewEndpointAddr(id, addrs...), nil
}

// printField writes one field's value, unlabeled, for use by another command.
// The addr field can hold several values and prints one per line.
func printField(w io.Writer, addr netaddr.EndpointAddr, field string) error {
	if field == "addr" {
		for _, a := range addr.Addrs() {
			if _, err := fmt.Fprintln(w, a); err != nil {
				return err
			}
		}
		return nil
	}
	values := map[string]string{
		"id":     addr.ID.String(),
		"z32":    addr.ID.Z32(),
		"ticket": endpointticket.Encode(addr),
		"short":  endpointticket.Encode(endpointticket.ShortAddr(addr)),
	}
	_, err := fmt.Fprintln(w, values[field])
	return err
}

func printAddr(w io.Writer, addr netaddr.EndpointAddr) error {
	if _, err := fmt.Fprintf(w, "id %s\n", addr.ID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "z32 %s\n", addr.ID.Z32()); err != nil {
		return err
	}
	for _, a := range addr.Addrs() {
		if _, err := fmt.Fprintf(w, "addr %s\n", a); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "ticket %s\n", endpointticket.Encode(addr)); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "short %s\n", endpointticket.Encode(endpointticket.ShortAddr(addr)))
	return err
}
