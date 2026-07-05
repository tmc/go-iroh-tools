package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

type addrFlags []string

func (f *addrFlags) String() string { return fmt.Sprint([]string(*f)) }

func (f *addrFlags) Set(s string) error {
	*f = append(*f, s)
	return nil
}

func main() {
	var idText string
	var addrs addrFlags
	var short bool
	fs := flag.NewFlagSet("giticket", flag.ExitOnError)
	fs.StringVar(&idText, "id", "", "endpoint id for a new ticket")
	fs.Var(&addrs, "addr", "transport address to include, such as ip:127.0.0.1:1234 or relay:https://relay.example./")
	fs.BoolVar(&short, "short", false, "drop direct IP and custom addresses")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: giticket [-short] <ticket>\n")
		fmt.Fprintf(fs.Output(), "       giticket [-short] -id <endpoint-id> [-addr <transport-addr>]...\n")
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	tool.Run(func(context.Context) error {
		return run(os.Stdout, fs.Args(), idText, addrs, short)
	})
}

func run(w io.Writer, args []string, idText string, addrTexts []string, short bool) error {
	if idText != "" {
		if len(args) != 0 {
			return fmt.Errorf("extra arguments with -id")
		}
		addr, err := buildAddr(idText, addrTexts)
		if err != nil {
			return err
		}
		if short {
			addr = endpointticket.ShortAddr(addr)
		}
		fmt.Fprintln(w, endpointticket.Encode(addr))
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: giticket [-short] <ticket>")
	}
	addr, err := endpointticket.Decode(args[0])
	if err != nil {
		return fmt.Errorf("decode ticket: %w", err)
	}
	if short {
		addr = endpointticket.ShortAddr(addr)
	}
	printAddr(w, addr)
	return nil
}

func buildAddr(idText string, addrTexts []string) (netaddr.EndpointAddr, error) {
	id, err := key.ParseEndpointID(idText)
	if err != nil {
		return netaddr.EndpointAddr{}, fmt.Errorf("parse endpoint id: %w", err)
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

func printAddr(w io.Writer, addr netaddr.EndpointAddr) {
	fmt.Fprintf(w, "id %s\n", addr.ID)
	fmt.Fprintf(w, "z32 %s\n", addr.ID.Z32())
	for _, a := range addr.Addrs() {
		fmt.Fprintf(w, "addr %s\n", a)
	}
	fmt.Fprintf(w, "ticket %s\n", endpointticket.Encode(addr))
	fmt.Fprintf(w, "short %s\n", endpointticket.Encode(endpointticket.ShortAddr(addr)))
}
