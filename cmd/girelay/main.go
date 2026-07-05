package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	var staging bool
	fs := flag.NewFlagSet("girelay", flag.ExitOnError)
	fs.BoolVar(&staging, "staging", false, "print staging relay map")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: girelay [-staging] [relay-url...]\n")
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	tool.Run(func(context.Context) error {
		return run(os.Stdout, fs.Args(), staging)
	})
}

func run(w io.Writer, args []string, staging bool) error {
	if len(args) == 0 {
		m := relay.DefaultMap()
		if staging {
			m = relay.StagingMap()
		}
		printMap(w, m)
		return nil
	}
	for _, text := range args {
		u, err := netaddr.ParseRelayURL(text)
		if err != nil {
			return fmt.Errorf("parse relay URL: %w", err)
		}
		fmt.Fprintln(w, u)
	}
	return nil
}

func printMap(w io.Writer, m *relay.Map) {
	for _, u := range m.URLs() {
		fmt.Fprintln(w, u)
	}
}
