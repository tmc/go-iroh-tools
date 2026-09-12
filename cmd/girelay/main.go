// Girelay prints relay URLs.
//
// Usage:
//
//	girelay [-staging]            print the configured relay map
//	girelay <relay-url>...        print each URL in normalized form
//
// Both forms are offline. Printing the relay map reports what this build is
// configured to use, and normalizing a URL is pure string work; neither
// contacts a relay, so neither says anything about whether a relay is running
// or reachable.
package main

import (
	"context"
	"errors"
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
	fs := flag.NewFlagSet("girelay", flag.ContinueOnError)
	fs.BoolVar(&staging, "staging", false, "print the staging relay map")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: girelay [-staging]\n")
		fmt.Fprintf(fs.Output(), "       girelay <relay-url>...\n")
		fs.PrintDefaults()
	}
	tool.Run(func(_ context.Context, args []string) error {
		if err := tool.ParseFlags(fs, args); err != nil {
			return err
		}
		return run(os.Stdout, fs.Args(), staging)
	})
}

func run(w io.Writer, args []string, staging bool) error {
	if len(args) == 0 {
		m := relay.DefaultMap()
		if staging {
			m = relay.StagingMap()
		}
		return printMap(w, m)
	}
	// -staging selects a built-in map, so it has nothing to say about URLs
	// given on the command line.
	if staging {
		return errors.New("-staging applies only when printing the relay map")
	}
	for _, text := range args {
		u, err := netaddr.ParseRelayURL(text)
		if err != nil {
			return fmt.Errorf("parse relay URL: %w", err)
		}
		if _, err := fmt.Fprintln(w, u); err != nil {
			return err
		}
	}
	return nil
}

func printMap(w io.Writer, m *relay.Map) error {
	for _, u := range m.URLs() {
		if _, err := fmt.Fprintln(w, u); err != nil {
			return err
		}
	}
	return nil
}
