package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
)

const defaultALPN = "iroh-tools/stream/1"

func main() {
	tool.Run(run)
}

func run(ctx context.Context) error {
	fs := flag.NewFlagSet("ginc", flag.ExitOnError)
	var bind tool.BindFlags
	alpn := fs.String("alpn", defaultALPN, "ALPN protocol")
	timeout := fs.Duration("timeout", 30*time.Second, "dial timeout")
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: ginc [flags] <endpoint-ticket-or-id>")
	}
	addr, err := tool.ParseEndpoint(fs.Arg(0))
	if err != nil {
		return err
	}
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	dctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	stream, err := ep.Dial(dctx, addr, *alpn)
	if err != nil {
		return err
	}
	defer stream.Close()
	return tool.CopyStdio(stream, os.Stdin, os.Stdout)
}
