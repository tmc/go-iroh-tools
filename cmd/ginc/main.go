// Ginc connects to an iroh endpoint and bridges the stream to stdin and stdout.
//
// Usage:
//
//	ginc [flags] <endpoint-ticket-or-id>
//
// Ginc is the dialing half of a pipe whose listening half is gilisten:
//
//	gilisten -1 >out 2>ticket &
//	ginc "$(cat ticket)" <in
//
// Both directions run at once, so a payload of any size can be sent while the
// peer replies. EOF on stdin half-closes the send direction only: the peer
// sees the end of the request and its reply is still read, which is what lets
// a request/response exchange work over a pipe. Ginc exits when the peer
// closes its own send direction.
//
// An interrupt ends the transfer and exits zero. Bytes already written to
// stdout are kept; ginc makes no promise that a stream ended this way is
// complete.
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

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ginc", flag.ContinueOnError)
	var bind tool.BindFlags
	alpn := fs.String("alpn", defaultALPN, "ALPN protocol")
	timeout := fs.Duration("timeout", 30*time.Second, "dial timeout")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
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
	conn, err := ep.Connect(dctx, addr, *alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	// Both directions run concurrently whether stdin is a terminal or a pipe.
	// Uploading first and only then reading the reply deadlocks as soon as the
	// payload exceeds the peer's flow-control window: the peer stops reading
	// to drain a send buffer nobody is receiving, and both sides wait.
	return tool.CopyStdio(ctx, stream, os.Stdin, os.Stdout)
}
