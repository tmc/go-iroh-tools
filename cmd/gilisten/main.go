// Gilisten accepts iroh connections and bridges one to stdin and stdout, or
// echoes every stream it is sent.
//
// It prints an endpoint ticket to stderr once the protocol is registered and
// the endpoint is reachable; a client that dials on seeing that line will find
// a listener ready. Payload bytes go to stdout, so the ticket and the data can
// be redirected separately.
//
// Usage:
//
//	gilisten -1 [flags]          bridge one connection to stdin and stdout
//	gilisten -echo [flags]       echo every stream, until interrupted
//	gilisten -echo -1 [flags]    echo the streams of one connection
//
// The stdio bridge requires -1: stdin cannot be split across concurrent
// clients. With -1 the command admits exactly one connection and exits when
// that connection ends.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

const defaultALPN = "iroh-tools/stream/1"

func main() {
	tool.Run(run)
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gilisten", flag.ContinueOnError)
	var bind tool.BindFlags
	alpn := fs.String("alpn", defaultALPN, "ALPN protocol")
	echo := fs.Bool("echo", false, "echo received bytes instead of bridging stdin/stdout")
	once := fs.Bool("1", false, "admit one connection and exit when it ends")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q; gilisten takes no operands", fs.Arg(0))
	}
	if !*echo && !*once {
		return fmt.Errorf("stdio bridge requires -1; use -echo for multi-connection mode")
	}
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	if *once {
		return serveOnce(ctx, ep, bind, *alpn, *echo, os.Stderr)
	}
	return serveMany(ctx, ep, bind, *alpn, os.Stderr)
}

// serveOnce admits exactly one connection and serves it.
//
// It accepts directly from the endpoint rather than running a router. A router
// starts a handler per connection, so it can only decline a second client
// after admitting it, and its handlers would each reach for the same stdin and
// stdout. Accept hands back one connection and leaves the next dialer
// unanswered, which is what -1 advertises. It also takes a context, so an
// interrupt arriving before any client does still ends the wait.
func serveOnce(ctx context.Context, ep *iroh.Endpoint, bind tool.BindFlags, alpn string, echo bool, ticket io.Writer) error {
	if err := ep.SetALPNs([]string{alpn}); err != nil {
		return fmt.Errorf("register alpn: %w", err)
	}
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(ticket, tool.LocalTicket(ep)); err != nil {
		return err
	}
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return serveConn(ctx, conn, echo)
}

// serveMany echoes the streams of every connection until ctx is done.
func serveMany(ctx context.Context, ep *iroh.Endpoint, bind tool.BindFlags, alpn string, ticket io.Writer) error {
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		if err := serveConn(ctx, conn, true); err != nil {
			fmt.Fprintf(os.Stderr, "gilisten: %v\n", err)
			return err
		}
		return nil
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{alpn: handler}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(context.Background())
	// The router registers the ALPN before NewRouter returns, so the ticket
	// printed here does not promise readiness the endpoint does not have.
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(ticket, tool.LocalTicket(ep)); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// serveConn serves the streams of one connection. The stdio bridge serves a
// single stream, because it hands that stream the process's stdin; echo mode
// keeps serving until the peer closes the connection.
func serveConn(ctx context.Context, conn *iroh.Conn, echo bool) error {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if closed(conn, err) {
				return nil
			}
			return fmt.Errorf("accept stream: %w", err)
		}
		if !echo {
			return tool.CopyStdio(ctx, stream, os.Stdin, os.Stdout)
		}
		if _, err := tool.Echo(stream); err != nil {
			return fmt.Errorf("echo stream: %w", err)
		}
	}
}

// closed reports whether err ends the stream loop because the peer finished
// with the connection rather than because something failed.
func closed(conn *iroh.Conn, err error) bool {
	return conn.Context().Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}
