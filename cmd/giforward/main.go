// Giforward forwards TCP connections across an iroh connection.
//
// Usage:
//
//	giforward -listen -to <host:port> [flags]
//	giforward -from <host:port> [flags] <endpoint-ticket-or-id>
//
// The -listen side accepts any number of iroh connections and, for every
// stream opened on them, dials the TCP address given by -to and copies bytes
// both ways. It prints its endpoint ticket to stderr once it is ready.
//
// The -from side makes one iroh connection to the listener, listens on the
// local TCP address given by -from, and opens one stream for every TCP
// connection it accepts. It prints the bound TCP address to stderr once both
// the iroh connection and the TCP listener are up, so "-from 127.0.0.1:0"
// reports the port it chose.
//
// For example, to reach an SSH server through a peer:
//
//	giforward -listen -to 127.0.0.1:22 2>ticket &
//	giforward -from 127.0.0.1:2222 "$(cat ticket)" &
//	ssh -p 2222 127.0.0.1
//
// Both directions run at once and each propagates half-close: when one side
// stops sending, the other sees EOF and may still reply. The dialing side
// writes a one-byte header on every stream, so a stream reaches the listener
// as soon as its TCP connection is accepted, before the client sends
// anything; protocols in which the server speaks first, such as SSH and
// SMTP, work unchanged.
//
// A failure on one forwarded connection is logged to stderr and does not stop
// the forwarder. An interrupt closes every forwarded connection and exits
// zero. The -from side exits nonzero if its iroh connection is lost.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const forwardALPN = "iroh-tools/forward/1"

// streamHeader is the first byte the dialing side writes on every stream. A
// bidirectional stream reaches the peer only with its first bytes, so without
// it the listener would not dial the target until the TCP client spoke. It
// doubles as a version number for the stream format.
const streamHeader = 1

// abortCode is the application error code used when a forwarded connection is
// torn down. The tools assign no meaning to individual codes.
const abortCode = 0

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stderr)
	})
}

// run parses args and runs the forwarder. The ticket (-listen) or the bound
// TCP address (-from) is written to stderr as the first line, followed by any
// per-connection errors.
func run(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("giforward", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "accept iroh streams and forward them to -to")
	to := fs.String("to", "", "with -listen, forward each stream to TCP `host:port`")
	from := fs.String("from", "", "listen on TCP `host:port` and forward each connection to the peer")
	timeout := fs.Duration("timeout", 30*time.Second, "dial timeout")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	}
	switch {
	case *listen && *from != "":
		return errors.New("-listen and -from are mutually exclusive")
	case *listen:
		if *to == "" {
			return errors.New("-listen requires -to")
		}
		if fs.NArg() != 0 {
			return fmt.Errorf("unexpected argument %q; -listen takes no operands", fs.Arg(0))
		}
		return serve(ctx, bind, *to, *timeout, stderr)
	case *from != "":
		if *to != "" {
			return errors.New("-to applies only with -listen")
		}
		if fs.NArg() != 1 {
			return errors.New("usage: giforward -from <host:port> [flags] <endpoint-ticket-or-id>")
		}
		addr, err := tool.ParseEndpoint(fs.Arg(0))
		if err != nil {
			return err
		}
		return dial(ctx, bind, *from, addr, *timeout, stderr)
	}
	return errors.New("one of -listen or -from is required")
}

// serve accepts iroh connections and forwards each of their streams to the
// TCP address target until ctx is done.
func serve(ctx context.Context, bind tool.BindFlags, target string, timeout time.Duration, stderr io.Writer) error {
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	logf := logger(stderr)
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		defer conn.Close()
		var wg sync.WaitGroup
		defer wg.Wait()
		for {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				if ctx.Err() != nil || conn.Context().Err() != nil || errors.Is(err, net.ErrClosed) {
					return nil
				}
				logf("accept stream from %s: %v", conn.RemoteID(), err)
				return nil
			}
			wg.Go(func() {
				if err := forwardOut(ctx, stream, target, timeout); err != nil {
					logf("forward %s to %s: %v", conn.RemoteID(), target, err)
				}
			})
		}
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{forwardALPN: handler}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(context.Background())
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(stderr, tool.LocalTicket(ep)); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// forwardOut reads the stream header and bridges stream to a new TCP
// connection to target.
func forwardOut(ctx context.Context, stream *iroh.Stream, target string, timeout time.Duration) error {
	var hdr [1]byte
	if _, err := io.ReadFull(stream, hdr[:]); err != nil {
		abort(stream)
		return fmt.Errorf("read stream header: %w", err)
	}
	if hdr[0] != streamHeader {
		abort(stream)
		return fmt.Errorf("unknown stream header %#x", hdr[0])
	}
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		abort(stream)
		return err
	}
	return bridge(ctx, stream, c.(*net.TCPConn))
}

// dial connects to addr and forwards every TCP connection accepted on local
// to it, one stream per connection, until ctx is done or the iroh connection
// is lost.
func dial(ctx context.Context, bind tool.BindFlags, local string, addr netaddr.EndpointAddr, timeout time.Duration, stderr io.Writer) error {
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	dctx, cancel := context.WithTimeout(ctx, timeout)
	conn, err := ep.Connect(dctx, addr, forwardALPN)
	cancel()
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr.ID, err)
	}
	defer conn.Close()
	ln, err := net.Listen("tcp", local)
	if err != nil {
		return err
	}
	defer ln.Close()
	// Closing the listener is what ends the accept loop, on either
	// cancellation or a lost connection.
	defer context.AfterFunc(conn.Context(), func() { ln.Close() })()
	defer context.AfterFunc(ctx, func() { ln.Close() })()
	if _, err := fmt.Fprintln(stderr, ln.Addr()); err != nil {
		return err
	}

	logf := logger(stderr)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if conn.Context().Err() != nil {
				return fmt.Errorf("connection to %s lost", addr.ID)
			}
			return fmt.Errorf("accept: %w", err)
		}
		wg.Go(func() {
			if err := forwardIn(ctx, conn, c.(*net.TCPConn)); err != nil {
				logf("forward %s to %s: %v", c.RemoteAddr(), addr.ID, err)
			}
		})
	}
}

// forwardIn opens a stream on conn and bridges c to it.
func forwardIn(ctx context.Context, conn *iroh.Conn, c *net.TCPConn) error {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		c.Close()
		return fmt.Errorf("open stream: %w", err)
	}
	if _, err := stream.Write([]byte{streamHeader}); err != nil {
		c.Close()
		abort(stream)
		return fmt.Errorf("write stream header: %w", err)
	}
	return bridge(ctx, stream, c)
}

// bridge copies between stream and c in both directions until both have
// finished, then closes c. EOF in either direction is passed on as a
// half-close. If either copy fails, or ctx is done, both directions are torn
// down and bridge reports why.
//
// A stream that finished cleanly is left alone: both of its directions are
// already closed, and cancelling the send direction now could reset data the
// peer has not yet acknowledged.
func bridge(ctx context.Context, stream *iroh.Stream, c *net.TCPConn) error {
	defer c.Close()
	var once sync.Once
	teardown := func() {
		once.Do(func() {
			c.Close()
			abort(stream)
		})
	}
	defer context.AfterFunc(ctx, teardown)()

	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(stream, c)
		if err == nil {
			err = stream.CloseWrite()
		}
		errc <- err
	}()
	go func() {
		_, err := io.Copy(c, stream)
		if err == nil {
			err = c.CloseWrite()
		}
		errc <- err
	}()
	var first error
	for range 2 {
		if err := <-errc; err != nil && first == nil {
			first = err
			teardown()
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return first
}

// abort cancels both directions of stream, which unblocks any goroutine
// reading or writing it.
func abort(stream *iroh.Stream) {
	stream.CancelRead(abortCode)
	stream.CancelWrite(abortCode)
}

// logger returns a function that writes one prefixed line to w per call.
// Forwarded connections log concurrently, so writes are serialized.
func logger(w io.Writer) func(format string, args ...any) {
	var mu sync.Mutex
	return func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(w, "giforward: "+format+"\n", args...)
	}
}
