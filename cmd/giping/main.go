// Giping measures application stream round-trip time to an iroh endpoint.
//
// Each probe opens a stream, writes a 16-byte request, and waits for the peer
// to echo it back. The reported time therefore covers opening a stream and
// having the peer's application serve it, not just packet propagation: it is
// not an ICMP echo and will read higher than ping(8) against the same host.
//
// Usage:
//
//	giping [flags] <endpoint-ticket-or-id>
//	giping -listen [flags]
//
// Giping exits zero if at least one probe is answered, and nonzero if none is,
// so partial loss is reported in the summary rather than in the exit status.
// A run stopped by a signal reports what it completed and exits zero.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

const pingALPN = "iroh-tools/ping/1"

// probeSize is the fixed request and reply size: a sequence number and the
// sender's timestamp, both big-endian.
const probeSize = 16

func main() {
	tool.Run(run)
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("giping", flag.ContinueOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for iroh ping requests")
	count := fs.Int("c", 4, "number of probes to send")
	interval := fs.Duration("i", time.Second, "interval between probes")
	timeout := fs.Duration("timeout", 5*time.Second, "per-probe timeout")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	// Validate every flag before binding an endpoint, so a typo fails
	// immediately instead of after opening a socket.
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	}
	if *listen {
		if fs.NArg() != 0 {
			return fmt.Errorf("unexpected argument %q; -listen takes no operands", fs.Arg(0))
		}
		return serve(ctx, bind)
	}
	if *count <= 0 {
		return fmt.Errorf("-c must be positive, got %d", *count)
	}
	if *interval < 0 {
		return fmt.Errorf("-i must not be negative, got %s", *interval)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: giping [flags] <endpoint-ticket-or-id>")
	}
	addr, err := tool.ParseEndpoint(fs.Arg(0))
	if err != nil {
		return err
	}
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	dctx, cancel := context.WithTimeout(ctx, *timeout)
	conn, err := ep.Connect(dctx, addr, pingALPN)
	cancel()
	if err != nil {
		// A failure here is a dial failure: no probe was ever sent. Say so,
		// rather than letting it read like an unanswered probe.
		return fmt.Errorf("dial %s: %w", addr.ID, err)
	}
	defer conn.Close()
	return probe(ctx, conn, *count, *interval, *timeout)
}

// probe sends count probes and reports a summary. It fails only if no probe is
// answered; see the package comment for the exit policy.
func probe(ctx context.Context, conn *iroh.Conn, count int, interval, timeout time.Duration) error {
	var sent, received int
	for seq := range count {
		if seq > 0 {
			select {
			case <-ctx.Done():
				return summarize(sent, received, conn.RemoteID())
			case <-time.After(interval):
			}
		}
		start := time.Now()
		pctx, cancel := context.WithTimeout(ctx, timeout)
		err := ping(pctx, conn, uint64(seq), uint64(start.UnixNano()))
		cancel()
		sent++
		if err != nil {
			if ctx.Err() != nil {
				return summarize(sent, received, conn.RemoteID())
			}
			fmt.Printf("seq=%d error=%v\n", seq, err)
			continue
		}
		received++
		fmt.Printf("seq=%d time=%s\n", seq, time.Since(start).Round(time.Microsecond))
	}
	return summarize(sent, received, conn.RemoteID())
}

// summarize prints the loss summary and reports whether the run found a peer.
func summarize(sent, received int, remote fmt.Stringer) error {
	loss := 100.0
	if sent > 0 {
		loss = float64(sent-received) / float64(sent) * 100
	}
	fmt.Printf("%d sent, %d received, %.0f%% loss\n", sent, received, loss)
	if sent > 0 && received == 0 {
		return fmt.Errorf("no reply from %s", remote)
	}
	return nil
}

func serve(ctx context.Context, bind tool.BindFlags) error {
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		for {
			stream, err := conn.AcceptStreamConn(ctx)
			if err != nil {
				if ctx.Err() != nil || conn.Context().Err() != nil || errors.Is(err, net.ErrClosed) {
					return nil
				}
				return err
			}
			if err := serveProbe(stream); err != nil {
				fmt.Fprintf(os.Stderr, "giping: serve probe: %v\n", err)
			}
		}
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{pingALPN: handler}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(context.Background())
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, tool.LocalTicket(ep))
	<-ctx.Done()
	return ctx.Err()
}

// serveProbe echoes one request. A malformed or truncated request ends this
// stream only; the connection stays up for the next probe.
func serveProbe(stream net.Conn) error {
	defer stream.Close()
	var buf [probeSize]byte
	if _, err := io.ReadFull(stream, buf[:]); err != nil {
		return err
	}
	_, err := stream.Write(buf[:])
	return err
}

// ping sends one probe and waits for its echo.
//
// ctx opens the stream but is not attached to the stream's I/O by the iroh
// package, so ping carries the deadline across explicitly and closes the
// stream if ctx is cancelled before that deadline. Without both, a peer that
// accepts a request and never replies leaves Read blocked past the timeout.
func ping(ctx context.Context, conn *iroh.Conn, seq, stamp uint64) error {
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := stream.SetDeadline(deadline); err != nil {
			return err
		}
	}
	// Closing the stream is what unblocks an in-flight Read or Write; the
	// net.Conn adapter cancels both directions on Close.
	defer context.AfterFunc(ctx, func() { stream.Close() })()

	var req [probeSize]byte
	binary.BigEndian.PutUint64(req[0:8], seq)
	binary.BigEndian.PutUint64(req[8:16], stamp)
	if _, err := stream.Write(req[:]); err != nil {
		return err
	}
	var resp [probeSize]byte
	if _, err := io.ReadFull(stream, resp[:]); err != nil {
		return err
	}
	if resp != req {
		return errors.New("reply did not match request")
	}
	return nil
}
