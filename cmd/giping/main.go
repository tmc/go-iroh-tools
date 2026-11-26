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
// Each answered probe prints its sequence number and round-trip time, and the
// run ends with a loss summary. With -json giping instead prints a single JSON
// object when it finishes, with the fields remote, probes (the -c count),
// sent, received, and loss_pct, and, when any probe was answered, min_ns,
// avg_ns, and max_ns: round-trip times in nanoseconds. -json does not apply
// with -listen.
//
// Giping exits zero if at least one probe is answered, and nonzero if none is,
// so partial loss is reported in the summary rather than in the exit status.
// A run stopped by a signal reports what it completed and exits zero.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

const pingALPN = "iroh-tools/ping/1"

// probeSize is the fixed request and reply size: a sequence number and the
// sender's timestamp, both big-endian.
const probeSize = 16

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stdout)
	})
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("giping", flag.ContinueOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for iroh ping requests")
	count := fs.Int("c", 4, "number of probes to send")
	interval := fs.Duration("i", time.Second, "interval between probes")
	timeout := fs.Duration("timeout", 5*time.Second, "per-probe timeout")
	asJSON := fs.Bool("json", false, "print a single JSON summary at exit")
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
		if *asJSON {
			return errors.New("-json applies only when sending probes, not with -listen")
		}
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
	lines := stdout
	if *asJSON {
		lines = io.Discard
	}
	r := probe(ctx, conn, *count, *interval, *timeout, lines)
	if *asJSON {
		err = r.writeJSON(stdout)
	} else {
		err = r.writeSummary(stdout)
	}
	if err != nil {
		return err
	}
	return r.err()
}

// result is the outcome of a run of probes.
type result struct {
	remote         fmt.Stringer
	probes         int             // probes requested
	sent, received int             // probes sent and answered
	rtts           []time.Duration // round-trip time of each answered probe
}

// probe sends count probes, writing a line for each to w, and returns what
// happened. A cancelled ctx ends the run early with the probes completed so
// far.
func probe(ctx context.Context, conn *iroh.Conn, count int, interval, timeout time.Duration, w io.Writer) result {
	r := result{remote: conn.RemoteID(), probes: count}
	for seq := range count {
		if seq > 0 {
			select {
			case <-ctx.Done():
				return r
			case <-time.After(interval):
			}
		}
		start := time.Now()
		pctx, cancel := context.WithTimeout(ctx, timeout)
		err := ping(pctx, conn, uint64(seq), uint64(start.UnixNano()))
		cancel()
		rtt := time.Since(start)
		r.sent++
		if err != nil {
			if ctx.Err() != nil {
				return r
			}
			fmt.Fprintf(w, "seq=%d error=%v\n", seq, err)
			continue
		}
		r.received++
		r.rtts = append(r.rtts, rtt)
		fmt.Fprintf(w, "seq=%d time=%s\n", seq, rtt.Round(time.Microsecond))
	}
	return r
}

// lossPct reports the percentage of sent probes that went unanswered, or 100
// if none was sent.
func (r result) lossPct() float64 {
	if r.sent == 0 {
		return 100
	}
	return float64(r.sent-r.received) / float64(r.sent) * 100
}

// err reports whether the run found a peer; see the package comment for the
// exit policy.
func (r result) err() error {
	if r.sent > 0 && r.received == 0 {
		return fmt.Errorf("no reply from %s", r.remote)
	}
	return nil
}

// writeSummary writes the text loss summary to w.
func (r result) writeSummary(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%d sent, %d received, %.0f%% loss\n", r.sent, r.received, r.lossPct())
	return err
}

// resultJSON is the -json output.
type resultJSON struct {
	Remote   string  `json:"remote"`
	Probes   int     `json:"probes"`
	Sent     int     `json:"sent"`
	Received int     `json:"received"`
	LossPct  float64 `json:"loss_pct"`
	MinNS    *int64  `json:"min_ns,omitempty"`
	AvgNS    *int64  `json:"avg_ns,omitempty"`
	MaxNS    *int64  `json:"max_ns,omitempty"`
}

// writeJSON writes r to w as one JSON object.
func (r result) writeJSON(w io.Writer) error {
	v := resultJSON{
		Remote:   r.remote.String(),
		Probes:   r.probes,
		Sent:     r.sent,
		Received: r.received,
		LossPct:  r.lossPct(),
	}
	if len(r.rtts) > 0 {
		lo, hi := slices.Min(r.rtts), slices.Max(r.rtts)
		var sum time.Duration
		for _, d := range r.rtts {
			sum += d
		}
		minNS, maxNS := int64(lo), int64(hi)
		avgNS := int64(sum) / int64(len(r.rtts))
		v.MinNS, v.AvgNS, v.MaxNS = &minNS, &avgNS, &maxNS
	}
	return json.NewEncoder(w).Encode(v)
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
