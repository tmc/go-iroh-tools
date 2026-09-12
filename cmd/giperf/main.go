// Giperf measures one-way throughput to an iroh endpoint.
//
// The client sends a length header followed by that many zero bytes; the
// server counts what it receives and replies with a confirmation carrying its
// own count. The client reports success only when that count matches what it
// asked to send, so a reported transfer means the receiving application
// consumed the bytes, not merely that the local writes returned.
//
// Two durations are reported because they measure different things. write is
// the time until the last payload byte was handed to the transport, which is
// what a send-side benchmark sees; total runs until the receiver's
// confirmation arrives, and so includes the drain of everything still in
// flight plus one round trip. total is the honest end-to-end figure.
//
// Usage:
//
//	giperf [flags] <endpoint-ticket-or-id>
//	giperf -listen [flags]
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

// perfALPN is version 2 of the protocol. Version 1 had no confirmation
// message, so a version 1 server leaves a version 2 client waiting for a reply
// that will never come; the ALPN change makes that mismatch a failed dial
// instead of a hang.
const perfALPN = "iroh-tools/perf/2"

const (
	// headerSize is the length of the request header: the payload size as a
	// big-endian uint64.
	headerSize = 8
	// replySize is the length of the confirmation: a big-endian status code
	// followed by the number of payload bytes the receiver consumed.
	replySize = 16

	// statusOK reports that the receiver consumed the whole payload.
	statusOK = 0
	// statusShort reports that the payload ended early.
	statusShort = 1

	// maxTransfer caps a single transfer. It bounds what a peer can ask this
	// process to read, and is far above any useful measurement; it is a
	// sanity limit, not a protocol constant.
	maxTransfer = 1 << 40
)

func main() {
	tool.Run(run)
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("giperf", flag.ContinueOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for throughput tests")
	n := fs.String("n", "64MiB", "bytes to send")
	alpn := fs.String("alpn", perfALPN, "ALPN protocol")
	timeout := fs.Duration("timeout", 30*time.Second, "dial timeout")
	transferTimeout := fs.Duration("transfer-timeout", 5*time.Minute, "deadline for the transfer and its confirmation")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	}
	if *transferTimeout <= 0 {
		return fmt.Errorf("-transfer-timeout must be positive, got %s", *transferTimeout)
	}
	if *listen {
		if fs.NArg() != 0 {
			return fmt.Errorf("unexpected argument %q; -listen takes no operands", fs.Arg(0))
		}
		return serve(ctx, bind, *alpn)
	}
	size, err := parseBytes(*n)
	if err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: giperf [flags] <endpoint-ticket-or-id>")
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
	conn, err := ep.Connect(dctx, addr, *alpn)
	cancel()
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr.ID, err)
	}
	defer conn.Close()
	return transfer(ctx, conn, size, *transferTimeout)
}

// transfer sends size bytes and waits for the receiver to confirm them.
func transfer(ctx context.Context, conn *iroh.Conn, size int64, timeout time.Duration) error {
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	defer context.AfterFunc(ctx, func() { stream.Close() })()

	var header [headerSize]byte
	binary.BigEndian.PutUint64(header[:], uint64(size))
	start := time.Now()
	if _, err := stream.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	written, err := io.CopyN(stream, zeroReader{}, size)
	if err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	write := time.Since(start)

	status, received, err := readReply(stream)
	if err != nil {
		return fmt.Errorf("read confirmation: %w", err)
	}
	total := time.Since(start)
	if status != statusOK {
		return fmt.Errorf("receiver reported status %d after %d of %d bytes", status, received, size)
	}
	if received != size {
		return fmt.Errorf("receiver confirmed %d bytes, sent %d", received, size)
	}

	stats := conn.Stats()
	fmt.Printf("bytes=%d write=%s write-rate=%s/s total=%s rate=%s/s rtt=%s sent=%d received=%d\n",
		written, write.Round(time.Millisecond), rate(written, write),
		total.Round(time.Millisecond), rate(written, total),
		stats.SmoothedRTT, stats.BytesSent, stats.BytesReceived)
	return nil
}

// readReply reads a confirmation message.
func readReply(stream net.Conn) (status uint64, received int64, err error) {
	var reply [replySize]byte
	if _, err := io.ReadFull(stream, reply[:]); err != nil {
		return 0, 0, err
	}
	status = binary.BigEndian.Uint64(reply[0:8])
	count := binary.BigEndian.Uint64(reply[8:16])
	if count > math.MaxInt64 {
		return 0, 0, fmt.Errorf("confirmed count %d out of range", count)
	}
	return status, int64(count), nil
}

func serve(ctx context.Context, bind tool.BindFlags, alpn string) error {
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		for {
			stream, err := conn.AcceptStreamConn(ctx)
			if err != nil {
				if ctx.Err() != nil || conn.Context().Err() != nil || errors.Is(err, net.ErrClosed) {
					return nil
				}
				return err
			}
			// Serve each stream in its own call so the stream is closed when
			// it is done with, rather than when the accept loop ends.
			if err := serveTransfer(stream); err != nil {
				fmt.Fprintf(os.Stderr, "giperf: %v\n", err)
			}
		}
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{alpn: handler}, nil)
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

// serveTransfer receives one transfer and confirms what it consumed. A short
// payload is confirmed too, with a status saying so, so the sender learns the
// count instead of waiting out its deadline.
func serveTransfer(stream net.Conn) error {
	defer stream.Close()
	var header [headerSize]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	want := binary.BigEndian.Uint64(header[:])
	if want > maxTransfer {
		return fmt.Errorf("refusing transfer of %d bytes; limit is %d", want, int64(maxTransfer))
	}
	start := time.Now()
	n, err := io.CopyN(io.Discard, stream, int64(want))
	elapsed := time.Since(start)
	status := uint64(statusOK)
	if err != nil {
		status = statusShort
	}
	if replyErr := writeReply(stream, status, n); replyErr != nil {
		return fmt.Errorf("write confirmation: %w", replyErr)
	}
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	fmt.Fprintf(os.Stderr, "received bytes=%d elapsed=%s throughput=%s/s\n",
		n, elapsed.Round(time.Millisecond), rate(n, elapsed))
	return nil
}

func writeReply(stream net.Conn, status uint64, received int64) error {
	var reply [replySize]byte
	binary.BigEndian.PutUint64(reply[0:8], status)
	binary.BigEndian.PutUint64(reply[8:16], uint64(received))
	_, err := stream.Write(reply[:])
	return err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// byteSuffixes are matched longest-first so that "kib" wins over "k".
var byteSuffixes = []struct {
	suffix string
	mult   int64
}{
	{"kib", 1 << 10},
	{"mib", 1 << 20},
	{"gib", 1 << 30},
	{"kb", 1000},
	{"mb", 1000 * 1000},
	{"gb", 1000 * 1000 * 1000},
	{"k", 1000},
	{"m", 1000 * 1000},
	{"g", 1000 * 1000 * 1000},
}

// parseBytes parses a byte count with an optional binary or decimal suffix.
// A transfer must be at least one byte and at most maxTransfer: zero measures
// nothing, and a count that overflows is rejected rather than wrapped.
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty byte count")
	}
	mult := int64(1)
	lower := strings.ToLower(s)
	for _, suffix := range byteSuffixes {
		if strings.HasSuffix(lower, suffix.suffix) {
			mult = suffix.mult
			s = s[:len(s)-len(suffix.suffix)]
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse byte count: %w", err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("byte count must be positive, got %d", n)
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("byte count %s overflows", strings.TrimSpace(lower))
	}
	total := n * mult
	if total > maxTransfer {
		return 0, fmt.Errorf("byte count %d exceeds the %d byte limit", total, int64(maxTransfer))
	}
	return total, nil
}

func rate(n int64, d time.Duration) string {
	if d <= 0 {
		return "inf"
	}
	bps := float64(n) / d.Seconds()
	switch {
	case bps >= 1<<30:
		return fmt.Sprintf("%.2fGiB", bps/(1<<30))
	case bps >= 1<<20:
		return fmt.Sprintf("%.2fMiB", bps/(1<<20))
	case bps >= 1<<10:
		return fmt.Sprintf("%.2fKiB", bps/(1<<10))
	default:
		return fmt.Sprintf("%.0fB", bps)
	}
}
