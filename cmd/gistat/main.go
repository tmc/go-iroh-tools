// Gistat reports transport statistics for a connection it makes itself.
//
// Gistat dials the given endpoint and describes that one connection. It does
// not enumerate connections belonging to other processes the way ss(8) or
// netstat(8) do; there is no system table of iroh connections to read. The
// numbers describe a connection that exists only for the duration of the run.
//
// Usage:
//
//	gistat [flags] <endpoint-ticket-or-id>
//	gistat -listen [flags]
//
// A fresh connection reports little: path validation, relay fallback, and NAT
// traversal all resolve over the first seconds. Use -n and -i to sample
// repeatedly and watch the paths settle.
//
// Output is one record per sample: name and tab-separated value per line, with
// a blank line between samples. A field the transport did not report is
// omitted rather than printed as zero.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

const statALPN = "iroh-tools/stat/1"

func main() {
	tool.Run(run)
}

func run(ctx context.Context) error {
	fs := flag.NewFlagSet("gistat", flag.ExitOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for stats probes")
	alpn := fs.String("alpn", statALPN, "ALPN protocol")
	timeout := fs.Duration("timeout", 5*time.Second, "dial timeout")
	count := fs.Int("n", 1, "number of samples to print")
	interval := fs.Duration("i", time.Second, "interval between samples")
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	}
	if *listen {
		if fs.NArg() != 0 {
			return fmt.Errorf("unexpected argument %q; -listen takes no operands", fs.Arg(0))
		}
		return serve(ctx, bind, *alpn)
	}
	if *count <= 0 {
		return fmt.Errorf("-n must be positive, got %d", *count)
	}
	if *interval < 0 {
		return fmt.Errorf("-i must not be negative, got %s", *interval)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: gistat [flags] <endpoint-ticket-or-id>")
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
	return sample(ctx, os.Stdout, conn, *count, *interval)
}

// sample prints count records, interval apart. It stops early if the peer goes
// away, reporting what it observed rather than an error: a connection ending
// is an observation too.
func sample(ctx context.Context, w io.Writer, conn *iroh.Conn, count int, interval time.Duration) error {
	for i := range count {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-conn.Context().Done():
				return nil
			case <-time.After(interval):
			}
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if err := printConn(w, conn); err != nil {
			return err
		}
	}
	return nil
}

func printConn(w io.Writer, conn *iroh.Conn) error {
	if err := printFields(w, [][2]string{
		{"remote", conn.RemoteID().String()},
		{"alpn", conn.ALPN()},
		{"local", conn.LocalAddr().String()},
		{"remote-addr", conn.RemoteAddr().String()},
		{"multipath", fmt.Sprint(conn.MultipathNegotiated())},
	}); err != nil {
		return err
	}
	if err := printStats(w, conn.Stats()); err != nil {
		return err
	}
	for _, p := range conn.Paths() {
		if err := printPath(w, p); err != nil {
			return err
		}
	}
	return nil
}

func printFields(w io.Writer, fields [][2]string) error {
	for _, f := range fields {
		if _, err := fmt.Fprintf(w, "%s\t%s\n", f[0], f[1]); err != nil {
			return err
		}
	}
	return nil
}

func printStats(w io.Writer, s iroh.ConnStats) error {
	return printFields(w, [][2]string{
		{"rtt-min", s.MinRTT.String()},
		{"rtt-latest", s.LatestRTT.String()},
		{"rtt-smoothed", s.SmoothedRTT.String()},
		{"bytes-sent", fmt.Sprint(s.BytesSent)},
		{"bytes-received", fmt.Sprint(s.BytesReceived)},
		{"packets-sent", fmt.Sprint(s.PacketsSent)},
		{"packets-received", fmt.Sprint(s.PacketsReceived)},
		{"bytes-lost", fmt.Sprint(s.BytesLost)},
		{"packets-lost", fmt.Sprint(s.PacketsLost)},
	})
}

func printPath(w io.Writer, p iroh.PathInfo) error {
	line := fmt.Sprintf("path\tid=%d selected=%t validated=%t relayed=%t", p.ID, p.Selected, p.Validated, p.Relayed)
	// Each Has field distinguishes "not reported" from "reported as zero", so
	// an absent field is left out rather than printed as a misleading zero.
	if p.HasAddr {
		line += fmt.Sprintf(" addr=%s", p.Addr)
	}
	if p.HasRTT {
		line += fmt.Sprintf(" rtt=%s", p.RTT)
	}
	if p.HasBytesSent {
		line += fmt.Sprintf(" sent=%d", p.BytesSent)
	}
	if p.HasBytesReceived {
		line += fmt.Sprintf(" received=%d", p.BytesReceived)
	}
	if p.HasBytesInFlight {
		line += fmt.Sprintf(" in-flight=%d", p.BytesInFlight)
	}
	if p.HasCongestionWindow {
		line += fmt.Sprintf(" cwnd=%d", p.CongestionWindow)
	}
	if p.HasLoss {
		line += fmt.Sprintf(" lost-packets=%d lost-bytes=%d", p.LostPackets, p.LostBytes)
	}
	_, err := fmt.Fprintln(w, line)
	return err
}

func serve(ctx context.Context, bind tool.BindFlags, alpn string) error {
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		// The probe measures the connection itself, so the server only has to
		// keep it open until the client is finished with it.
		select {
		case <-conn.Context().Done():
		case <-ctx.Done():
		}
		return nil
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
