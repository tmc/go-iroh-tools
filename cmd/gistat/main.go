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
// Without -listen, gistat dials the endpoint, prints the requested samples to
// standard output, and exits. With -listen, it accepts probes on -alpn, prints
// its ticket to standard error once it is reachable, and runs until
// interrupted. The exit status is 0 on success or interrupt, 1 on a runtime
// error, and 2 on a usage error.
//
// A fresh connection reports little: path validation, relay fallback, and NAT
// traversal all resolve over the first seconds. Use -n and -i to sample
// repeatedly and watch the paths settle.
//
// Output is one record per sample: name and tab-separated value per line, with
// a blank line between samples. A field the transport did not report is
// omitted rather than printed as zero.
//
// With -json, each sample is instead one JSON object on its own line
// (NDJSON), with no blank line between samples. Wrapped here for width:
//
//	{"remote":"…","alpn":"…","local":"…","remote_addr":"…","multipath":false,
//	 "stats":{"rtt_min_ns":0,…},"paths":[{"id":0,"selected":true,…}]}
//
// Durations are integer nanoseconds in fields ending in _ns. The stats object
// carries every counter in [iroh.ConnStats], including rtt_mean_deviation_ns,
// ptos, and spurious_losses, which the text form does not print. paths is
// always an array, never null. A path field the transport did not report is
// omitted, as in the text form. -json applies only when dialing; it is an
// error alongside -listen.
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stdout, os.Stderr)
	})
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("gistat", flag.ContinueOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for stats probes")
	alpn := fs.String("alpn", statALPN, "ALPN protocol")
	timeout := fs.Duration("timeout", 5*time.Second, "dial timeout")
	count := fs.Int("n", 1, "number of samples to print")
	interval := fs.Duration("i", time.Second, "interval between samples")
	asJSON := fs.Bool("json", false, "print one JSON object per sample")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	}
	if *listen {
		if *asJSON {
			return errors.New("-json applies only when dialing, not with -listen")
		}
		if fs.NArg() != 0 {
			return fmt.Errorf("unexpected argument %q; -listen takes no operands", fs.Arg(0))
		}
		return serve(ctx, stderr, bind, *alpn)
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
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	dctx, cancel := context.WithTimeout(ctx, *timeout)
	conn, err := ep.Connect(dctx, addr, *alpn)
	cancel()
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr.ID, err)
	}
	defer conn.Close()
	return sample(ctx, stdout, conn, *count, *interval, *asJSON)
}

// sample prints count records, interval apart. It stops early if the peer goes
// away, reporting what it observed rather than an error: a connection ending
// is an observation too. Text records are separated by a blank line; JSON
// records are one per line.
func sample(ctx context.Context, w io.Writer, conn *iroh.Conn, count int, interval time.Duration, asJSON bool) error {
	for i := range count {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-conn.Context().Done():
				return nil
			case <-time.After(interval):
			}
			if !asJSON {
				if _, err := fmt.Fprintln(w); err != nil {
					return err
				}
			}
		}
		printSample := printConn
		if asJSON {
			printSample = printJSON
		}
		if err := printSample(w, conn); err != nil {
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

// connJSON is the -json form of one sample.
type connJSON struct {
	Remote     string     `json:"remote"`
	ALPN       string     `json:"alpn"`
	Local      string     `json:"local"`
	RemoteAddr string     `json:"remote_addr"`
	Multipath  bool       `json:"multipath"`
	Stats      statsJSON  `json:"stats"`
	Paths      []pathJSON `json:"paths"`
}

type statsJSON struct {
	MinRTT          int64  `json:"rtt_min_ns"`
	LatestRTT       int64  `json:"rtt_latest_ns"`
	SmoothedRTT     int64  `json:"rtt_smoothed_ns"`
	MeanDeviation   int64  `json:"rtt_mean_deviation_ns"`
	BytesSent       uint64 `json:"bytes_sent"`
	BytesReceived   uint64 `json:"bytes_received"`
	PacketsSent     uint64 `json:"packets_sent"`
	PacketsReceived uint64 `json:"packets_received"`
	BytesLost       uint64 `json:"bytes_lost"`
	PacketsLost     uint64 `json:"packets_lost"`
	PTOs            uint64 `json:"ptos"`
	SpuriousLosses  uint64 `json:"spurious_losses"`
}

// pathJSON is one [iroh.PathInfo]. Each pointer field is nil, and so omitted,
// when the corresponding Has field is false.
type pathJSON struct {
	ID               uint32  `json:"id"`
	Selected         bool    `json:"selected"`
	Validated        bool    `json:"validated"`
	Relayed          bool    `json:"relayed"`
	Addr             *string `json:"addr,omitempty"`
	RTT              *int64  `json:"rtt_ns,omitempty"`
	BytesSent        *uint64 `json:"bytes_sent,omitempty"`
	BytesReceived    *uint64 `json:"bytes_received,omitempty"`
	BytesInFlight    *uint64 `json:"bytes_in_flight,omitempty"`
	CongestionWindow *uint64 `json:"congestion_window,omitempty"`
	LostPackets      *uint64 `json:"lost_packets,omitempty"`
	LostBytes        *uint64 `json:"lost_bytes,omitempty"`
}

func printJSON(w io.Writer, conn *iroh.Conn) error {
	c := connJSON{
		Remote:     conn.RemoteID().String(),
		ALPN:       conn.ALPN(),
		Local:      conn.LocalAddr().String(),
		RemoteAddr: conn.RemoteAddr().String(),
		Multipath:  conn.MultipathNegotiated(),
		Stats:      newStatsJSON(conn.Stats()),
		Paths:      newPathsJSON(conn.Paths()),
	}
	return json.NewEncoder(w).Encode(c)
}

func newStatsJSON(s iroh.ConnStats) statsJSON {
	return statsJSON{
		MinRTT:          int64(s.MinRTT),
		LatestRTT:       int64(s.LatestRTT),
		SmoothedRTT:     int64(s.SmoothedRTT),
		MeanDeviation:   int64(s.MeanDeviation),
		BytesSent:       s.BytesSent,
		BytesReceived:   s.BytesReceived,
		PacketsSent:     s.PacketsSent,
		PacketsReceived: s.PacketsReceived,
		BytesLost:       s.BytesLost,
		PacketsLost:     s.PacketsLost,
		PTOs:            s.PTOs,
		SpuriousLosses:  s.SpuriousLosses,
	}
}

func newPathsJSON(paths []iroh.PathInfo) []pathJSON {
	out := make([]pathJSON, 0, len(paths))
	for _, p := range paths {
		j := pathJSON{
			ID:        p.ID,
			Selected:  p.Selected,
			Validated: p.Validated,
			Relayed:   p.Relayed,
		}
		if p.HasAddr {
			addr := p.Addr.String()
			j.Addr = &addr
		}
		if p.HasRTT {
			rtt := int64(p.RTT)
			j.RTT = &rtt
		}
		if p.HasBytesSent {
			j.BytesSent = &p.BytesSent
		}
		if p.HasBytesReceived {
			j.BytesReceived = &p.BytesReceived
		}
		if p.HasBytesInFlight {
			j.BytesInFlight = &p.BytesInFlight
		}
		if p.HasCongestionWindow {
			j.CongestionWindow = &p.CongestionWindow
		}
		if p.HasLoss {
			j.LostPackets = &p.LostPackets
			j.LostBytes = &p.LostBytes
		}
		out = append(out, j)
	}
	return out
}

func serve(ctx context.Context, stderr io.Writer, bind tool.BindFlags, alpn string) error {
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
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
	fmt.Fprintln(stderr, tool.LocalTicket(ep))
	<-ctx.Done()
	return ctx.Err()
}
