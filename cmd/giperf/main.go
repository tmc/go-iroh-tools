package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

const perfALPN = "iroh-tools/perf/1"

func main() {
	tool.Run(run)
}

func run(ctx context.Context) error {
	fs := flag.NewFlagSet("giperf", flag.ExitOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for throughput tests")
	n := fs.String("n", "64MiB", "bytes to send")
	alpn := fs.String("alpn", perfALPN, "ALPN protocol")
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *listen {
		return serve(ctx, bind, *alpn)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: giperf [flags] <endpoint-ticket-or-id>")
	}
	size, err := parseBytes(*n)
	if err != nil {
		return err
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
	conn, err := ep.Connect(ctx, addr, *alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	written, err := io.CopyN(stream, zeroReader{}, size)
	if closeErr := stream.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	stats := conn.Stats()
	fmt.Printf("bytes=%d elapsed=%s throughput=%s/s rtt=%s sent=%d received=%d\n",
		written, elapsed.Round(time.Millisecond), rate(written, elapsed), stats.SmoothedRTT, stats.BytesSent, stats.BytesReceived)
	return nil
}

func serve(ctx context.Context, bind tool.BindFlags, alpn string) error {
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, tool.LocalTicket(ep))
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			return err
		}
		defer stream.Close()
		start := time.Now()
		n, err := io.Copy(io.Discard, stream)
		if err != nil {
			return err
		}
		elapsed := time.Since(start)
		fmt.Fprintf(os.Stderr, "received bytes=%d elapsed=%s throughput=%s/s\n", n, elapsed.Round(time.Millisecond), rate(n, elapsed))
		return nil
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{alpn: handler}, nil)
	if err != nil {
		ep.Shutdown(context.Background())
		return err
	}
	defer router.Shutdown(context.Background())
	<-ctx.Done()
	return ctx.Err()
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty byte count")
	}
	mult := int64(1)
	suffixes := []struct {
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
	lower := strings.ToLower(s)
	for _, suffix := range suffixes {
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
	if n < 0 {
		return 0, fmt.Errorf("negative byte count")
	}
	return n * mult, nil
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
