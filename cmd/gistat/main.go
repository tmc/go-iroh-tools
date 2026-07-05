package main

import (
	"context"
	"flag"
	"fmt"
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
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *listen {
		return serve(ctx, bind, *alpn)
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
	defer cancel()
	conn, err := ep.Connect(dctx, addr, *alpn)
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Printf("remote\t%s\n", conn.RemoteID())
	fmt.Printf("alpn\t%s\n", conn.ALPN())
	fmt.Printf("local\t%s\n", conn.LocalAddr())
	fmt.Printf("remote-addr\t%s\n", conn.RemoteAddr())
	fmt.Printf("multipath\t%t\n", conn.MultipathNegotiated())
	printStats(conn.Stats())
	for _, p := range conn.Paths() {
		printPath(p)
	}
	return nil
}

func serve(ctx context.Context, bind tool.BindFlags, alpn string) error {
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, tool.LocalTicket(ep))
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		<-conn.Context().Done()
		return nil
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{alpn: handler}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(context.Background())
	<-ctx.Done()
	return ctx.Err()
}

func printStats(s iroh.ConnStats) {
	fmt.Printf("rtt-min\t%s\n", s.MinRTT)
	fmt.Printf("rtt-latest\t%s\n", s.LatestRTT)
	fmt.Printf("rtt-smoothed\t%s\n", s.SmoothedRTT)
	fmt.Printf("bytes-sent\t%d\n", s.BytesSent)
	fmt.Printf("bytes-received\t%d\n", s.BytesReceived)
	fmt.Printf("packets-sent\t%d\n", s.PacketsSent)
	fmt.Printf("packets-received\t%d\n", s.PacketsReceived)
	fmt.Printf("bytes-lost\t%d\n", s.BytesLost)
	fmt.Printf("packets-lost\t%d\n", s.PacketsLost)
}

func printPath(p iroh.PathInfo) {
	fmt.Printf("path\tid=%d selected=%t validated=%t relayed=%t", p.ID, p.Selected, p.Validated, p.Relayed)
	if p.HasAddr {
		fmt.Printf(" addr=%s", p.Addr)
	}
	if p.HasRTT {
		fmt.Printf(" rtt=%s", p.RTT)
	}
	if p.HasBytesSent {
		fmt.Printf(" sent=%d", p.BytesSent)
	}
	if p.HasBytesReceived {
		fmt.Printf(" received=%d", p.BytesReceived)
	}
	if p.HasBytesInFlight {
		fmt.Printf(" in-flight=%d", p.BytesInFlight)
	}
	if p.HasCongestionWindow {
		fmt.Printf(" cwnd=%d", p.CongestionWindow)
	}
	if p.HasLoss {
		fmt.Printf(" lost-packets=%d lost-bytes=%d", p.LostPackets, p.LostBytes)
	}
	fmt.Println()
}
