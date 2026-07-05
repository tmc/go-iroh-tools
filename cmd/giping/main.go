package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

const pingALPN = "iroh-tools/ping/1"

func main() {
	tool.Run(run)
}

func run(ctx context.Context) error {
	fs := flag.NewFlagSet("giping", flag.ExitOnError)
	var bind tool.BindFlags
	listen := fs.Bool("listen", false, "listen for iroh ping requests")
	count := fs.Int("c", 4, "ping count")
	interval := fs.Duration("i", time.Second, "interval between pings")
	timeout := fs.Duration("timeout", 5*time.Second, "per-ping timeout")
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *listen {
		return serve(ctx, bind)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: giping [flags] <endpoint-ticket-or-id>")
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
	for seq := 0; seq < *count; seq++ {
		if seq > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(*interval):
			}
		}
		start := time.Now()
		pctx, cancel := context.WithTimeout(ctx, *timeout)
		err := ping(pctx, ep, addr, uint64(seq), uint64(start.UnixNano()))
		cancel()
		if err != nil {
			fmt.Printf("seq=%d error=%v\n", seq, err)
			continue
		}
		fmt.Printf("seq=%d time=%s\n", seq, time.Since(start).Round(time.Microsecond))
	}
	return nil
}

func serve(ctx context.Context, bind tool.BindFlags) error {
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
		var buf [16]byte
		if _, err := io.ReadFull(stream, buf[:]); err != nil {
			return err
		}
		_, err = stream.Write(buf[:])
		return err
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{pingALPN: handler}, nil)
	if err != nil {
		ep.Shutdown(context.Background())
		return err
	}
	defer router.Shutdown(context.Background())
	<-ctx.Done()
	return ctx.Err()
}

func ping(ctx context.Context, ep *iroh.Endpoint, addr netaddr.EndpointAddr, seq, stamp uint64) error {
	stream, err := ep.Dial(ctx, addr, pingALPN)
	if err != nil {
		return err
	}
	defer stream.Close()
	var req [16]byte
	binary.BigEndian.PutUint64(req[0:8], seq)
	binary.BigEndian.PutUint64(req[8:16], stamp)
	if _, err := stream.Write(req[:]); err != nil {
		return err
	}
	var resp [16]byte
	if _, err := io.ReadFull(stream, resp[:]); err != nil {
		return err
	}
	if resp != req {
		return fmt.Errorf("bad ping echo")
	}
	return nil
}
