package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
)

func main() {
	tool.Run(run)
}

func run(ctx context.Context) error {
	fs := flag.NewFlagSet("giaddr", flag.ExitOnError)
	var bind tool.BindFlags
	wait := fs.Duration("wait", 0, "keep endpoint alive after printing")
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	fmt.Printf("id\t%s\n", ep.ID())
	fmt.Printf("local\t%s\n", ep.LocalAddr())
	fmt.Printf("addr\t%s\n", tool.LocalEndpointAddr(ep))
	fmt.Printf("ticket\t%s\n", tool.LocalTicket(ep))
	if *wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*wait):
		}
	}
	return nil
}
