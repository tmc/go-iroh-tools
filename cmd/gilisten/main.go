package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/iroh"
)

const defaultALPN = "iroh-tools/stream/1"

func main() {
	tool.Run(run)
}

func run(ctx context.Context) error {
	fs := flag.NewFlagSet("gilisten", flag.ExitOnError)
	var bind tool.BindFlags
	alpn := fs.String("alpn", defaultALPN, "ALPN protocol")
	echo := fs.Bool("echo", false, "echo received bytes instead of bridging stdin/stdout")
	once := fs.Bool("1", false, "exit after one connection")
	tool.RegisterBindFlags(fs, &bind)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	ep, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer ep.Shutdown(context.Background())
	if !*echo && !*once {
		return fmt.Errorf("stdio bridge requires -1; use -echo for multi-connection mode")
	}
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, tool.LocalTicket(ep))
	done := make(chan error, 1)
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		var err error
		defer func() {
			if *once {
				done <- err
			}
		}()
		for {
			stream, acceptErr := conn.AcceptStreamConn(ctx)
			if acceptErr != nil {
				err = acceptErr
				return err
			}
			if *echo {
				_, err = io.Copy(stream, stream)
			} else {
				err = tool.CopyStdio(stream, os.Stdin, os.Stdout)
			}
			_ = stream.Close()
			if err != nil || *once {
				return err
			}
		}
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{*alpn: handler}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(context.Background())
	if *once {
		return <-done
	}
	<-ctx.Done()
	return ctx.Err()
}
