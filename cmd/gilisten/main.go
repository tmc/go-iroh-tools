package main

import (
	"context"
	"errors"
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
			stream, acceptErr := conn.AcceptStream(ctx)
			if acceptErr != nil {
				if errors.Is(acceptErr, context.Canceled) {
					return nil
				}
				err = acceptErr
				fmt.Fprintf(os.Stderr, "accept stream: %v\n", err)
				return err
			}
			if *echo {
				err = echoStream(stream)
			} else {
				err = tool.CopyStdio(stream, os.Stdin, os.Stdout)
			}
			_ = stream.Close()
			if err != nil || *once {
				if err != nil {
					fmt.Fprintf(os.Stderr, "handle stream: %v\n", err)
				}
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

func echoStream(stream interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}) error {
	buf := make([]byte, 32*1024)
	for {
		n, readErr := stream.Read(buf)
		if n > 0 {
			if _, err := stream.Write(buf[:n]); err != nil {
				fmt.Fprintf(os.Stderr, "echo write: %v\n", err)
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, os.ErrClosed) {
				return nil
			}
			return readErr
		}
	}
}
