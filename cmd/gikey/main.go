package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/key"
)

func main() {
	fs := flag.NewFlagSet("gikey", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: gikey generate\n")
		fmt.Fprintf(fs.Output(), "       gikey public <secret-key>\n")
		fmt.Fprintf(fs.Output(), "       gikey inspect <public-key-or-endpoint-id>\n")
	}
	fs.Parse(os.Args[1:])
	tool.Run(func(context.Context) error {
		return run(os.Stdout, fs.Args())
	})
}

func run(w io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: gikey generate|public|inspect")
	}
	switch args[0] {
	case "generate":
		if len(args) != 1 {
			return fmt.Errorf("usage: gikey generate")
		}
		sk, err := key.GenerateSecretKey()
		if err != nil {
			return err
		}
		printSecret(w, sk)
	case "public":
		if len(args) != 2 {
			return fmt.Errorf("usage: gikey public <secret-key>")
		}
		sk, err := key.ParseSecretKey(args[1])
		if err != nil {
			return fmt.Errorf("parse secret key: %w", err)
		}
		printPublic(w, sk.Public())
	case "inspect":
		if len(args) != 2 {
			return fmt.Errorf("usage: gikey inspect <public-key-or-endpoint-id>")
		}
		id, err := parseEndpoint(args[1])
		if err != nil {
			return err
		}
		printPublic(w, id.PublicKey())
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}

func printSecret(w io.Writer, sk key.SecretKey) {
	seed := sk.Bytes()
	fmt.Fprintf(w, "secret %s\n", hex.EncodeToString(seed[:]))
	printPublic(w, sk.Public())
}

func printPublic(w io.Writer, pk key.PublicKey) {
	id := pk.EndpointID()
	fmt.Fprintf(w, "public %s\n", pk)
	fmt.Fprintf(w, "endpoint %s\n", id)
	fmt.Fprintf(w, "z32 %s\n", id.Z32())
}

func parseEndpoint(s string) (key.EndpointID, error) {
	if id, err := key.ParseEndpointID(s); err == nil {
		return id, nil
	}
	id, err := key.ParseEndpointIDZ32(s)
	if err != nil {
		return key.EndpointID{}, fmt.Errorf("parse endpoint id: %w", err)
	}
	return id, nil
}
