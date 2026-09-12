// Gikey generates and inspects iroh keys.
//
// Usage:
//
//	gikey generate                              print a new secret and its public key
//	gikey public [secret-key]                   derive the public key of a secret
//	gikey inspect <public-key-or-endpoint-id>   print a public key's identifiers
//
// Public reads the secret from stdin when no operand is given, so a secret
// need not appear in a command line, where the process table exposes it:
//
//	gikey generate >key
//	gikey public <key
//
// Stdin may be either a bare hex secret or the labeled "secret" line that
// generate prints, so the two commands pipe together directly.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/key"
)

func main() {
	fs := flag.NewFlagSet("gikey", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: gikey generate\n")
		fmt.Fprintf(fs.Output(), "       gikey public [secret-key]\n")
		fmt.Fprintf(fs.Output(), "       gikey inspect <public-key-or-endpoint-id>\n")
	}
	fs.Parse(os.Args[1:])
	tool.Run(func(context.Context) error {
		return run(os.Stdout, os.Stdin, fs.Args())
	})
}

func run(w io.Writer, in io.Reader, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: gikey generate|public|inspect")
	}
	switch args[0] {
	case "generate":
		if len(args) != 1 {
			return errors.New("usage: gikey generate")
		}
		sk, err := key.GenerateSecretKey()
		if err != nil {
			return err
		}
		return printSecret(w, sk)
	case "public":
		if len(args) > 2 {
			return errors.New("usage: gikey public [secret-key]")
		}
		text, err := secretText(in, args[1:])
		if err != nil {
			return err
		}
		sk, err := key.ParseSecretKey(text)
		if err != nil {
			return fmt.Errorf("parse secret key: %w", err)
		}
		return printPublic(w, sk.Public())
	case "inspect":
		if len(args) != 2 {
			return errors.New("usage: gikey inspect <public-key-or-endpoint-id>")
		}
		id, err := tool.ParseEndpointID(args[1])
		if err != nil {
			return err
		}
		return printPublic(w, id.PublicKey())
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// secretText returns the secret key text from args, or reads it from in.
func secretText(in io.Reader, args []string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Accept the labeled line that generate prints as well as a bare key,
		// so the two commands pipe together.
		if rest, ok := strings.CutPrefix(line, "secret "); ok {
			return strings.TrimSpace(rest), nil
		}
		return line, nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read secret key: %w", err)
	}
	return "", errors.New("no secret key on stdin")
}

func printSecret(w io.Writer, sk key.SecretKey) error {
	seed := sk.Bytes()
	if _, err := fmt.Fprintf(w, "secret %s\n", hex.EncodeToString(seed[:])); err != nil {
		return err
	}
	return printPublic(w, sk.Public())
}

func printPublic(w io.Writer, pk key.PublicKey) error {
	id := pk.EndpointID()
	if _, err := fmt.Fprintf(w, "public %s\n", pk); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "endpoint %s\n", id); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "z32 %s\n", id.Z32())
	return err
}
