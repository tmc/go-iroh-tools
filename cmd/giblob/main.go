// Giblob transfers a single blob over the iroh-blobs protocol, verifying
// every byte against the blob's BLAKE3 hash as it arrives.
//
// Usage:
//
//	giblob -serve [-1] [-short] [-store dir] [flags] [file]
//	giblob [-o path] [-timeout d] [flags] <blob-ticket>
//
// With -serve, giblob serves file, or standard input when file is omitted or
// "-". It imports the data first, then prints a blob ticket to stderr once the
// endpoint is ready to serve it, and serves until interrupted. With -1 it
// serves exactly one connection and exits zero when that client disconnects.
// -short leaves the endpoint's direct addresses out of the ticket, keeping
// only its relay; it is an error without relays.
//
// A file is imported into the store directory named by -store, by hardlink
// where possible. Without -store, a file goes into a temporary store removed
// at exit, and standard input is held in memory. With -store, standard input
// is written to that store too.
//
// Without -serve, giblob fetches the blob named by the ticket and writes it to
// stdout, or to path with -o. Only raw blob tickets are accepted, not hash
// sequences. Data is written only after it is verified, but a fetch that fails
// partway may already have written a verified prefix to stdout; the nonzero
// exit status is what reports the failure. With -o the file is written to a
// temporary name in the same directory and renamed into place on success, so
// a failed fetch leaves no file behind. -timeout bounds dialing and every
// wait for more data, not the whole transfer, so a large blob may take
// longer as long as it keeps arriving.
//
// Giblob exits 0 on success, 1 on a runtime error, and 2 on a usage error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
)

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stdin, os.Stdout, os.Stderr)
	})
}

// run is giblob with its standard streams injected. With -serve the blob
// ticket is written to stderr, so a caller can watch stderr for readiness.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("giblob", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var bind tool.BindFlags
	serveMode := fs.Bool("serve", false, "serve a file or standard input")
	once := fs.Bool("1", false, "with -serve, exit after one connection")
	short := fs.Bool("short", false, "with -serve, leave direct addresses out of the ticket")
	storeDir := fs.String("store", "", "with -serve, import into the blob store in `dir`")
	out := fs.String("o", "", "write the fetched blob to `path` instead of stdout")
	timeout := fs.Duration("timeout", 30*time.Second, "give up dialing or waiting for data after this long")
	tool.RegisterBindFlags(fs, &bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return err
	}
	if *serveMode {
		if *out != "" {
			return errors.New("-o applies only when fetching")
		}
		if *short && (bind.DirectOnly || !bind.Relay) {
			return errors.New("-short drops direct addresses and needs a relay; relays are disabled")
		}
		if fs.NArg() > 1 {
			return fmt.Errorf("unexpected argument %q; -serve takes at most one file", fs.Arg(1))
		}
		return serve(ctx, bind, fs.Arg(0), *storeDir, *once, *short, stdin, stderr)
	}
	switch {
	case *once:
		return errors.New("-1 applies only with -serve")
	case *short:
		return errors.New("-short applies only with -serve")
	case *storeDir != "":
		return errors.New("-store applies only with -serve")
	case *timeout <= 0:
		return fmt.Errorf("-timeout must be positive, got %s", *timeout)
	case fs.NArg() != 1:
		return errors.New("usage: giblob [flags] <blob-ticket>")
	}
	return fetch(ctx, bind, fs.Arg(0), *out, *timeout, stdout)
}

// serve imports name, or stdin when name is "" or "-", and serves it.
func serve(ctx context.Context, bind tool.BindFlags, name, storeDir string, once, short bool, stdin io.Reader, stderr io.Writer) error {
	store, hash, cleanup, err := importBlob(ctx, name, storeDir, stdin)
	if err != nil {
		return err
	}
	defer cleanup()

	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var admitted atomic.Bool
	served := make(chan struct{})
	handler := iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
		defer conn.Close()
		if once {
			if !admitted.CompareAndSwap(false, true) {
				return nil
			}
			defer close(served)
		}
		accept := func(ctx context.Context) (blobs.BidiStream, error) {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return nil, err
			}
			return s, nil
		}
		return blobs.ServeBlobStreams(ctx, accept, store)
	})
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{blobs.ALPN: handler}, nil)
	if err != nil {
		return err
	}
	defer router.Shutdown(context.Background())
	if err := tool.WaitRelay(ctx, ep, bind); err != nil {
		return err
	}
	ticket := blobTicket(ep, hash, short)
	if _, err := fmt.Fprintln(stderr, ticket); err != nil {
		return err
	}
	if !once {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-served:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// blobTicket returns the ticket for hash on ep.
func blobTicket(ep *iroh.Endpoint, hash blobs.Hash, short bool) blobs.Ticket {
	t := blobs.NewTicket(ep.Addr(), hash, blobs.Raw)
	if short {
		t = t.Short()
	}
	return t
}

// importBlob loads the blob to serve into a store and returns the store, the
// blob's hash, and a function that releases the store.
func importBlob(ctx context.Context, name, storeDir string, stdin io.Reader) (blobs.Store, blobs.Hash, func(), error) {
	fromStdin := name == "" || name == "-"
	if fromStdin && storeDir == "" {
		store, err := blobs.NewMemStore()
		if err != nil {
			return nil, blobs.Hash{}, nil, err
		}
		hash, err := blobs.ReadFromBlob(ctx, store, stdin)
		if err != nil {
			return nil, blobs.Hash{}, nil, fmt.Errorf("read stdin: %w", err)
		}
		return store, hash, func() {}, nil
	}

	dir := storeDir
	remove := func() {}
	if dir == "" {
		tmp, err := os.MkdirTemp("", "giblob-store-")
		if err != nil {
			return nil, blobs.Hash{}, nil, fmt.Errorf("create store: %w", err)
		}
		dir = tmp
		remove = func() { os.RemoveAll(tmp) }
	}
	store, err := blobs.NewFSStore(dir)
	if err != nil {
		remove()
		return nil, blobs.Hash{}, nil, fmt.Errorf("open store: %w", err)
	}
	cleanup := func() {
		store.Close()
		remove()
	}
	var hash blobs.Hash
	if fromStdin {
		hash, err = blobs.ReadFromBlob(ctx, store, stdin)
	} else {
		hash, err = store.ImportFile(name, blobs.ImportTryReference)
	}
	if err != nil {
		cleanup()
		return nil, blobs.Hash{}, nil, fmt.Errorf("import %s: %w", displayName(name), err)
	}
	return store, hash, cleanup, nil
}

func displayName(name string) string {
	if name == "" || name == "-" {
		return "stdin"
	}
	return name
}

// fetch downloads the blob named by ticketText to outPath, or to stdout when
// outPath is empty.
func fetch(ctx context.Context, bind tool.BindFlags, ticketText, outPath string, timeout time.Duration, stdout io.Writer) error {
	ticket, err := blobs.ParseTicket(ticketText)
	if err != nil {
		return fmt.Errorf("parse blob ticket: %w", err)
	}
	if !ticket.Format().IsRaw() {
		return fmt.Errorf("ticket names a %v blob; giblob fetches only raw blobs", ticket.Format())
	}
	if outPath == "" {
		return download(ctx, bind, ticket, timeout, stdout)
	}

	f, err := os.CreateTemp(filepath.Dir(outPath), "."+filepath.Base(outPath)+".tmp*")
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	tmp := f.Name()
	err = download(ctx, bind, ticket, timeout, f)
	if closeErr := f.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("write output: %w", closeErr)
	}
	if err == nil {
		err = os.Chmod(tmp, 0o644)
	}
	if err == nil {
		err = os.Rename(tmp, outPath)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// download fetches ticket's blob into w. It gives up if dialing, or any wait
// for the next verified data, takes longer than timeout.
func download(ctx context.Context, bind tool.BindFlags, ticket blobs.Ticket, timeout time.Duration, w io.Writer) error {
	ep, closeEndpoint, err := tool.Bind(ctx, bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errStalled := fmt.Errorf("timed out after %s", timeout)
	timer := time.AfterFunc(timeout, func() { cancel(errStalled) })
	defer timer.Stop()

	addr := ticket.Addr()
	conn, err := ep.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr.ID, stallCause(ctx, err))
	}
	defer conn.Close()
	timer.Reset(timeout)
	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", stallCause(ctx, err))
	}
	defer s.Close()
	sw := &stallWriter{w: w, timer: timer, timeout: timeout}
	if err := blobs.DownloadBlob(ctx, s, ticket.Hash(), sw); err != nil {
		return fmt.Errorf("fetch %s: %w", ticket.Hash(), stallCause(ctx, err))
	}
	return nil
}

// stallCause reports the stall timeout in place of err when the stall timer
// is what cancelled ctx.
func stallCause(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

// stallWriter restarts the stall timer after every write. The timer is
// stopped while the write runs, so a slow consumer of the output is not
// mistaken for a stalled peer.
type stallWriter struct {
	w       io.Writer
	timer   *time.Timer
	timeout time.Duration
}

func (s *stallWriter) Write(p []byte) (int, error) {
	s.timer.Stop()
	defer s.timer.Reset(s.timeout)
	return s.w.Write(p)
}
