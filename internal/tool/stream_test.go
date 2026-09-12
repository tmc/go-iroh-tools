package tool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/irohtest"
	"github.com/tmc/go-iroh/iroh"
)

const testALPN = "iroh-tools/test/1"

// echoPeer serves conn by echoing its first stream. It reports when it is done
// so a test can be sure the peer finished rather than leaking.
func echoPeer(t *testing.T, conn *iroh.Conn) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			done <- err
			return
		}
		_, err = Echo(stream)
		done <- err
	}()
	return done
}

// TestCopyStdioLargePayload is the regression test for the deadlock that a
// sequential upload-then-download path produced. An echo peer stops reading
// once its own send buffer fills, so a payload larger than the flow-control
// window only completes if both directions run at once.
func TestCopyStdioLargePayload(t *testing.T) {
	client, server := irohtest.Pair(t, testALPN)
	peer := echoPeer(t, server)

	stream, err := client.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}

	// Comfortably larger than any default QUIC flow-control window.
	payload := make([]byte, 8<<20)
	r := rand.NewChaCha8([32]byte{1})
	r.Read(payload)

	var got bytes.Buffer
	if err := CopyStdio(context.Background(), stream, bytes.NewReader(payload), &got); err != nil {
		t.Fatalf("CopyStdio: %v", err)
	}
	if err := <-peer; err != nil {
		t.Fatalf("echo peer: %v", err)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("echoed %d bytes, sent %d; contents equal: %t",
			got.Len(), len(payload), bytes.Equal(got.Bytes(), payload))
	}
}

// TestCopyStdioEmptyInput checks the degenerate case completes rather than
// hanging waiting for bytes that will never come.
func TestCopyStdioEmptyInput(t *testing.T) {
	client, server := irohtest.Pair(t, testALPN)
	peer := echoPeer(t, server)

	stream, err := client.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	var got bytes.Buffer
	if err := CopyStdio(context.Background(), stream, bytes.NewReader(nil), &got); err != nil {
		t.Fatalf("CopyStdio: %v", err)
	}
	if err := <-peer; err != nil {
		t.Fatalf("echo peer: %v", err)
	}
	if got.Len() != 0 {
		t.Fatalf("read %d bytes, want none", got.Len())
	}
}

// TestCopyStdioReplyAfterEOF checks that EOF on the input finishes only the
// send direction: a peer that replies after seeing EOF must still be heard.
func TestCopyStdioReplyAfterEOF(t *testing.T) {
	client, server := irohtest.Pair(t, testALPN)

	const reply = "reply-after-eof"
	done := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(context.Background())
		if err != nil {
			done <- err
			return
		}
		// Drain to EOF first, so the reply is written strictly after the peer
		// has finished sending.
		if _, err := io.Copy(io.Discard, stream); err != nil {
			done <- err
			return
		}
		if _, err := io.WriteString(stream, reply); err != nil {
			done <- err
			return
		}
		done <- stream.CloseWrite()
	}()

	stream, err := client.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	var got bytes.Buffer
	if err := CopyStdio(context.Background(), stream, bytes.NewReader([]byte("request")), &got); err != nil {
		t.Fatalf("CopyStdio: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("peer: %v", err)
	}
	if got.String() != reply {
		t.Fatalf("reply = %q, want %q", got.String(), reply)
	}
}

// TestCopyStdioCancelStalledPeer checks that a peer which accepts a stream and
// then says nothing does not hold the copy open past its context.
func TestCopyStdioCancelStalledPeer(t *testing.T) {
	client, server := irohtest.Pair(t, testALPN)

	stalled := make(chan struct{})
	go func() {
		defer close(stalled)
		if _, err := server.AcceptStream(context.Background()); err != nil {
			return
		}
		// Accept the stream and then neither read nor write.
		<-stalled
	}()

	stream, err := client.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	errc := make(chan error, 1)
	go func() {
		errc <- CopyStdio(ctx, stream, blockingReader{}, io.Discard)
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("CopyStdio = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CopyStdio did not return after its context expired")
	}
}

// TestCopyStdioOutputError checks that a failing output writer ends the copy
// with that error rather than being discarded.
func TestCopyStdioOutputError(t *testing.T) {
	client, server := irohtest.Pair(t, testALPN)
	go func() {
		stream, err := server.AcceptStream(context.Background())
		if err != nil {
			return
		}
		// Keep writing so the failing writer is certain to be reached.
		for {
			if _, err := stream.Write(make([]byte, 4096)); err != nil {
				return
			}
		}
	}()

	stream, err := client.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	// A bidirectional stream reaches the peer with its first bytes, so send
	// one before handing the stream a reader that never produces any.
	if _, err := io.WriteString(stream, "x"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := errors.New("output rejected")
	errc := make(chan error, 1)
	go func() {
		errc <- CopyStdio(context.Background(), stream, blockingReader{}, errWriter{want})
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, want) {
			t.Fatalf("CopyStdio = %v, want %v", err, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CopyStdio did not return after its output failed")
	}
}

// TestEcho checks the echo helper round-trips bytes and finishes its send side.
func TestEcho(t *testing.T) {
	client, server := irohtest.Pair(t, testALPN)
	peer := echoPeer(t, server)

	stream, err := client.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	const msg = "echo me"
	if _, err := io.WriteString(stream, msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != msg {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
	if err := <-peer; err != nil {
		t.Fatalf("echo peer: %v", err)
	}
}

// blockingReader never returns, standing in for a terminal stdin that has no
// input and cannot be interrupted.
type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) {
	select {}
}

type errWriter struct{ err error }

func (w errWriter) Write(p []byte) (int, error) { return 0, w.err }
