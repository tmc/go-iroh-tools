package tool

import (
	"context"
	"io"
)

// closeWriter is implemented by streams that can half-close their send
// direction, leaving the receive direction open for the peer's reply.
// [github.com/tmc/go-iroh/iroh.Stream] implements it.
type closeWriter interface {
	CloseWrite() error
}

// readCanceler is implemented by streams whose receive direction can be
// aborted. It is the only way to unblock a goroutine parked in Read: on an
// iroh stream Close finishes the send direction and leaves Read blocked.
type readCanceler interface {
	CancelRead(code uint64)
}

// abortCode is the application error code CopyStdio reports when it abandons
// the receive direction. The tools do not assign meaning to individual codes.
const abortCode = 0

// CopyStdio bridges in and out across stream until the peer stops sending.
//
// Ownership is split three ways:
//
//   - CopyStdio owns the send direction. It copies in to stream and, at EOF,
//     half-closes the send side so the peer sees EOF and can still reply.
//   - The receive direction ends the call. CopyStdio returns when the peer
//     closes its send side, when the copy to out fails, or when ctx is done,
//     in which case it aborts the receive direction so Read unblocks.
//   - The caller owns the connection and the stream. CopyStdio never closes
//     the connection, and closes the stream only to the extent half-closing
//     the send direction requires.
//
// CopyStdio does not wait for the send goroutine. in is typically a terminal
// or a pipe that no context can interrupt, so joining it could block after the
// peer has already finished; the goroutine ends with the process. For that
// reason CopyStdio suits short-lived commands and is not a general helper.
func CopyStdio(ctx context.Context, stream io.ReadWriteCloser, in io.Reader, out io.Writer) error {
	sendc := make(chan error, 1)
	go func() {
		_, err := io.Copy(stream, in)
		if closeErr := closeWrite(stream); err == nil {
			err = closeErr
		}
		sendc <- err
	}()
	recvc := make(chan error, 1)
	go func() {
		_, err := io.Copy(out, stream)
		recvc <- err
	}()
	for {
		select {
		case <-ctx.Done():
			abortRead(stream)
			return ctx.Err()
		case err := <-recvc:
			return err
		case err := <-sendc:
			if err != nil {
				abortRead(stream)
				return err
			}
			// The send direction is finished. Keep receiving: the peer may
			// still have a reply for the bytes it just saw EOF after.
			sendc = nil
		}
	}
}

// closeWrite finishes the send direction of stream.
func closeWrite(stream io.ReadWriteCloser) error {
	if cw, ok := stream.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return stream.Close()
}

// abortRead unblocks a goroutine reading from stream. Close is the fallback
// for streams that cannot cancel a read; on those it is what releases Read.
func abortRead(stream io.ReadWriteCloser) {
	if rc, ok := stream.(readCanceler); ok {
		rc.CancelRead(abortCode)
		return
	}
	_ = stream.Close()
}

// Echo copies stream back to itself until the peer stops sending, then
// finishes the send direction. It reports the number of bytes echoed.
//
// The copy carries no framing of its own, so io.Copy is enough. An iroh stream
// implements [io.ReaderFrom] and not [io.WriterTo], so this dispatches to the
// stream's own ReadFrom, which is an ordinary buffered read-write loop and is
// safe to point at the same stream.
func Echo(stream io.ReadWriteCloser) (int64, error) {
	n, err := io.Copy(stream, stream)
	if closeErr := closeWrite(stream); err == nil {
		err = closeErr
	}
	return n, err
}
