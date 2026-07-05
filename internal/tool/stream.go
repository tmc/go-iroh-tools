package tool

import (
	"io"
)

type readWriteCloser interface {
	io.Reader
	io.Writer
	io.Closer
}

type closeWriter interface {
	CloseWrite() error
}

// CopyStdio copies a stream to out and in to the stream until the remote side
// stops sending. EOF on in half-closes the stream when the stream supports it.
func CopyStdio(stream readWriteCloser, in io.Reader, out io.Writer) error {
	writec := make(chan error, 1)
	readc := make(chan error, 1)
	go func() {
		_, err := io.Copy(stream, in)
		if cw, ok := stream.(closeWriter); ok {
			if closeErr := cw.CloseWrite(); err == nil {
				err = closeErr
			}
		} else {
			_ = stream.Close()
		}
		writec <- err
	}()
	go func() {
		_, err := io.Copy(out, stream)
		readc <- err
	}()
	for {
		select {
		case err := <-writec:
			if err != nil {
				_ = stream.Close()
				return err
			}
		case err := <-readc:
			_ = stream.Close()
			return err
		}
	}
}
