package tool

import (
	"io"
	"net"
	"sync"
)

// CopyStdio copies a stream to out and in to the stream until both sides stop.
func CopyStdio(stream net.Conn, in io.Reader, out io.Writer) error {
	var wg sync.WaitGroup
	errc := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := io.Copy(stream, in)
		_ = stream.Close()
		errc <- err
	}()
	go func() {
		defer wg.Done()
		_, err := io.Copy(out, stream)
		errc <- err
	}()
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}
