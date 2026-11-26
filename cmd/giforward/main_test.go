package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// directFlags keep every endpoint on loopback with relays disabled, so no
// test depends on relay availability.
var directFlags = []string{"-bind", "127.0.0.1:0", "-direct-only"}

// lines is the stderr of one run. The first write, the ticket or the bound
// address, is delivered on first and later ones on more; everything is logged.
type lines struct {
	t     *testing.T
	name  string
	once  sync.Once
	first chan string
	more  chan string
}

func newLines(t *testing.T, name string) *lines {
	return &lines{t: t, name: name, first: make(chan string, 1), more: make(chan string, 64)}
}

func (l *lines) Write(p []byte) (int, error) {
	s := strings.TrimSpace(string(p))
	l.t.Logf("%s: %s", l.name, s)
	first := false
	l.once.Do(func() { first = true; l.first <- s })
	if !first {
		select {
		case l.more <- s:
		default:
		}
	}
	return len(p), nil
}

func (l *lines) wait(t *testing.T, errc <-chan error) string {
	t.Helper()
	select {
	case s := <-l.first:
		return s
	case err := <-errc:
		t.Fatalf("%s exited before ready: %v", l.name, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s not ready", l.name)
	}
	return ""
}

// echoServer starts a TCP server that writes greeting, then echoes until the
// client half-closes, then half-closes itself.
func echoServer(t *testing.T, greeting string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		ln.Close()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer c.Close()
				if greeting != "" {
					if _, err := io.WriteString(c, greeting); err != nil {
						return
					}
				}
				io.Copy(c, c)
				c.(*net.TCPConn).CloseWrite()
			})
		}
	})
	return ln.Addr().String()
}

// forwarder runs a listener forwarding to target and a dialer in front of it.
// It returns the dialer's local TCP address and a stop function that cancels
// both and reports their results.
func forwarder(t *testing.T, target string) (addr string, stop func() (listenErr, dialErr error)) {
	addr, stop, _ = forwarderLog(t, target)
	return addr, stop
}

// forwarderLog is forwarder, also returning the dialer's stderr.
func forwarderLog(t *testing.T, target string) (addr string, stop func() (listenErr, dialErr error), dlog *lines) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	lout := newLines(t, "listen")
	lerrc := make(chan error, 1)
	go func() {
		lerrc <- run(ctx, append([]string{"-listen", "-to", target}, directFlags...), lout)
	}()
	ticket := lout.wait(t, lerrc)

	dout := newLines(t, "from")
	derrc := make(chan error, 1)
	go func() {
		args := append([]string{"-from", "127.0.0.1:0"}, directFlags...)
		derrc <- run(ctx, append(args, ticket), dout)
	}()
	addr = dout.wait(t, derrc)

	var stopped bool
	stop = func() (error, error) {
		stopped = true
		cancel()
		return waitErr(t, lerrc), waitErr(t, derrc)
	}
	t.Cleanup(func() {
		if !stopped {
			stop()
		}
	})
	return addr, stop, dout
}

func waitErr(t *testing.T, errc <-chan error) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancellation")
		return nil
	}
}

// roundTrip sends payload through addr, half-closes, and returns everything
// read back until the server's EOF.
func roundTrip(addr string, payload []byte) ([]byte, error) {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(20 * time.Second))
	errc := make(chan error, 1)
	go func() {
		_, err := c.Write(payload)
		if err == nil {
			err = c.(*net.TCPConn).CloseWrite()
		}
		errc <- err
	}()
	got, err := io.ReadAll(c)
	if werr := <-errc; err == nil {
		err = werr
	}
	return got, err
}

func TestForwardEcho(t *testing.T) {
	addr, stop := forwarder(t, echoServer(t, ""))

	large := make([]byte, 8<<20)
	rand.Read(large)
	payloads := [][]byte{
		nil,
		[]byte("x"),
		[]byte("hello, forwarder"),
		bytes.Repeat([]byte("abcdefgh"), 4096),
		large,
	}
	for i := range 8 {
		payloads = append(payloads, []byte(fmt.Sprintf("client %d", i)))
	}

	var wg sync.WaitGroup
	for i, p := range payloads {
		wg.Go(func() {
			got, err := roundTrip(addr, p)
			if err != nil {
				t.Errorf("client %d: %v", i, err)
				return
			}
			if !bytes.Equal(got, p) {
				t.Errorf("client %d: echoed %d bytes, want %d", i, len(got), len(p))
			}
		})
	}
	wg.Wait()

	lerr, derr := stop()
	if !errors.Is(lerr, context.Canceled) || !errors.Is(derr, context.Canceled) {
		t.Fatalf("run = %v, %v; want %v", lerr, derr, context.Canceled)
	}
}

// TestForwardHalfClose checks that a client which has finished sending can
// still read the reply: the server sees EOF, then answers.
func TestForwardHalfClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		req, err := io.ReadAll(c)
		if err != nil {
			return
		}
		fmt.Fprintf(c, "got %d bytes", len(req))
	}()

	addr, _ := forwarder(t, ln.Addr().String())
	got, err := roundTrip(addr, []byte("request"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "got 7 bytes"; string(got) != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

// TestForwardServerSpeaksFirst checks that a client that sends nothing still
// reaches the target, as SSH and SMTP clients wait for the server's banner.
func TestForwardServerSpeaksFirst(t *testing.T) {
	const banner = "220 ready\r\n"
	addr, _ := forwarder(t, echoServer(t, banner))

	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, len(banner))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read banner: %v", err)
	}
	if string(buf) != banner {
		t.Fatalf("banner = %q, want %q", buf, banner)
	}
}

// TestForwardSurvivesFailedConnection checks that a client resetting its
// connection mid-transfer does not stop the forwarder.
func TestForwardSurvivesFailedConnection(t *testing.T) {
	addr, _, dlog := forwarderLog(t, echoServer(t, ""))

	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(10 * time.Second))
	c.Write(bytes.Repeat([]byte("x"), 1<<20))
	c.(*net.TCPConn).SetLinger(0)
	c.Close()
	select {
	case line := <-dlog.more:
		if !strings.Contains(line, "forward") {
			t.Fatalf("logged %q, want a forwarding error", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reset connection was not logged")
	}

	got, err := roundTrip(addr, []byte("still here"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "still here" {
		t.Fatalf("echo = %q", got)
	}
}

// TestForwardShutdown checks that cancellation ends both sides promptly with
// a forwarded connection still open, and that the connection is closed.
func TestForwardShutdown(t *testing.T) {
	addr, stop := forwarder(t, echoServer(t, ""))

	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}

	lerr, derr := stop()
	if !errors.Is(lerr, context.Canceled) || !errors.Is(derr, context.Canceled) {
		t.Fatalf("run = %v, %v; want %v", lerr, derr, context.Canceled)
	}
	if _, err := c.Read(buf); err == nil {
		t.Fatal("forwarded connection still open after shutdown")
	} else if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
		t.Fatalf("forwarded connection not closed: %v", err)
	}
}

// TestForwardConnectionLost checks that the dialing side exits with an error,
// not success, when the listener goes away.
func TestForwardConnectionLost(t *testing.T) {
	lctx, lcancel := context.WithCancel(context.Background())
	defer lcancel()
	target := echoServer(t, "")
	lout := newLines(t, "listen")
	lerrc := make(chan error, 1)
	go func() {
		lerrc <- run(lctx, append([]string{"-listen", "-to", target}, directFlags...), lout)
	}()
	ticket := lout.wait(t, lerrc)

	dctx, dcancel := context.WithCancel(context.Background())
	defer dcancel()
	dout := newLines(t, "from")
	derrc := make(chan error, 1)
	go func() {
		args := append([]string{"-from", "127.0.0.1:0"}, directFlags...)
		derrc <- run(dctx, append(args, ticket), dout)
	}()
	dout.wait(t, derrc)

	lcancel()
	waitErr(t, lerrc)
	if err := waitErr(t, derrc); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want a lost-connection error", err)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestRunUsage(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"neither", nil, "one of -listen or -from is required"},
		{"both", []string{"-listen", "-to", "127.0.0.1:1", "-from", "127.0.0.1:0"}, "mutually exclusive"},
		{"listen without to", []string{"-listen"}, "-listen requires -to"},
		{"listen operand", []string{"-listen", "-to", "127.0.0.1:1", "extra"}, "takes no operands"},
		{"to without listen", []string{"-from", "127.0.0.1:0", "-to", "127.0.0.1:1", "x"}, "-to applies only with -listen"},
		{"from without peer", []string{"-from", "127.0.0.1:0"}, "usage"},
		{"bad timeout", []string{"-listen", "-to", "127.0.0.1:1", "-timeout", "0s"}, "-timeout must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run = %v, want error containing %q", err, tt.want)
			}
		})
	}
}
