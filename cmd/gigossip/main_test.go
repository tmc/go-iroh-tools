package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/gossip"
	"lukechampine.com/blake3"
)

func TestParseTopic(t *testing.T) {
	hexID := strings.Repeat("ab", 32)
	var fromHex gossip.TopicID
	hex.Decode(fromHex[:], []byte(hexID))
	tests := []struct {
		name string
		in   string
		want gossip.TopicID
	}{
		{"hex", hexID, fromHex},
		{"name", "chat", blake3.Sum256([]byte("chat"))},
		{"64 non-hex", strings.Repeat("z", 64), blake3.Sum256([]byte(strings.Repeat("z", 64)))},
		{"63 hex", hexID[:63], blake3.Sum256([]byte(hexID[:63]))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTopic(tt.in)
			if err != nil {
				t.Fatalf("parseTopic(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("parseTopic(%q) = %x, want %x", tt.in, got, tt.want)
			}
		})
	}
	if _, err := parseTopic(""); err == nil {
		t.Error("parseTopic(\"\") succeeded, want error")
	}
}

func TestReadLines(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		limit int
		want  []string // "!n" is a skipped line of n bytes
	}{
		{"lines", "a\nbb\n", 5, []string{"a", "bb"}},
		{"crlf", "a\r\nb\r\n", 5, []string{"a", "b"}},
		{"no final newline", "a\nlast", 5, []string{"a", "last"}},
		{"empty line", "\nx\n", 5, []string{"", "x"}},
		{"too long", "123456\nok\n", 5, []string{"!6", "ok"}},
		{"exactly limit", "12345\n", 5, []string{"12345"}},
		{"far too long", strings.Repeat("x", 100) + "\nok\n", 20, []string{"!100", "ok"}},
		{"empty input", "", 5, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := make(chan line)
			go readLines(context.Background(), strings.NewReader(tt.in), tt.limit, lines)
			var got []string
			for l := range lines {
				if l.err != nil {
					if l.err != io.EOF {
						t.Fatalf("read error: %v", l.err)
					}
					break
				}
				if l.data == nil {
					got = append(got, "!"+strconv.Itoa(l.n))
					continue
				}
				got = append(got, string(l.data))
			}
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Errorf("lines = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFrameSize(t *testing.T) {
	for _, n := range []int{1, 600, 4096, 10000} {
		m := frameSize(n)
		if got := gossip.NewGossip(nil, gossip.WithMaxMessageSize(m)).MaxContentSize(); got < n {
			t.Errorf("frameSize(%d) = %d with content limit %d, want at least %d", n, m, got, n)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no topic", nil},
		{"negative max size", []string{"-max-size", "-1", "t"}},
		{"sub-only neighbors", []string{"-sub-only", "-neighbors", "t"}},
		{"bad bootstrap", []string{"t", "not-a-ticket"}},
		{"bad flag", []string{"-nope", "t"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args, strings.NewReader(""), io.Discard, io.Discard)
			if err == nil {
				t.Fatal("run succeeded, want error")
			}
		})
	}
}

// A node is one running gigossip.
type node struct {
	t      *testing.T
	ticket string
	stdin  *io.PipeWriter
	out    chan string // stdout lines
	log    chan string // stderr lines after the ticket
	done   chan error
	cancel context.CancelFunc
}

// start runs gigossip with args on a loopback, relay-free endpoint and
// returns once it has printed its ticket.
func start(t *testing.T, args ...string) *node {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	n := &node{
		t:      t,
		stdin:  inW,
		out:    make(chan string, 1000),
		log:    make(chan string, 1000),
		done:   make(chan error, 1),
		cancel: cancel,
	}
	args = append([]string{"-bind", "127.0.0.1:0", "-direct-only", "-v"}, args...)
	go func() {
		err := run(ctx, args, inR, outW, errW)
		outW.Close()
		errW.Close()
		n.done <- err
	}()
	go scanLines(outR, n.out)
	ticket := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(errR)
		if s.Scan() {
			ticket <- s.Text()
		}
		close(ticket)
		for s.Scan() {
			n.log <- s.Text()
		}
		close(n.log)
	}()
	t.Cleanup(func() {
		cancel()
		inW.Close()
		select {
		case <-n.done:
		case <-time.After(10 * time.Second):
			t.Error("gigossip did not exit after cancellation")
		}
	})
	select {
	case n.ticket = <-ticket:
	case <-time.After(10 * time.Second):
		t.Fatal("no ticket")
	}
	if _, err := tool.ParseEndpoint(n.ticket); err != nil {
		t.Fatalf("first stderr line %q is not a ticket: %v", n.ticket, err)
	}
	return n
}

func scanLines(r io.Reader, c chan<- string) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		c <- s.Text()
	}
	close(c)
}

func (n *node) shortID() string {
	addr, err := tool.ParseEndpoint(n.ticket)
	if err != nil {
		n.t.Fatal(err)
	}
	return addr.ID.Short()
}

func (n *node) send(s string) {
	n.t.Helper()
	if _, err := io.WriteString(n.stdin, s+"\n"); err != nil {
		n.t.Fatalf("write stdin: %v", err)
	}
}

// expect waits for the next stdout line and checks it.
func (n *node) expect(want string) {
	n.t.Helper()
	select {
	case got, ok := <-n.out:
		if !ok {
			n.t.Fatalf("stdout closed, want %q", want)
		}
		if got != want {
			n.t.Fatalf("stdout = %q, want %q", got, want)
		}
	case <-time.After(10 * time.Second):
		n.t.Fatalf("timed out waiting for %q", want)
	}
}

// expectPayload waits for the next stdout line and checks it, ignoring a
// -prefix field.
func (n *node) expectPayload(want string) {
	n.t.Helper()
	select {
	case got, ok := <-n.out:
		if !ok {
			n.t.Fatalf("stdout closed, want %q", want)
		}
		if _, payload, _ := strings.Cut(got, "\t"); payload != want {
			n.t.Fatalf("stdout = %q, want payload %q", got, want)
		}
	case <-time.After(10 * time.Second):
		n.t.Fatalf("timed out waiting for %q", want)
	}
}

// waitLog waits for count stderr lines containing substr.
func (n *node) waitLog(substr string, count int) {
	n.t.Helper()
	timeout := time.After(10 * time.Second)
	for count > 0 {
		select {
		case l, ok := <-n.log:
			if !ok {
				n.t.Fatalf("stderr closed waiting for %q", substr)
			}
			if strings.Contains(l, substr) {
				count--
			}
		case <-timeout:
			n.t.Fatalf("timed out waiting for stderr %q", substr)
		}
	}
}

// stop cancels the node and checks that it exits cleanly.
func (n *node) stop() {
	n.t.Helper()
	n.cancel()
	select {
	case err := <-n.done:
		if !errors.Is(err, context.Canceled) {
			n.t.Errorf("run = %v, want %v", err, context.Canceled)
		}
		n.done <- err
	case <-time.After(10 * time.Second):
		n.t.Fatal("gigossip did not exit after cancellation")
	}
}

// running checks that the node has not exited.
func (n *node) running() {
	n.t.Helper()
	select {
	case err := <-n.done:
		n.t.Fatalf("gigossip exited early: %v", err)
	default:
	}
}

func TestTwoNodes(t *testing.T) {
	for _, mode := range []string{"swarm", "neighbors"} {
		t.Run(mode, func(t *testing.T) {
			var extra []string
			if mode == "neighbors" {
				extra = []string{"-neighbors"}
			}
			topic := "two-" + mode
			a := start(t, append(extra, "-prefix", topic)...)
			b := start(t, append(extra, topic, a.ticket)...)
			b.waitLog("joined", 1)
			a.waitLog("neighbor up", 1)

			b.send("hello from b")
			a.expect(b.shortID() + "\thello from b")
			a.send("hello from a")
			b.expect("hello from a")

			// The end of stdin must not stop either node.
			a.stdin.Close()
			b.stdin.Close()
			a.waitLog("end of input", 1)
			b.waitLog("end of input", 1)
			a.running()
			b.running()

			// Both still receive: a from c directly, and b through a.
			c := start(t, topic, a.ticket)
			c.waitLog("joined", 1)
			c.send("after eof")
			// b and c may be neighbors by now, so the line can reach a
			// through either; -prefix was checked above with two nodes.
			a.expectPayload("after eof")
			if mode == "swarm" {
				b.expect("after eof")
			}

			c.stop()
			b.stop()
			a.stop()
		})
	}
}

func TestThreeNodes(t *testing.T) {
	const topic = "three"
	a := start(t, topic)
	b := start(t, topic, a.ticket)
	c := start(t, topic, a.ticket)
	b.waitLog("joined", 1)
	c.waitLog("joined", 1)
	a.waitLog("neighbor up", 2)

	b.send("from b")
	a.expect("from b")
	c.expect("from b")

	c.send("from c")
	a.expect("from c")
	b.expect("from c")

	a.send("from a")
	b.expect("from a")
	c.expect("from a")

	b.stdin.Close()
	b.waitLog("end of input", 1)
	c.send("to b after eof")
	b.expect("to b after eof")

	for _, n := range []*node{a, b, c} {
		n.running()
	}
	for _, n := range []*node{c, b, a} {
		n.stop()
	}
}

func TestMaxSize(t *testing.T) {
	const topic = "max-size"
	a := start(t, "-max-size", "5", topic)
	b := start(t, topic, a.ticket)
	b.waitLog("joined", 1)
	a.waitLog("neighbor up", 1)

	a.send("toolong")
	a.waitLog("skipped 7-byte line", 1)
	a.send("ok")
	b.expect("ok")

	b.send("123456")
	a.waitLog("dropped 6-byte message", 1)
	b.send("short")
	a.expect("short")
}

func TestSubOnly(t *testing.T) {
	const topic = "sub-only"
	a := start(t, "-sub-only", topic)
	b := start(t, topic, a.ticket)
	b.waitLog("joined", 1)
	// a never reads stdin, so writing to it would block; a only receives.
	b.send("to sub")
	a.expect("to sub")
}

// TestJoinBeforeSend checks that a node with bootstrap peers holds its first
// line until it has joined: the line is written before the node can have a
// neighbor, and must still arrive.
func TestJoinBeforeSend(t *testing.T) {
	const topic = "join-wait"
	a := start(t, topic)
	b := start(t, topic, a.ticket)
	go io.WriteString(b.stdin, "first\n")
	a.expect("first")
}

// TestLargeMaxSize checks that a -max-size above the default content limit
// raises the frame limit far enough to carry such a line.
func TestLargeMaxSize(t *testing.T) {
	const topic = "large"
	big := strings.Repeat("x", 6000)
	a := start(t, "-max-size", "8000", topic)
	b := start(t, "-max-size", "8000", topic, a.ticket)
	b.waitLog("joined", 1)
	b.send(big)
	a.expect(big)
}
