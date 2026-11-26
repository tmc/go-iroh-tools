// Gigossip publishes stdin lines to an iroh-gossip topic and prints the
// messages it receives on that topic to stdout.
//
// Usage:
//
//	gigossip [flags] <topic> [bootstrap-ticket-or-id...]
//
// The topic is a 64-character hex topic ID, or any other string, which names
// the topic whose ID is the BLAKE3 hash of that string. Every node of a swarm
// must name the same topic. Bootstrap peers are endpoint tickets or bare
// endpoint ids of nodes already subscribed to it; a node given none waits for
// others to join through it.
//
// Gigossip prints its own endpoint ticket to stderr once it is subscribed and
// reachable, so another node can bootstrap from that line. With bootstrap
// peers it then waits until it has a neighbor before sending anything.
//
// Each stdin line, without its line ending, is broadcast to the whole swarm,
// or with -neighbors to direct neighbors only. A line sent while the node has
// no neighbors reaches no one. Gossip identifies a swarm message by the hash
// of its content, so a line broadcast to the swarm that is identical to one
// seen there in the last 90 seconds is not delivered again. With -sub-only
// stdin is not read.
//
// Each message received is printed to stdout as one line. With -prefix the
// line starts with the short id of the neighbor that delivered the message
// and a tab; gossip does not carry the original author, so for a message
// relayed through the swarm that is the relaying neighbor.
//
// The end of stdin does not stop gigossip: it keeps receiving until
// interrupted, and then exits zero. With -v it logs neighbors coming and going
// to stderr. It always warns there when it falls behind and drops messages.
//
// -max-size limits messages to n bytes of content. Longer stdin lines are
// skipped with a warning, and longer received messages are dropped with one.
// The default is the largest content the gossip frame limit allows. A size
// above that raises the frame limit, and every node of the swarm must then
// use the same -max-size, since a node drops frames above its own limit.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/tmc/go-iroh-tools/internal/tool"
	"github.com/tmc/go-iroh/gossip"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"lukechampine.com/blake3"
)

// shutdownTimeout bounds how long gigossip waits on exit for its leave
// messages to reach neighbors.
const shutdownTimeout = 2 * time.Second

func main() {
	tool.Run(func(ctx context.Context, args []string) error {
		return run(ctx, args, os.Stdin, os.Stdout, os.Stderr)
	})
}

// config holds the parsed command line.
type config struct {
	bind      tool.BindFlags
	subOnly   bool
	prefix    bool
	neighbors bool
	maxSize   int
	verbose   bool
	topic     gossip.TopicID
	bootstrap []netaddr.EndpointAddr
}

func parseArgs(args []string, stderr io.Writer) (*config, error) {
	fs := flag.NewFlagSet("gigossip", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c config
	fs.BoolVar(&c.subOnly, "sub-only", false, "receive only; do not read stdin")
	fs.BoolVar(&c.prefix, "prefix", false, "prefix each received line with the delivering neighbor's short id and a tab")
	fs.BoolVar(&c.neighbors, "neighbors", false, "send to direct neighbors only, not the whole swarm")
	fs.IntVar(&c.maxSize, "max-size", 0, "limit messages to `n` bytes of content (0 means the gossip default)")
	fs.BoolVar(&c.verbose, "v", false, "log neighbor changes to stderr")
	tool.RegisterBindFlags(fs, &c.bind)
	if err := tool.ParseFlags(fs, args); err != nil {
		return nil, err
	}
	if fs.NArg() < 1 {
		return nil, errors.New("usage: gigossip [flags] <topic> [bootstrap-ticket-or-id...]")
	}
	if c.maxSize < 0 {
		return nil, fmt.Errorf("-max-size must not be negative, got %d", c.maxSize)
	}
	if c.subOnly && c.neighbors {
		return nil, errors.New("-neighbors applies only when sending; -sub-only does not send")
	}
	var err error
	if c.topic, err = parseTopic(fs.Arg(0)); err != nil {
		return nil, err
	}
	for _, s := range fs.Args()[1:] {
		addr, err := tool.ParseEndpoint(s)
		if err != nil {
			return nil, err
		}
		c.bootstrap = append(c.bootstrap, addr)
	}
	return &c, nil
}

// parseTopic returns the topic ID s names: s itself if it is 64 hex
// characters, and otherwise the BLAKE3 hash of s.
func parseTopic(s string) (gossip.TopicID, error) {
	if s == "" {
		return gossip.TopicID{}, errors.New("empty topic")
	}
	var id gossip.TopicID
	if len(s) == 2*len(id) {
		if _, err := hex.Decode(id[:], []byte(s)); err == nil {
			return id, nil
		}
	}
	return gossip.TopicID(blake3.Sum256([]byte(s))), nil
}

// frameSize returns the smallest gossip frame limit whose content limit is
// at least n bytes.
func frameSize(n int) int {
	m := n
	for gossip.NewGossip(nil, gossip.WithMaxMessageSize(m)).MaxContentSize() < n {
		m++
	}
	return m
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	c, err := parseArgs(args, stderr)
	if err != nil {
		return err
	}
	stderr = &lockedWriter{w: stderr}

	var opts []gossip.GossipOption
	if c.maxSize > gossip.NewGossip(nil).MaxContentSize() {
		opts = append(opts, gossip.WithMaxMessageSize(frameSize(c.maxSize)))
	}

	ep, closeEndpoint, err := tool.Bind(ctx, c.bind)
	if err != nil {
		return err
	}
	defer closeEndpoint()
	g := gossip.NewGossip(ep, opts...)
	router, err := iroh.NewRouter(ep, map[string]iroh.ProtocolHandler{gossip.ALPN: g.Handler()}, nil)
	if err != nil {
		return fmt.Errorf("start router: %w", err)
	}
	defer router.Shutdown(context.Background())

	// Subscribe before printing the ticket: a peer that joins on seeing it
	// would otherwise have its Join dropped and wait for the retry.
	topic, err := g.SubscribeWithOpts(ctx, c.topic, gossip.JoinOptions{Bootstrap: c.bootstrap})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	limit := topic.MaxContentSize()
	if c.maxSize > 0 {
		limit = c.maxSize
	}

	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		g.Shutdown(ctx)
	}
	defer shutdown()

	if err := tool.WaitRelay(ctx, ep, c.bind); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(stderr, tool.LocalTicket(ep)); err != nil {
		return err
	}

	// The event stream buffers from Subscribe, so starting to read it only
	// now loses nothing and keeps the ticket the first line on stderr.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	received := make(chan struct{})
	go func() {
		defer close(received)
		if err := receive(topic, c, limit, stdout, stderr); err != nil {
			cancel(err)
		}
	}()
	// Shutdown closes the event stream, which ends receive.
	defer func() {
		shutdown()
		<-received
	}()

	if len(c.bootstrap) > 0 {
		if err := topic.Joined(ctx); err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return fmt.Errorf("join: %w", err)
		}
		if c.verbose {
			fmt.Fprintln(stderr, "gigossip: joined")
		}
	}
	if !c.subOnly {
		if err := send(ctx, topic, c, limit, stdin, stderr); err != nil {
			return err
		}
	}
	return wait(ctx)
}

// wait blocks until ctx is done and reports why.
func wait(ctx context.Context) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

// receive prints the topic's messages until its event stream ends.
// It stops early only if stdout cannot be written.
func receive(topic *gossip.Topic, c *config, limit int, stdout, stderr io.Writer) error {
	var buf []byte
	for ev := range topic.Events() {
		switch ev.Kind {
		case gossip.Received:
			if len(ev.Content) > limit {
				fmt.Fprintf(stderr, "gigossip: dropped %d-byte message from %s: exceeds limit of %d bytes\n", len(ev.Content), ev.DeliveredFrom.Short(), limit)
				continue
			}
			buf = buf[:0]
			if c.prefix {
				buf = append(buf, ev.DeliveredFrom.Short()...)
				buf = append(buf, '\t')
			}
			buf = append(buf, ev.Content...)
			buf = append(buf, '\n')
			if _, err := stdout.Write(buf); err != nil {
				return fmt.Errorf("write output: %w", err)
			}
		case gossip.NeighborUp:
			if c.verbose {
				fmt.Fprintf(stderr, "gigossip: neighbor up %s\n", ev.Peer)
			}
		case gossip.NeighborDown:
			if c.verbose {
				fmt.Fprintf(stderr, "gigossip: neighbor down %s\n", ev.Peer)
			}
		case gossip.Lagged:
			fmt.Fprintf(stderr, "gigossip: fell behind and dropped %d events\n", ev.Dropped)
		}
	}
	return nil
}

// send broadcasts each line of stdin until it ends or ctx is done.
//
// A read from stdin cannot be interrupted, so the lines are read in a
// goroutine that send does not wait for; it ends at the next line or at the
// end of input.
func send(ctx context.Context, topic *gossip.Topic, c *config, limit int, stdin io.Reader, stderr io.Writer) error {
	lines := make(chan line)
	go readLines(ctx, stdin, limit, lines)
	broadcast := topic.Broadcast
	if c.neighbors {
		broadcast = topic.BroadcastNeighbors
	}
	for {
		var l line
		select {
		case <-ctx.Done():
			return nil
		case l = <-lines:
		}
		if l.err != nil {
			if l.err == io.EOF {
				if c.verbose {
					fmt.Fprintln(stderr, "gigossip: end of input; still receiving")
				}
				return nil
			}
			return fmt.Errorf("read input: %w", l.err)
		}
		if l.data == nil {
			fmt.Fprintf(stderr, "gigossip: skipped %d-byte line: exceeds limit of %d bytes\n", l.n, limit)
			continue
		}
		if err := broadcast(ctx, l.data); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("broadcast: %w", err)
		}
	}
}

// A line is one line of input without its line ending. A line longer than
// the limit has nil data and its length in n. The last value sent has a
// non-nil err, which is io.EOF at the end of input.
type line struct {
	data []byte
	n    int
	err  error
}

// readLines sends the lines of r on lines until r ends or ctx is done.
// Its memory use is bounded by limit, whatever the length of a line.
func readLines(ctx context.Context, r io.Reader, limit int, lines chan<- line) {
	// Two more bytes hold a "\r\n" line ending.
	br := bufio.NewReaderSize(r, limit+2)
	for {
		l := readLine(br, limit)
		select {
		case lines <- l:
		case <-ctx.Done():
			return
		}
		if l.err != nil {
			return
		}
	}
}

// readLine reads one line from br, whose buffer must hold limit+2 bytes.
// A final line without a line ending is returned before io.EOF.
func readLine(br *bufio.Reader, limit int) line {
	b, err := br.ReadSlice('\n')
	n := len(b)
	long := false
	for err == bufio.ErrBufferFull {
		long = true
		b, err = br.ReadSlice('\n')
		n += len(b)
	}
	if err != nil && (err != io.EOF || n == 0) {
		return line{err: err}
	}
	n -= len(b) - len(trimEOL(b))
	if long || n > limit {
		return line{n: n}
	}
	return line{data: bytes.Clone(trimEOL(b)), n: n}
}

// trimEOL removes a trailing "\n" or "\r\n" from b.
func trimEOL(b []byte) []byte {
	b, ok := bytes.CutSuffix(b, []byte("\n"))
	if ok {
		b, _ = bytes.CutSuffix(b, []byte("\r"))
	}
	return b
}

// lockedWriter serializes writes to w.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}
