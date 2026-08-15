# go-iroh-tools expansion

This is an internal planning note, not user documentation. It records where
the tool set should grow next and why, so additions stay coherent with the
existing design.

## Design invariants

Every addition must keep these properties:

- Each tool mirrors a familiar Unix networking tool (`nc`, `ping`, `iperf`,
  `ss`); a new tool needs a recognizable analogue or it belongs elsewhere.
- Tools compose over pipes: data on stdout, diagnostics on stderr, tickets
  and IDs as plain text arguments.
- One command per binary, flags over subcommands, `internal/tool` for the
  shared endpoint/ticket plumbing.

## Current coverage

The nine tools (`giaddr`, `gilisten`, `ginc`, `giping`, `giperf`, `gistat`,
`giticket`, `gikey`, `girelay`) cover the connectivity layer only: endpoint
identity, addressing, streams, and path inspection. None of the protocol
packages (`blobs`, `gossip`, `docs`) or the diagnostic net-report machinery
is reachable from the command line.

## Planned additions, in order

### 1. gireport — iroh-native `mtr`/`traceroute` for reachability

The strongest gap. go-iroh's relay servers now answer net-report probes at
`/ping`, and the net-report machinery already classifies NAT behavior, relay
latency, and direct-path viability. `gireport` runs a report against the
default (or a given) relay map and prints one line per relay plus a summary:
NAT class, hairpinning, IPv4/IPv6 reachability, nearest relay.

Complements `giaddr` (what I look like) and `gistat` (what one connection
did) with "what the network lets me do".

### 2. giblob — iroh-native `curl`/`scp` for content

`blobs` has tickets, identifiers, and BAO transfer, but no CLI. Two modes:

    giblob serve <path>          # print blob ticket, serve until interrupted
    giblob fetch <blob-ticket>   # verified fetch to stdout or -o file

This is the `sendme` workflow as a pipe-friendly pair. Keep it to single
blobs first; collections can come later if needed.

### 3. gigossip — iroh-native pub/sub `tail -f`

Subscribe to a gossip topic and print each message as a line; read stdin and
broadcast each line. The symmetric stdin/stdout shape makes it a building
block for shell-scripted meshes:

    gigossip <topic> <bootstrap-ticket...>

### 4. Not planned

- Relay and DNS server commands: go-iroh ships `cmd/iroh-relay` and
  `cmd/iroh-dns-server`; duplicating them here adds nothing.
- A `docs` (multi-writer KV) tool: the API is not yet stable enough to
  freeze into a CLI surface; revisit after go-iroh tags a release.
- Key-exchange policy: a flag on existing tools (`-key-exchange`) if ever
  needed for handshake testing, not a tool.

## Infrastructure debt to pay alongside

- Integration tests: the stream tools (`gilisten`, `ginc`, `giping`,
  `giperf`, `gistat`) have no test files. Use rsc.io/script txtar tests that
  exercise listener/dialer pairs over loopback.
- A `-json` output mode on the inspection tools (`giaddr`, `gistat`,
  `gireport`) for scripting; default output stays human-oriented.
