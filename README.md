# go-iroh-tools

`go-iroh-tools` provides small Unix-style networking tools backed by
`github.com/tmc/go-iroh`.

Install a command by its module path and version:

```sh
go install github.com/tmc/go-iroh-tools/cmd/ginc@latest
```

Each command is versioned with this repository. Before tagging a release,
test all commands against the `go-iroh` version recorded in `go.mod`.

The tools are:

- `giaddr`: print an iroh endpoint id, local address, endpoint address, and ticket.
- `gilisten`: listen on an iroh ALPN and bridge one stream to stdin/stdout, or echo.
- `ginc`: connect to an endpoint ticket and bridge stdin/stdout.
- `giping`: measure stream round-trip time against an iroh ping listener.
- `giperf`: measure iroh stream throughput.
- `gistat`: print connection statistics and path information.
- `giticket`: inspect, create, and shorten endpoint tickets.
- `gikey`: generate keys and inspect endpoint IDs.
- `girelay`: print and normalize relay URLs.
- `giblob`: serve a file or stdin as a verified blob, and fetch one by ticket.
- `gigossip`: publish stdin lines to a gossip topic and print what arrives.
- `giforward`: forward TCP connections across an iroh connection.
- `gireport`: report UDP reachability, public addresses, and relay latency.
- `gilookup`: look up the addresses an endpoint id has published.

Run `go doc ./cmd/<name>` for a command's input, output, and exit contract.

## Tickets and readiness

Every listener prints an endpoint ticket to stderr and its payload to stdout,
so the two are redirected separately. The ticket is printed only after the
protocol is registered and the endpoint is reachable: a client that dials on
seeing that line will find a listener ready, with no settling delay.

```sh
go run ./cmd/gilisten -echo -direct-only 2>ticket &
go run ./cmd/ginc -direct-only "$(head -1 ticket)" <request >response
```

Tickets are endpoint tickets from `github.com/tmc/go-iroh/endpointticket`.
Wherever a command takes an endpoint id, both hex and z-base-32 are accepted,
so an id printed by any of these tools can be pasted into any other.

## Identity

Without `-key`, every run generates a fresh key, so its endpoint id and
tickets name that run only. `-key file` loads a secret key as printed by
`gikey generate`, so a listener keeps its id across restarts:

```sh
go run ./cmd/gikey generate >key
chmod 600 key
go run ./cmd/gilisten -key key -echo
```

The id is stable, but a ticket still records the addresses of the run that
printed it, and these tools neither publish addresses nor look them up when
dialing. After a restart, hand clients a fresh ticket; the id lets them
check it names the endpoint they expect. `gilookup` finds addresses only for
endpoints that publish them, such as other iroh software.

## Piping

`ginc` and `gilisten` are iroh-native `nc`. Both directions run at once, so a
payload of any size can be sent while the peer replies:

```sh
go run ./cmd/gilisten -echo -1 -direct-only 2>ticket &
dd if=/dev/urandom bs=1m count=64 | go run ./cmd/ginc -direct-only "$(head -1 ticket)" >echoed
```

EOF on stdin half-closes the send direction only. The peer sees the end of the
request and its reply is still read, which is what lets a request/response
exchange work over a pipe. `ginc` exits when the peer closes its own send
direction.

`-1` means one connection: `gilisten -1` admits exactly one client and exits
when that connection ends. It is required for the stdio bridge, because stdin
cannot be split across concurrent clients; `-echo` without `-1` serves every
client until interrupted.

An interrupt ends a transfer and exits zero. Bytes already written to stdout
are kept; no tool promises that a stream ended this way is complete.

## Binding and relays

The default bind address is `127.0.0.1:0`, a loopback ephemeral port, and
relays are on by default. A ticket minted by default therefore advertises a
loopback direct address plus a relay address: only this host can take the
direct path, and anyone else reaches it through the relay.

```sh
# Same host, no network at all: skip the relay entirely.
go run ./cmd/giping -listen -direct-only

# Another host on a reachable network: bind an address it can route to.
go run ./cmd/giping -listen -bind 0.0.0.0:0

# Anywhere else: the default relay path already covers it.
go run ./cmd/giping -listen
```

Binding to a wildcard address exposes the endpoint to every network the host
is on, so it is a choice rather than a default.

`-relay=false` and `-direct-only` both leave an endpoint reachable only over
direct addresses, but they are not the same thing: `-relay=false` declines to
configure any relay, while `-direct-only` disables relay transports outright.
`-direct-only` takes precedence over `-relay`. Prefer it when direct-only
operation is the intent.

`-relay-url url` (repeatable) replaces the default relays with the ones
named, and `-staging` selects number0's staging relays. A relay URL may carry
a path prefix, which is kept. Both choose relays, so each is an error with the
other, with `-direct-only`, or with `-relay=false`.

With relays enabled — the default — a listener waits up to ten seconds to reach
its home relay before printing its ticket, and fails if it cannot. Relay
service is thus a startup dependency of the default configuration. Pass
`-direct-only` to skip the wait, which is what the loopback examples here do so
they run offline.

## Debugging

`-qlog dir` writes a qlog trace of each QUIC connection into dir, and
`-keylog file` appends the TLS secrets of direct connections to file in NSS
key log format, for Wireshark. A key log lets anyone holding it decrypt those
connections; use it only for debugging.

## Measurement contracts

`giping` reports application stream round-trip time. Each probe opens a stream
and waits for the peer's application to echo it, so the number includes serving
the request and reads higher than `ping(8)`. It exits zero if any probe is
answered and nonzero if none is, so partial loss shows up in the summary rather
than the exit status.

`giperf` reports a transfer only after the receiver confirms the byte count it
consumed, so a reported result means the bytes arrived, not merely that the
local writes returned. It prints two durations: `write`, until the last byte
was handed to the transport, and `total`, until the confirmation arrived.
`total` is the honest end-to-end figure.

`gistat` describes a connection it makes itself. It does not enumerate other
processes' connections the way `ss(8)` does; there is no system table of iroh
connections to read. A fresh connection reports little, so use `-n` and `-i` to
sample repeatedly and watch paths settle.

`gireport` binds to the wildcard address by default, unlike the other
tools, so its probes can leave the host. It waits for one network report and
prints it; it never invents a field the report does not have, such as
hairpinning.

`girelay` is offline in both forms. Printing the relay map reports what this
build is configured to use and normalizing a URL is string work; neither
contacts a relay, so neither says whether a relay is reachable.

## Machine-readable output

`giaddr`, `giping`, `gistat`, `gireport`, and `gilookup` take `-json`.
`giaddr`, `giping`, and `gireport` print one object; `gistat` prints one
object per sample and `gilookup` one per record, each on its own line.
Durations are integer nanoseconds in fields ending in `_ns`, and lists are
empty arrays rather than null. `-json` and `-f` are mutually exclusive.

## Exit status

A command exits 0 on success, 2 for a command-line mistake such as an unknown
flag, and 1 for anything else. A run stopped by SIGINT or SIGTERM exits 0: the
operator asked it to stop and it stopped. A command that needs an interruption
to count as failure says so itself — `giping` reports total loss as an error
whether or not the run was cut short, and `gireport` and `gilookup` fail when
interrupted before they have an answer to print.

## Examples

```sh
go run ./cmd/giaddr
go run ./cmd/giaddr -f ticket

go run ./cmd/giping -listen -direct-only
go run ./cmd/giping -c 10 <endpoint-ticket>

go run ./cmd/giperf -listen -direct-only
go run ./cmd/giperf -n 256MiB <endpoint-ticket>

go run ./cmd/gistat -listen -direct-only
go run ./cmd/gistat -n 5 -i 2s <endpoint-ticket>

go run ./cmd/giticket <endpoint-ticket>
go run ./cmd/giticket -id <endpoint-id> -addr ip:127.0.0.1:1234
go run ./cmd/giticket -short -f ticket <endpoint-ticket>

go run ./cmd/gikey generate >key
go run ./cmd/gikey public <key
go run ./cmd/gikey inspect <public-key-or-endpoint-id>

go run ./cmd/girelay
go run ./cmd/girelay -staging

go run ./cmd/giblob -serve -direct-only file 2>ticket &
go run ./cmd/giblob -direct-only -o copy "$(head -1 ticket)"

go run ./cmd/gigossip chat 2>ticket          # first node
go run ./cmd/gigossip chat <endpoint-ticket> # every other node

go run ./cmd/giforward -listen -to 127.0.0.1:22 2>ticket &
go run ./cmd/giforward -from 127.0.0.1:2222 "$(head -1 ticket)" &
ssh -p 2222 127.0.0.1

go run ./cmd/gireport
go run ./cmd/gireport -json

go run ./cmd/gilookup <endpoint-id>
go run ./cmd/ginc "$(go run ./cmd/gilookup -f ticket <endpoint-id>)"
```

`giaddr`, `giticket`, `gireport`, and `gilookup` print several labeled fields; `-f` selects one and
prints its value alone, for substitution into another command.

The commands intentionally mirror familiar Unix tools:

- `giaddr` is the iroh-native `ip addr`/`ifconfig` inspection point.
- `ginc` and `gilisten` are iroh-native `nc`.
- `giping` is iroh-native `ping`.
- `giperf` is iroh-native `iperf`.
- `gistat` is a compact iroh-native `ss -i`/`netstat` view.
- `giblob` is a verified, iroh-native `scp` for one file.
- `giforward` is iroh-native `ssh -L`.
- `gireport` is an iroh-native `netcheck`.
- `gilookup` is iroh-native `dig`.
- `giticket`, `gikey`, and `girelay` cover the address, identity, and relay
  plumbing around those data-path tools.
