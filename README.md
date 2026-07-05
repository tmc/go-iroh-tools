# go-iroh-tools

`go-iroh-tools` provides small Unix-style networking tools backed by
`github.com/tmc/go-iroh`.

The first tools are:

- `giaddr`: print an iroh endpoint id, local address, endpoint address, and ticket.
- `gilisten`: listen on an iroh ALPN and bridge one stream to stdin/stdout, or echo.
- `ginc`: connect to an endpoint ticket and bridge stdin/stdout.
- `giping`: measure stream round-trip time against an iroh ping listener.
- `giperf`: measure iroh stream throughput.
- `gistat`: print connection statistics and path information.
- `giticket`: inspect, create, and shorten endpoint tickets.
- `gikey`: generate keys and inspect endpoint IDs.
- `girelay`: print and normalize relay URLs.

Examples:

```sh
go run ./cmd/gilisten -echo
go run ./cmd/ginc <endpoint-ticket>

go run ./cmd/giping -listen
go run ./cmd/giping <endpoint-ticket>

go run ./cmd/giperf -listen
go run ./cmd/giperf -n 256MiB <endpoint-ticket>

go run ./cmd/gistat -listen
go run ./cmd/gistat <endpoint-ticket>

go run ./cmd/giticket <endpoint-ticket>
go run ./cmd/giticket -id <endpoint-id> -addr ip:127.0.0.1:1234

go run ./cmd/gikey generate
go run ./cmd/gikey inspect <endpoint-id-or-z32>

go run ./cmd/girelay
go run ./cmd/girelay -staging
```

Tickets are endpoint tickets from `github.com/tmc/go-iroh/endpointticket`.

The commands intentionally mirror familiar Unix tools:

- `giaddr` is the iroh-native `ip addr`/`ifconfig` inspection point.
- `ginc` and `gilisten` are iroh-native `nc`.
- `giping` is iroh-native `ping`.
- `giperf` is iroh-native `iperf`.
- `gistat` is a compact iroh-native `ss -i`/`netstat` view.
- `giticket`, `gikey`, and `girelay` cover the address, identity, and relay
  plumbing around those data-path tools.
