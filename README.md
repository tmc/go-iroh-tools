# go-iroh-tools

`go-iroh-tools` provides small Unix-style networking tools backed by
`github.com/tmc/go-iroh`.

The first tools are:

- `iaddr`: print an iroh endpoint id, local address, endpoint address, and ticket.
- `ilisten`: listen on an iroh ALPN and bridge one stream to stdin/stdout, or echo.
- `inc`: connect to an endpoint ticket and bridge stdin/stdout.
- `iping`: measure stream round-trip time against an iroh ping listener.

Examples:

```sh
go run ./cmd/ilisten -echo
go run ./cmd/inc <endpoint-ticket>

go run ./cmd/iping -listen
go run ./cmd/iping <endpoint-ticket>
```

Tickets are endpoint tickets from `github.com/tmc/go-iroh/endpointticket`.
