# CapDAG for Go

This public module is the Go mirror of CapDAG. Use it for Go applications and
cartridges that need the reference URN, definitions, dispatch, planning,
Machine Notation, Bifaci, host, relay, and cartridge-runtime contracts.

Rust is the behavioral reference. Shared numbered tests carry the same meaning
in every mirror that implements a feature; Go-specific implementation tests use
the reserved implementation range.

## Install the module

```bash
go get github.com/machinefabric/capdag-go
```

The module requires Go 1.21 or newer.

## Parse and build Cap URNs

```go
package main

import (
    "fmt"
    capdag "github.com/machinefabric/capdag-go"
)

func main() {
    parsed, err := capdag.NewCapUrnFromString(
        `cap:disbind;in="media:ext=pdf";out="media:enc=utf-8;page"`,
    )
    if err != nil {
        panic(err)
    }

    built := capdag.NewCapUrnBuilder().
        InSpec("media:ext=pdf").
        OutSpec("media:enc=utf-8;page").
        Marker("disbind").
        MustBuild()

    fmt.Println(parsed.ToString() == built.ToString())
}
```

Treat the parsed values as opaque. Use CapDAG predicates for equivalence,
conformance, dispatch, and ranking; do not route by splitting or comparing raw
strings.

## Find the relevant API

The module is organized around the same conceptual boundaries as the
[CapDAG specification](https://github.com/machinefabric/capdag/blob/main/docs/01-overview.md):

- `urn` and the root package provide Tagged, Media, and Cap URNs;
- `cap` provides capability definitions, arguments, outputs, and callers;
- `machine` and planner packages parse and plan Machine Notation;
- `bifaci` provides frames, streams, flow control, runtimes, hosts, and relay
  components; and
- registry packages resolve versioned fabric and cartridge definitions.

API comments beside exported Go identifiers are the language-specific
reference. The normative dispatch, effect, stream, and protocol semantics live
in the Rust specification rather than in copied tables here.

## Scaffold a Go cartridge

Every CapDAG CLI renders the same canonical starter project:

```bash
capdag new sentiment-tagger --go
cd sentiment-tagger
go mod tidy
go build -buildvcs=false -o sentiment-tagger .
capdag dev-install .
echo "I love this" | capdag sentiment-tagger
```

The generated README explains its model-backed peer call and development loop.
See [Build and Run a Cartridge](https://github.com/machinefabric/capdag/blob/main/docs/18.2-getting-started-cartridge-development.md)
for the language-neutral tutorial.

## Verify changes

```bash
go test ./...
```

When changing shared behavior, port the reference's applicable substantive test
with the same number and assertions.

## License

MIT
