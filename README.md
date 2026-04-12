# Cap URN - Go Implementation

Go implementation of Cap URN (Capability Uniform Resource Names), built on [Tagged URN](https://github.com/machinefabric/tagged-urn-go).

## Features

- **Required Direction Specifiers** - `in`/`out` tags for input/output media types
- **Media URN Validation** - Validates direction spec values are valid Media URNs
- **Special Pattern Values** - `*` (must-have-any), `?` (unspecified), `!` (must-not-have)
- **Graded Specificity** - Exact values score higher than wildcards
- **Cap Definitions** - Full capability definitions with arguments, output, and metadata
- **Cap Matrix** - Registry for capability lookup and matching
- **Cap Caller** - Fluent API for invoking capabilities
- **Schema Validation** - JSON Schema validation for arguments and outputs

## Installation

```bash
go get github.com/machinefabric/capdag-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/machinefabric/capdag-go"
)

func main() {
    // Parse a Cap URN
    cap, err := capdag.NewCapUrnFromString(`cap:in="media:binary";op=extract;out="media:object"`)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Input:", cap.GetInSpec())   // "media:binary"
    fmt.Println("Output:", cap.GetOutSpec()) // "media:object"
    fmt.Println("Op:", cap.GetTag("op"))     // "extract"

    // Build a Cap URN
    built := capdag.NewCapUrnBuilder().
        InSpec("media:void").
        OutSpec("media:object").
        Tag("op", "generate").
        Tag("target", "thumbnail").
        MustBuild()

    // Check matching
    pattern, _ := capdag.NewCapUrnFromString(`cap:in="media:binary";op=extract;out="media:object"`)
    if cap.Accepts(pattern) {
        fmt.Println("Cap matches pattern")
    }

    // Get specificity (graded scoring)
    fmt.Println("Specificity:", cap.Specificity())
}
```

## Cap Definitions

```go
// Create a full capability definition
capDef := &capdag.Cap{
    Urn:   cap,
    Title: "PDF Text Extractor",
    Args: []capdag.CapArg{
        {Name: "pages", Type: "string", Description: "Page range (e.g., '1-5')"},
    },
    Output: &capdag.CapOutput{
        Type:        "text",
        Description: "Extracted text content",
    },
}
```

## Cap Matrix (Registry)

```go
// Create a capability registry
matrix := capdag.NewCapMatrix()

// Register a capability with its handler
matrix.RegisterCapSet("my-cartridge", myHandler, []*capdag.Cap{capDef})

// Find matching capabilities
caps, err := matrix.FindCapSets(`cap:in="media:binary";op=extract;out=*`)

// Find the best match by specificity
host, cap, err := matrix.FindBestCapSet(requestUrn)
```

## API Reference

### CapUrn

| Function/Method | Description |
|-----------------|-------------|
| `NewCapUrnFromString(s)` | Parse Cap URN from string |
| `NewCapUrnFromTags(tags)` | Create from tag map (must include in/out) |
| `GetInSpec()` | Get input media URN |
| `GetOutSpec()` | Get output media URN |
| `GetTag(key)` | Get value for a tag key |
| `WithTag(key, value)` | Return new CapUrn with tag added/updated |
| `WithInSpec(spec)` | Return new CapUrn with changed input spec |
| `WithOutSpec(spec)` | Return new CapUrn with changed output spec |
| `Accepts(request)` | Check if Cap (as pattern) accepts a request |
| `ConformsTo(pattern)` | Check if Cap conforms to a pattern |
| `Specificity()` | Get graded specificity score |
| `ToString()` | Get canonical string representation |

### CapUrnBuilder

| Method | Description |
|--------|-------------|
| `NewCapUrnBuilder()` | Create a new builder |
| `InSpec(spec)` | Set input media URN (required) |
| `OutSpec(spec)` | Set output media URN (required) |
| `Tag(key, value)` | Add or update a tag (chainable) |
| `Build()` | Build the CapUrn (returns error if invalid) |
| `MustBuild()` | Build the CapUrn (panics if invalid) |

## Matching Semantics

| Pattern | Instance Missing | Instance=v | Instance=x (x≠v) |
|---------|------------------|------------|------------------|
| (missing) or `?` | Match | Match | Match |
| `K=!` | Match | No Match | No Match |
| `K=*` | No Match | Match | Match |
| `K=v` | No Match | Match | No Match |

## Graded Specificity

| Value Type | Score |
|------------|-------|
| Exact value (`K=v`) | 3 |
| Must-have-any (`K=*`) | 2 |
| Must-not-have (`K=!`) | 1 |
| Unspecified (`K=?`) or missing | 0 |

## Error Codes

| Code | Constant | Description |
|------|----------|-------------|
| 10 | `ErrorMissingInSpec` | Missing required `in` tag |
| 11 | `ErrorMissingOutSpec` | Missing required `out` tag |
| 12 | `ErrorInvalidMediaUrn` | Invalid Media URN in direction spec |

For base Tagged URN error codes, see [Tagged URN documentation](https://github.com/machinefabric/tagged-urn-go).

## Testing

```bash
go test -v ./...
```

## Cross-Language Compatibility

This Go implementation produces identical results to:
- [Rust reference implementation](https://github.com/machinefabric/capdag)
- [JavaScript implementation](https://github.com/machinefabric/capdag-js)
- [Objective-C implementation](https://github.com/machinefabric/capdag-objc)

All implementations follow the same rules. See:
- [Cap URN RULES.md](https://github.com/machinefabric/capdag/blob/main/docs/RULES.md) - Cap-specific rules
- [Tagged URN RULES.md](https://github.com/machinefabric/tagged-urn-rs/blob/main/docs/RULES.md) - Base format rules
