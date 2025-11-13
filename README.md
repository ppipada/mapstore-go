# MapDB for Go

MapDB is a local, filesystem‑backed map database with pluggable codecs (JSON or custom), with optional per‑key encryption via the OS keyring, and optional full‑text search via SQLite FTS5.

## Features

- File store

  - It keeps a `map[string]any` in sync with files on disk, the file can be encoded as JSON (inbuilt), or any format using a custom file encoder/decoder.
  - It is a thread-safe map store with atomic file writes and optimistic concurrency.
  - Pluggable codecs for both keys and values inside the map, including an encrypted string encoder backed by `github.com/zalando/go-keyring`.
  - Listener hooks so callers can observe every mutation written to disk.
  - Optional SQLite FTS5 integration for fast search, with helpers for incremental sync.

- Directory store: A convenience manager that partitions data across subdirectories and paginates listings.

- Pure Go implementation with no cgo, compatible with Go 1.25+.

## Extensibility Highlights

- **Custom encoders** - supply your own `encdec.EncoderDecoder` via `filestore.WithEncoderDecoder`, or override specific keys with `WithValueEncDecGetter`.
- **Per-key encryption** - use `encdec.EncryptedStringValueEncoderDecoder` to transparently store sensitive strings through the OS keyring.
- **Partitioning** - swap in your own `dirstore.PartitionProvider` to control directory layout.
- **File naming** - implement `filenameprovider.Provider` or use the provided UUIDv7-based default to keep file names collision-free.
- **Full text sync** - plug custom iterators into `ftsengine.SyncIterToFTS` for efficient, incremental index updates.

## Installation

```bash
go get github.com/ppipada/mapdb-go
```

## Quick Start

<details>
<summary>Single file store</summary>

```go
package main

import (
"fmt"
"log"

    "github.com/ppipada/mapdb-go/filestore"

)

func main() {
store, err := filestore.NewMapFileStore(
"config.json",
map[string]any{"env": "dev"},
filestore.WithCreateIfNotExists(true),
)
if err != nil {
log.Fatal(err)
}
defer store.Close()

    if err := store.SetKey([]string{"features", "logging"}, true); err != nil {
      log.Fatal(err)
    }

    data, err := store.GetAll(false)
    if err != nil {
      log.Fatal(err)
    }

    fmt.Println(data["features"]) // map[logging:true]

}

```

</details>

<details>
<summary>Managing files inside a directory</summary>

```go
package main

import (
  "log"
  "time"

  "github.com/ppipada/mapdb-go/dirstore"
)

func main() {
  mds, err := dirstore.NewMapDirectoryStore(
    "./data",
    true,
    dirstore.WithPartitionProvider(&dirstore.MonthPartitionProvider{
      TimeFn: func(key dirstore.FileKey) (time.Time, error) {
        return time.Now(), nil
      },
    }),
  )
  if err != nil {
    log.Fatal(err)
  }
  defer mds.CloseAll()

  fileKey := dirstore.FileKey{FileName: "profile.json"}
  if err := mds.SetFileData(fileKey, map[string]any{"name": "Ada"}); err != nil {
    log.Fatal(err)
  }
}
```

</details>

<details>
<summary>Full-Text Search Engine</summary>

```go
package main

import (
  "context"
  "fmt"
  "log"

  "github.com/ppipada/mapdb-go/ftsengine"
)

func main() {
  engine, err := ftsengine.NewEngine(ftsengine.Config{
    BaseDir:    ftsengine.MemoryDBBaseDir,
    DBFileName: "",
    Table:      "docs",
    Columns: []ftsengine.Column{
      {Name: "title"},
      {Name: "body"},
    },
  })
  if err != nil {
    log.Fatal(err)
  }
  defer engine.Close()

  ctx := context.Background()
  if err := engine.Upsert(ctx, "doc-1", map[string]string{
    "title": "MapDB introduction",
    "body":  "MapDB keeps JSON maps on disk with optional full text search.",
  }); err != nil {
    log.Fatal(err)
  }

  hits, _, err := engine.Search(ctx, "MapDB search", "", 5)
  if err != nil {
    log.Fatal(err)
  }
  for _, hit := range hits {
    fmt.Println(hit.ID, hit.Score)
  }
}
```

</details>

## Development

- Formatting follows `gofumpt` and `golines` via `golangci-lint`, which is also used for linting. All rules are in [.golangci.yml](.golangci.yml).

- Useful scripts are defined in `taskfile.yml` (requires [Task](https://taskfile.dev/)):

  - `task lint` - run `golangci-lint`.
  - `task test` - run `go test ./...`.
  - `task lt` - lint then test.

## License

MapDB is released under the [MIT License](LICENSE).
