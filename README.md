# MapStore for Go

MapStore is a local, filesystem‑backed map database with pluggable codecs (JSON or custom), with optional per‑key encryption via the OS keyring, and optional full‑text search via SQLite FTS5.

## Features

- File store

  - It keeps a `map[string]any` in sync with files on disk, the file can be encoded as JSON (inbuilt), or any format using a custom file encoder/decoder.
  - It is a thread-safe map store with atomic file writes and optimistic concurrency.
  - Pluggable codecs for both keys and values inside the map, including an encrypted string encoder backed by `github.com/zalando/go-keyring`.
  - Listener hooks so callers can observe every mutation written to disk.
  - Optional SQLite FTS5 integration for fast search, with helpers for incremental sync.

- Directory store: A convenience manager that partitions data across subdirectories and paginates listings.

- Pure Go implementation with no cgo, compatible with Go 1.25+.

## Capabilities and Extensibility

- **File encoders**
  - _JSON file encode/decode_ - use the inbuilt `encdecjson.JSONEncoderDecoder` to encode/decode files as JSON.
  - Supply your own `IOEncoderDecoder` via `WithFileEncoderDecoder`.
- **Encode key or value at sub-path**
  - _Value encryption_ - use the inbuilt `encdeckeyring.EncryptedStringValueEncoderDecoder` to transparently store sensitive string values through the OS keyring.
  - Override encoding of specific keys or values with `WithKeyEncDecGetter` or `WithValueEncDecGetter`.
- **Partitioning**
  - _Month based partitioning_ - use the inbuilt `dirpartition.MonthPartitionProvider` to split files across month based directories.
  - Swap in your own `PartitionProvider` to control directory layout.
- **File change events**
  - Custom listeners can be plugged into `filestore` to observe file events.
  - Pluggable _Full text search_
    - Inbuilt, pure go, sqlite backed (via [glebarez driver](https://github.com/glebarez/go-sqlite) + [modernc sqlite](https://pkg.go.dev/modernc.org/sqlite)), fts engine.
    - Pluggable iterator utility `ftsengine.SyncIterToFTS` for efficient, incremental index updates.
- **File naming** - implement `filenameprovider.Provider` or use the provided UUIDv7-based default to keep file names collision-free.

## Installation

```bash
go get github.com/ppipada/mapstore-go
```

## Quick Start

<details>
<summary>Single file store</summary>

```go
package main

import (
"fmt"
"log"

    "github.com/ppipada/mapstore-go/filestore"

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

  "github.com/ppipada/mapstore-go/dirstore"
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

  "github.com/ppipada/mapstore-go/ftsengine"
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
    "title": "MapStore introduction",
    "body":  "MapStore keeps JSON maps on disk with optional full text search.",
  }); err != nil {
    log.Fatal(err)
  }

  hits, _, err := engine.Search(ctx, "MapStore search", "", 5)
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

MapStore is released under the [MIT License](LICENSE).
