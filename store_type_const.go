package mapdb

import (
	"errors"
	"time"

	"github.com/ppipada/mapdb-go/encdec"
)

// ErrConflict is returned when flush/delete detects that somebody modified the file since we last read or wrote it.
var (
	ErrConflict            = errors.New("filestore: concurrent modification detected")
	ErrCannotReadPartition = errors.New("failed to read partition directory")
)

const (
	SortOrderAscending  = "asc"
	SortOrderDescending = "desc"
)

// Operation is the kind of mutation that happened on a file or a key.
type Operation string

const (
	OpSetFile    Operation = "setFile"
	OpResetFile  Operation = "resetFile"
	OpDeleteFile Operation = "deleteFile"
	OpSetKey     Operation = "setKey"
	OpDeleteKey  Operation = "deleteKey"
)

// FileEvent is delivered *after* a mutation has been written to disk.
type FileEvent struct {
	Op Operation
	// Absolute path of the backing JSON file.
	File string
	// Nil for file-level ops.
	Keys []string
	// Nil for OpSetFile / OpResetFile.
	OldValue any
	// Nil for delete.
	NewValue any
	// Deep-copy of the entire map after the change.
	Data      map[string]any
	Timestamp time.Time
}

// FileListener is a callback that observes mutations.
type FileListener func(FileEvent)

// KeyEncDecGetter: given the path so far, if applicable, returns a StringEncoderDecoder
// It encodes decodes: The key at the path i.e last part of the path array.
type KeyEncDecGetter func(pathSoFar []string) encdec.StringEncoderDecoder

// ValueEncDecGetter: given the path so far, if applicable, returns a EncoderDecoder
// It encodes decodes: Value at the key i.e value at last part of the path array.
type ValueEncDecGetter func(pathSoFar []string) encdec.EncoderDecoder

type FileKey struct {
	FileName string
	XAttr    any
}

// PartitionProvider defines an interface for determining the partition directory for a file.
type PartitionProvider interface {
	GetPartitionDir(key FileKey) (string, error)
	ListPartitions(baseDir, sortOrder, pageToken string,
		pageSize int) ([]string, string, error)
}
