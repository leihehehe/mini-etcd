package internal

const (
	DefaultPort    = 2379
	DefaultDataDir = "data"
	WALFileName    = "wal.log"

	BTreeDegree = 64
)

const (
	OpTypePut    = "PUT"
	OpTypeDelete = "DELETE"
	OpTypeRange  = "RANGE"
)
