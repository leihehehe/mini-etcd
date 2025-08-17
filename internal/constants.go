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

const (
	SnapshotFilePrefix = "snapshot"
	SnapshotFileExt    = ".snap"
	// DefaultSnapshotThreshold create snapshot when WAL entries exceed this threshold
	DefaultSnapshotThreshold = 10
)
