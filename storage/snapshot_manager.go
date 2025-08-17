package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"mini-etcd/internal"
	pb "mini-etcd/proto"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type SnapshotManager struct {
	mu      sync.Mutex
	dataDir string
}

func NewSnapshotManager(dataDir string) *SnapshotManager {
	return &SnapshotManager{
		dataDir: dataDir,
	}
}

func (sm *SnapshotManager) CreateSnapshot(snapshot *pb.Snapshot, curRevision int64) error {
	data, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s-%d-%d%s",
		internal.SnapshotFilePrefix, curRevision, time.Now().Unix(), internal.SnapshotFileExt)
	filePath := filepath.Join(sm.dataDir, filename)

	if err := os.MkdirAll(sm.dataDir, 0755); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	length := uint32(len(data))
	if err := binary.Write(file, binary.BigEndian, length); err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	log.Printf("Snapshot created: %s (revision=%d, keys=%d)",
		filename, curRevision, len(snapshot.Kvs))
	return nil
}

// LoadLatestSnapshot load the latest snapshot and return its revision
func (sm *SnapshotManager) LoadLatestSnapshot() (*pb.Snapshot, error) {
	files, err := os.ReadDir(sm.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	type snapshotFile struct {
		name     string
		revision int64
	}

	// find the latest snapshot
	var snapshots []snapshotFile
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if !strings.HasPrefix(name, internal.SnapshotFilePrefix) || !strings.HasSuffix(name, internal.SnapshotFileExt) {
			continue
		}
		var revision, timestamp int64
		if n, err := fmt.Sscanf(
			name,
			internal.SnapshotFilePrefix+"-%d-%d"+internal.SnapshotFileExt,
			&revision, &timestamp,
		); err == nil && n == 2 {
			snapshots = append(snapshots, snapshotFile{name: name, revision: revision})
		}
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].revision < snapshots[j].revision
	})
	latest := snapshots[len(snapshots)-1]

	//load the latest snapshot
	filepath := filepath.Join(sm.dataDir, latest.name)
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var length uint32
	if err := binary.Read(file, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}

	var snapshot pb.Snapshot
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}

	return &snapshot, nil
}
