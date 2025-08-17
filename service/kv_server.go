package service

import (
	"context"
	"log"
	"mini-etcd/internal"
	pb "mini-etcd/proto"
	"mini-etcd/storage"
	"sync"
	"time"

	"github.com/google/btree"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServer struct {
	pb.UnimplementedKVServer
	mu              sync.RWMutex
	data            *btree.BTree
	revision        int64
	watchManager    *WatcherManager
	walManager      *storage.WALManager
	snapshotManager *storage.SnapshotManager
	walEntryCount   int64
}

type kvEntry struct {
	key            string
	val            []byte
	createRevision int64
	modRevision    int64
	version        int64
	lease          int64
}

type kvItem struct {
	key   string
	value *kvEntry
}

// Less comparator
func (item *kvItem) Less(than btree.Item) bool {
	return item.key < than.(*kvItem).key
}

func NewKVServer(dataDir ...string) *KvServer {
	dir := internal.DefaultDataDir
	if len(dataDir) > 0 && dataDir[0] != "" {
		dir = dataDir[0]
	}

	kvServer := &KvServer{
		data:            btree.New(internal.BTreeDegree),
		watchManager:    NewWatcherManager(),
		snapshotManager: storage.NewSnapshotManager(dir),
		walEntryCount:   0,
	}

	if wal, err := storage.NewWALManager(dir); err == nil {
		kvServer.walManager = wal
		if err := kvServer.recover(); err != nil {
			println("fail to recover data")
		}
	}

	return kvServer
}

func (s *KvServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//WALManager
	if s.walManager != nil {
		entry := storage.WALEntry{
			Type:     internal.OpTypePut,
			Key:      req.Key,
			Value:    req.Value,
			Revision: s.revision + 1,
		}
		if err := s.walManager.Write(&entry); err != nil {
			return nil, err
		}
	}

	//update b-tree
	s.revision++
	keyStr := string(req.Key)
	item := &kvItem{key: keyStr}
	var prevKv *pb.KeyValue
	var event *pb.Event

	if existingItem := s.data.Get(item); existingItem != nil {
		entry := existingItem.(*kvItem).value

		if req.PrevKv {
			prevKv = s.getKeyValueFromKvEntry(entry)
		}
		eventPrevKv := s.getKeyValueFromKvEntry(entry)

		entry.version++
		entry.modRevision = s.revision

		if !req.IgnoreValue {
			entry.val = append([]byte(nil), req.Value...)
		}
		if !req.IgnoreLease {
			entry.lease = req.Lease
		}
		newKv := s.getKeyValueFromKvEntry(entry)

		event = &pb.Event{
			Type:   pb.Event_PUT,
			Kv:     newKv,
			PrevKv: eventPrevKv,
		}

	} else {
		var val []byte
		var lease int64
		if !req.IgnoreValue {
			val = append([]byte(nil), req.Value...)
		}
		if !req.IgnoreLease {
			lease = req.Lease
		}
		newItem := &kvItem{
			key: keyStr,
			value: &kvEntry{
				key:            string(req.Key),
				val:            val,
				createRevision: s.revision,
				modRevision:    s.revision,
				version:        1,
				lease:          lease,
			},
		}
		s.data.ReplaceOrInsert(newItem)

		event = &pb.Event{
			Type: pb.Event_PUT,
			Kv: &pb.KeyValue{
				Key:            append([]byte(nil), req.Key...),
				Value:          val,
				CreateRevision: s.revision,
				ModRevision:    s.revision,
				Version:        1,
				Lease:          lease,
			},
			PrevKv: nil,
		}
	}

	resp := &pb.PutResponse{
		Header: &pb.ResponseHeader{
			Revision: s.revision,
		},
	}

	resp.PrevKv = prevKv

	if event != nil {
		s.watchManager.notify(event)
	}

	s.updateSnapshot()
	return resp, nil
}

func (s *KvServer) updateSnapshot() {
	s.walEntryCount++
	if s.walEntryCount >= internal.DefaultSnapshotThreshold {
		s.walEntryCount = 0
		go func() {
			if err := s.createSnapshot(); err != nil {
				log.Printf("fail to create snapshot %v", err)
			}
		}()
	}
}

func (s *KvServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	log.Printf("range request: %v", req)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var kvs []*pb.KeyValue

	//validate limit
	if req.Limit < 0 {
		return s.rangeResp(kvs), nil
	}

	//check mvcc
	if req.Revision != 0 && req.Revision > s.revision {
		log.Printf("revision out of range: %v", req.Revision)
		//here we just gracefully swallow the error and just return a response with empty data
		return s.rangeResp(kvs), nil
	}

	startKey := string(req.Key)
	item := &kvItem{key: startKey}
	if req.RangeEnd == nil {
		if existingItem := s.data.Get(item); existingItem != nil {
			entry := existingItem.(*kvItem).value
			if s.validRevision(req, entry) {
				kvs = s.appendItemToKvs(kvs, entry, req.KeysOnly)
			}
		}
	} else {
		rangeEndItem := &kvItem{key: string(req.RangeEnd)}
		s.data.AscendRange(item, rangeEndItem, func(item btree.Item) bool {
			entry := item.(*kvItem).value
			if req.Limit > 0 && int64(len(kvs)) >= req.Limit {
				return false
			}
			if s.validRevision(req, entry) {
				kvs = s.appendItemToKvs(kvs, entry, req.KeysOnly)
			}
			return true
		})
	}

	return s.rangeResp(kvs), nil
}

func (s *KvServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("delete request: %v", req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++

	//walManager
	if s.walManager != nil {
		walEntry := &storage.WALEntry{
			Type:     internal.OpTypeDelete,
			Key:      req.Key,
			Value:    nil,
			Revision: s.revision,
		}
		if err := s.walManager.Write(walEntry); err != nil {
			log.Printf("failed to write walManager entry: %v", err)
			return nil, err
		}
	}

	var deletedCount int64
	var prevKvs []*pb.KeyValue

	keyStr := string(req.Key)
	if req.RangeEnd == nil {
		item := &kvItem{key: keyStr}
		if existingItem := s.data.Get(item); existingItem != nil {
			entry := existingItem.(*kvItem).value
			if req.PrevKey {
				prevKvs = append(prevKvs, s.getKeyValueFromKvEntry(entry))
			}

			s.data.Delete(item)
			deletedCount = 1

			event := &pb.Event{
				Type: pb.Event_DELETE,
				Kv:   s.getKeyValueFromKvEntry(entry),
			}
			s.watchManager.notify(event)
		}
	} else {
		//range delete
		startItem := &kvItem{key: keyStr}
		endItem := &kvItem{key: string(req.RangeEnd)}

		var toDelete []btree.Item

		s.data.AscendRange(startItem, endItem, func(item btree.Item) bool {
			kfItem := item.(*kvItem).value
			toDelete = append(toDelete, item)
			if req.PrevKey {
				prevKvs = append(prevKvs, s.getKeyValueFromKvEntry(kfItem))
			}
			return true
		})

		for _, item := range toDelete {
			s.data.Delete(item)
			deletedCount++
			event := &pb.Event{
				Type: pb.Event_DELETE,
				Kv:   s.getKeyValueFromKvEntry(item.(*kvItem).value),
			}
			s.watchManager.notify(event)
		}
	}

	s.updateSnapshot()

	return &pb.DeleteResponse{
		Header: &pb.ResponseHeader{
			Revision: s.revision,
		},
		Deleted: deletedCount,
		PrevKvs: prevKvs,
	}, nil
}

func (s *KvServer) Watch(req *pb.WatchRequest, stream pb.KV_WatchServer) error {
	switch request := req.RequestUnion.(type) {
	case *pb.WatchRequest_CreateRequest:
		return s.handleWatchCreate(request.CreateRequest, stream)
	case *pb.WatchRequest_CancelRequest:
		return s.handleWatchCancel(request.CancelRequest, stream)
	default:
		return status.Errorf(codes.InvalidArgument, "unknown WatchRequest")
	}
}

func (s *KvServer) handleWatchCreate(req *pb.WatchCreateRequest, stream pb.KV_WatchServer) error {
	currentRevision := s.getCurrentRevision()

	startRev := req.StartRevision
	if startRev == 0 {
		startRev = currentRevision + 1
	}

	if startRev <= currentRevision {
		log.Printf("Historical revision %v is not supported.", startRev)
		startRev = currentRevision + 1
	}

	key := string(req.Key)
	wm := s.watchManager
	newWatcher := &watcher{
		id:            wm.generateWatchId(),
		key:           key,
		rangeEnd:      string(req.RangeEnd),
		startRev:      startRev,
		eventChannel:  make(chan *pb.Event, 100),
		cancelChannel: make(chan struct{}),
		prevKv:        req.PrevKv,
		stream:        stream,
	}

	createdWatcher := wm.watch(newWatcher)
	defer s.watchManager.removeWatcher(createdWatcher.id)

	if err := stream.Send(&pb.WatchResponse{
		WatchId: createdWatcher.id,
		Created: true,
		Header: &pb.ResponseHeader{
			Revision: startRev,
		},
	}); err != nil {
		return err
	}

	for {
		select {
		case event := <-createdWatcher.eventChannel:
			if err := stream.Send(&pb.WatchResponse{
				WatchId: createdWatcher.id,
				Events:  []*pb.Event{event},
				Header: &pb.ResponseHeader{
					Revision: s.getCurrentRevision(),
				},
			}); err != nil {
				return err
			}
		case <-createdWatcher.cancelChannel:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *KvServer) appendItemToKvs(kvs []*pb.KeyValue, entry *kvEntry, keysOnly bool) []*pb.KeyValue {
	var value []byte
	if !keysOnly {
		value = entry.val
	}
	kvs = append(kvs, &pb.KeyValue{
		Key:            []byte(entry.key),
		Value:          value,
		CreateRevision: entry.createRevision,
		ModRevision:    entry.modRevision,
		Version:        entry.version,
		Lease:          entry.lease,
	})
	return kvs
}

func (s *KvServer) rangeResp(kvs []*pb.KeyValue) *pb.RangeResponse {
	return &pb.RangeResponse{
		Header: &pb.ResponseHeader{
			Revision: s.getCurrentRevision(),
		},
		Kvs:   kvs,
		Count: int64(len(kvs)),
	}
}

func (s *KvServer) validRevision(req *pb.RangeRequest, entry *kvEntry) bool {
	return req.Revision == 0 || entry.createRevision <= req.Revision
}
func (s *KvServer) handleWatchCancel(request *pb.WatchCancelRequest, stream pb.KV_WatchServer) error {
	s.watchManager.removeWatcher(request.WatchId)
	return stream.Send(&pb.WatchResponse{
		WatchId:  request.WatchId,
		Canceled: true,
		Header: &pb.ResponseHeader{
			Revision: s.getCurrentRevision(),
		},
	})
}

func (s *KvServer) getCurrentRevision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

func (s *KvServer) getKeyValueFromKvEntry(entry *kvEntry) *pb.KeyValue {
	return &pb.KeyValue{
		Key:            []byte(entry.key),
		Value:          append([]byte(nil), entry.val...),
		CreateRevision: entry.createRevision,
		ModRevision:    entry.modRevision,
		Version:        entry.version,
		Lease:          entry.lease,
	}
}

func (s *KvServer) recoverFromWAL(afterRevision int64) error {
	if s.walManager == nil {
		return nil
	}
	entries, err := s.walManager.Read()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Revision <= afterRevision {
			continue
		}
		switch entry.Type {
		case internal.OpTypePut:
			item := &kvItem{
				key: string(entry.Key),
				value: &kvEntry{
					key:            string(entry.Key),
					val:            entry.Value,
					createRevision: entry.Revision,
					modRevision:    entry.Revision,
					version:        1,
					lease:          0,
				},
			}
			s.mu.Lock()
			s.data.ReplaceOrInsert(item)
			if entry.Revision > s.revision {
				s.revision = entry.Revision
			}
			s.mu.Unlock()
		case internal.OpTypeDelete:
			item := &kvItem{
				key: string(entry.Key),
			}
			s.mu.Lock()
			s.data.Delete(item)
			if entry.Revision > s.revision {
				s.revision = entry.Revision
			}
			s.mu.Unlock()
		}
	}
	return nil
}

func (s *KvServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.walManager != nil {
		return s.walManager.Close()
	}
	return nil
}

func (s *KvServer) createSnapshot() error {
	s.mu.Lock()
	var kvs []*pb.KeyValue
	s.data.Ascend(func(item btree.Item) bool {
		entry := item.(*kvItem).value
		kvs = append(kvs, &pb.KeyValue{
			Key:            []byte(entry.key),
			Value:          append([]byte(nil), entry.val...),
			CreateRevision: entry.createRevision,
			ModRevision:    entry.modRevision,
			Version:        entry.version,
			Lease:          entry.lease,
		})
		return true
	})
	currentRevision := s.revision
	s.mu.Unlock()

	snapshot := &pb.Snapshot{
		Revision:  currentRevision,
		CreatedAt: time.Now().Unix(),
		Kvs:       kvs,
	}
	if err := s.snapshotManager.CreateSnapshot(snapshot, currentRevision); err != nil {
		return err
	}
	return nil
}

func (s *KvServer) loadSnapshot() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot, err := s.snapshotManager.LoadLatestSnapshot()
	if err != nil {
		return 0, err
	}

	if snapshot == nil {
		return 0, nil
	}
	//load the snapshot to b-tree
	s.data = btree.New(internal.BTreeDegree)
	s.revision = snapshot.Revision

	for _, kv := range snapshot.Kvs {
		item := &kvItem{
			key: string(kv.Key),
			value: &kvEntry{
				key:            string(kv.Key),
				val:            kv.Value,
				createRevision: kv.CreateRevision,
				modRevision:    kv.ModRevision,
				version:        kv.Version,
				lease:          kv.Lease,
			},
		}
		s.data.ReplaceOrInsert(item)
	}
	return snapshot.Revision, nil
}

func (s *KvServer) recover() error {
	snapshotRevision, err := s.loadSnapshot()
	if err != nil {
		return err
	}
	if err := s.recoverFromWAL(snapshotRevision); err != nil {
		return err
	}
	return nil
}
