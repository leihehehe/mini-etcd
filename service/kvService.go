package service

import (
	"context"
	"log"
	pb "mini-etcd/proto"
	"sync"

	"github.com/google/btree"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServer struct {
	pb.UnimplementedKVServer
	mu           sync.RWMutex
	data         *btree.BTree
	revision     int64
	watchManager *WatcherManager
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

func NewKVServer() *KvServer {
	kvServer := &KvServer{
		data: btree.New(64),
	}
	kvServer.watchManager = NewWatcherManager()
	return kvServer
}

func (s *KvServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("put request: %v", req)
	s.mu.Lock()
	defer s.mu.Unlock()
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
		newItem := &kvItem{
			key: keyStr,
			value: &kvEntry{
				key:            string(req.Key),
				val:            append([]byte(nil), req.Value...),
				createRevision: s.revision,
				modRevision:    s.revision,
				version:        1,
				lease:          req.Lease,
			},
		}
		s.data.ReplaceOrInsert(newItem)

		event = &pb.Event{
			Type: pb.Event_PUT,
			Kv: &pb.KeyValue{
				Key:            append([]byte(nil), req.Key...),
				Value:          append([]byte(nil), req.Value...),
				CreateRevision: s.revision,
				ModRevision:    s.revision,
				Version:        1,
				Lease:          req.Lease,
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

	return resp, nil
}

func (s *KvServer) Range(context context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	log.Printf("range reqeust: %v", req)
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
