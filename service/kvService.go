package service

import (
	"context"
	"log"
	pb "mini-etcd/proto"
	"sync"
)

type KvServer struct {
	pb.UnimplementedKVServer
	mu       sync.RWMutex
	data     map[string]*kvEntry
	revision int64
}

type kvEntry struct {
	key            []byte
	val            []byte
	createRevision int64
	modRevision    int64
	version        int64
	lease          int64
}

func NewKVServer() *KvServer {
	return &KvServer{
		data: make(map[string]*kvEntry),
	}
}

// Put TODO: change map to B-tree
func (s *KvServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("put request: %v", req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++
	keyStr := string(req.Key)
	var prevKv *pb.KeyValue
	if entry, exist := s.data[keyStr]; exist {
		if req.PrevKv {
			prevKv = &pb.KeyValue{
				Key:            entry.key,
				Value:          entry.val,
				CreateRevision: entry.createRevision,
				ModRevision:    entry.modRevision,
				Version:        entry.version,
				Lease:          entry.lease,
			}
		}

		entry.version++
		entry.modRevision = s.revision

		if !req.IgnoreValue {
			entry.val = append([]byte(nil), req.Value...)
		}
		if !req.IgnoreLease {
			entry.lease = req.Lease
		}
	} else {
		s.data[keyStr] = &kvEntry{
			key:            append([]byte(nil), req.Key...),
			val:            append([]byte(nil), req.Value...),
			createRevision: s.revision,
			modRevision:    s.revision,
			version:        1,
			lease:          req.Lease,
		}
	}

	resp := &pb.PutResponse{
		Header: &pb.ResponseHeader{
			Revision: s.revision,
		},
	}

	resp.PrevKv = prevKv

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
	if req.RangeEnd == nil {
		if entry, exist := s.data[startKey]; exist && s.validRevision(req, entry) {
			kvs = s.appendEntryToKvs(kvs, entry, req.KeysOnly)
		}
	} else {
		rangeEndStr := string(req.RangeEnd)
		for k, entry := range s.data {
			if req.Limit > 0 && int64(len(kvs)) >= req.Limit {
				break
			}
			if k >= startKey && k < rangeEndStr && s.validRevision(req, entry) {
				kvs = s.appendEntryToKvs(kvs, entry, req.KeysOnly)
			}
		}
	}

	return s.rangeResp(kvs), nil
}

func (s *KvServer) appendEntryToKvs(kvs []*pb.KeyValue, entry *kvEntry, keysOnly bool) []*pb.KeyValue {
	var value []byte
	if !keysOnly {
		value = entry.val
	}
	kvs = append(kvs, &pb.KeyValue{
		Key:            entry.key,
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
			Revision: s.revision,
		},
		Kvs:   kvs,
		Count: int64(len(kvs)),
	}
}

func (s *KvServer) validRevision(req *pb.RangeRequest, entry *kvEntry) bool {
	return req.Revision == 0 || entry.createRevision <= req.Revision
}
