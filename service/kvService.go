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

func (s *KvServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("put: %v", req)
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
