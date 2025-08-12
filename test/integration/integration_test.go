package integration

import (
	"context"
	"testing"
	"time"

	pb "mini-etcd/proto"
	"mini-etcd/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestIntegration_PutDeleteWatch(t *testing.T) {
	server := service.NewKVServer()

	// Set up watcher first
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)

	watchReq := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("integration-key"),
			},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Watch(watchReq, stream)
	}()

	time.Sleep(100 * time.Millisecond) // Wait for watcher setup

	// Perform PUT
	putReq := &pb.PutRequest{
		Key:   []byte("integration-key"),
		Value: []byte("integration-value"),
	}
	putResp, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), putResp.Header.Revision)

	// Verify with Range
	rangeReq := &pb.RangeRequest{
		Key: []byte("integration-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	assert.Equal(t, []byte("integration-value"), rangeResp.Kvs[0].Value)

	// Perform DELETE
	deleteReq := &pb.DeleteRequest{
		Key: []byte("integration-key"),
	}
	deleteResp, err := server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleteResp.Deleted)

	// Verify deletion with Range
	rangeResp2, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rangeResp2.Count)

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Verify watch events
	msgs := stream.getSentMessages()
	require.GreaterOrEqual(t, len(msgs), 3) // creation + put + delete

	var putEvent, deleteEvent *pb.Event
	for _, msg := range msgs {
		for _, event := range msg.Events {
			if event.Type == pb.Event_PUT {
				putEvent = event
			} else if event.Type == pb.Event_DELETE {
				deleteEvent = event
			}
		}
	}

	require.NotNil(t, putEvent)
	require.NotNil(t, deleteEvent)
	assert.Equal(t, []byte("integration-key"), putEvent.Kv.Key)
	assert.Equal(t, []byte("integration-key"), deleteEvent.Kv.Key)
}

func TestIntegration_TransactionLike(t *testing.T) {
	server := service.NewKVServer()

	// Simulate a transaction-like operation
	// Put multiple keys that form a logical unit
	keys := []string{"user:123:name", "user:123:email", "user:123:age"}
	values := []string{"John Doe", "john@example.com", "30"}

	var putRevisions []int64

	// Put all keys
	for i, key := range keys {
		req := &pb.PutRequest{
			Key:   []byte(key),
			Value: []byte(values[i]),
		}
		resp, err := server.Put(context.Background(), req)
		require.NoError(t, err)
		putRevisions = append(putRevisions, resp.Header.Revision)
	}

	// Verify all keys exist
	for i, key := range keys {
		rangeReq := &pb.RangeRequest{
			Key: []byte(key),
		}
		rangeResp, err := server.Range(context.Background(), rangeReq)
		require.NoError(t, err)
		require.Len(t, rangeResp.Kvs, 1)
		assert.Equal(t, []byte(values[i]), rangeResp.Kvs[0].Value)
	}

	// Read all user keys with prefix
	userRangeReq := &pb.RangeRequest{
		Key:      []byte("user:123:"),
		RangeEnd: []byte("user:123:z"),
	}
	userRangeResp, err := server.Range(context.Background(), userRangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(3), userRangeResp.Count)

	// Delete all user keys
	userDeleteReq := &pb.DeleteRequest{
		Key:      []byte("user:123:"),
		RangeEnd: []byte("user:123:z"),
	}
	deleteResp, err := server.Delete(context.Background(), userDeleteReq)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleteResp.Deleted)

	// Verify deletion
	userRangeResp2, err := server.Range(context.Background(), userRangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(0), userRangeResp2.Count)
}

func TestIntegration_VersionControl(t *testing.T) {
	server := service.NewKVServer()

	key := []byte("version-controlled-key")

	// Version 1
	req1 := &pb.PutRequest{
		Key:   key,
		Value: []byte("version-1"),
	}
	resp1, err := server.Put(context.Background(), req1)
	require.NoError(t, err)
	rev1 := resp1.Header.Revision

	// Version 2
	req2 := &pb.PutRequest{
		Key:   key,
		Value: []byte("version-2"),
	}
	resp2, err := server.Put(context.Background(), req2)
	require.NoError(t, err)
	rev2 := resp2.Header.Revision

	// Version 3
	req3 := &pb.PutRequest{
		Key:   key,
		Value: []byte("version-3"),
	}
	_, err = server.Put(context.Background(), req3)
	require.NoError(t, err)

	// Read current version
	rangeReq := &pb.RangeRequest{
		Key: key,
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	assert.Equal(t, []byte("version-3"), rangeResp.Kvs[0].Value)
	assert.Equal(t, int64(3), rangeResp.Kvs[0].Version)

	// Read from specific revision (simulating historical read)
	// Note: Current implementation doesn't store historical values
	// but we can verify the key existed at that revision
	rangeReq1 := &pb.RangeRequest{
		Key:      key,
		Revision: rev1,
	}
	rangeResp1, err := server.Range(context.Background(), rangeReq1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rangeResp1.Count) // Key existed at rev1

	rangeReq2 := &pb.RangeRequest{
		Key:      key,
		Revision: rev2,
	}
	rangeResp2, err := server.Range(context.Background(), rangeReq2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rangeResp2.Count) // Key existed at rev2
}

// Mock stream implementation for integration tests
type mockWatchStream struct {
	ctx      context.Context
	sentMsgs []*pb.WatchResponse
}

func newMockWatchStream(ctx context.Context) *mockWatchStream {
	return &mockWatchStream{
		ctx:      ctx,
		sentMsgs: make([]*pb.WatchResponse, 0),
	}
}

func (m *mockWatchStream) Send(resp *pb.WatchResponse) error {
	m.sentMsgs = append(m.sentMsgs, resp)
	return nil
}

func (m *mockWatchStream) Context() context.Context {
	return m.ctx
}

func (m *mockWatchStream) getSentMessages() []*pb.WatchResponse {
	return m.sentMsgs
}

// Additional interface methods for grpc.ServerStream
func (m *mockWatchStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockWatchStream) SendHeader(metadata.MD) error { return nil }
func (m *mockWatchStream) SetTrailer(metadata.MD)       {}
func (m *mockWatchStream) SendMsg(interface{}) error    { return nil }
func (m *mockWatchStream) RecvMsg(interface{}) error    { return nil }
