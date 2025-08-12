package unit

import (
	"context"
	pb "mini-etcd/proto"
	"mini-etcd/service"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Mock stream for testing Watch functionality
type mockWatchStream struct {
	grpc.ServerStream
	ctx      context.Context
	sentMsgs []*pb.WatchResponse
	mu       sync.Mutex
}

func newMockWatchStream(ctx context.Context) *mockWatchStream {
	return &mockWatchStream{
		ctx:      ctx,
		sentMsgs: make([]*pb.WatchResponse, 0),
	}
}

func (m *mockWatchStream) Send(resp *pb.WatchResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMsgs = append(m.sentMsgs, resp)
	return nil
}

func (m *mockWatchStream) Context() context.Context {
	return m.ctx
}

func (m *mockWatchStream) getSentMessages() []*pb.WatchResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*pb.WatchResponse, len(m.sentMsgs))
	copy(result, m.sentMsgs)
	return result
}

func TestWatch_CreateWatcher(t *testing.T) {
	server := service.NewKVServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)

	req := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("test-key"),
			},
		},
	}

	// Run watch in goroutine since it blocks
	done := make(chan error, 1)
	go func() {
		done <- server.Watch(req, stream)
	}()

	// Give some time for watcher to be created
	time.Sleep(100 * time.Millisecond)
	cancel() // Cancel to stop the watch

	err := <-done
	assert.Error(t, err) // Should get context canceled error

	// Check that creation response was sent
	msgs := stream.getSentMessages()
	require.Len(t, msgs, 1)
	assert.True(t, msgs[0].Created)
	assert.NotEmpty(t, msgs[0].WatchId)
}

func TestWatch_SingleKeyEvents(t *testing.T) {
	server := service.NewKVServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)

	req := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("test-key"),
			},
		},
	}

	// Start watching
	done := make(chan error, 1)
	go func() {
		done <- server.Watch(req, stream)
	}()

	// Wait for watcher to be set up
	time.Sleep(100 * time.Millisecond)

	// Perform PUT operation
	putReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	// Give time for event to be processed
	time.Sleep(100 * time.Millisecond)

	// Perform DELETE operation
	deleteReq := &pb.DeleteRequest{
		Key: []byte("test-key"),
	}
	_, err = server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	cancel()

	<-done

	// Check messages
	msgs := stream.getSentMessages()
	require.GreaterOrEqual(t, len(msgs), 3) // creation + put + delete

	// First message should be creation
	assert.True(t, msgs[0].Created)

	// Find PUT and DELETE events
	var putEvent, deleteEvent *pb.Event
	for i := 1; i < len(msgs); i++ {
		if len(msgs[i].Events) > 0 {
			event := msgs[i].Events[0]
			if event.Type == pb.Event_PUT {
				putEvent = event
			} else if event.Type == pb.Event_DELETE {
				deleteEvent = event
			}
		}
	}

	require.NotNil(t, putEvent)
	assert.Equal(t, pb.Event_PUT, putEvent.Type)
	assert.Equal(t, []byte("test-key"), putEvent.Kv.Key)
	assert.Equal(t, []byte("test-value"), putEvent.Kv.Value)

	require.NotNil(t, deleteEvent)
	assert.Equal(t, pb.Event_DELETE, deleteEvent.Type)
	assert.Equal(t, []byte("test-key"), deleteEvent.Kv.Key)
}

func TestWatch_PrefixWatchEvents(t *testing.T) {
	server := service.NewKVServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)

	req := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:      []byte("config/"),
				RangeEnd: []byte("config/z"),
			},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Watch(req, stream)
	}()

	time.Sleep(100 * time.Millisecond)

	// Put keys in watched range
	keys := []string{"config/app1", "config/app2", "other/key"}
	for _, key := range keys {
		putReq := &pb.PutRequest{
			Key:   []byte(key),
			Value: []byte("value"),
		}
		_, err := server.Put(context.Background(), putReq)
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	<-done

	msgs := stream.getSentMessages()

	// Should have creation message + 2 PUT events (config/app1, config/app2)
	// "other/key" should not trigger an event
	eventCount := 0
	for _, msg := range msgs {
		eventCount += len(msg.Events)
	}
	assert.Equal(t, 2, eventCount) // Only config/app1 and config/app2
}

func TestWatch_WithPrevKv(t *testing.T) {
	server := service.NewKVServer()

	// First, put a key
	putReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("initial-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)

	req := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:    []byte("test-key"),
				PrevKv: true,
			},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Watch(req, stream)
	}()

	time.Sleep(100 * time.Millisecond)

	// Update the key
	updateReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("updated-value"),
	}
	_, err = server.Put(context.Background(), updateReq)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	msgs := stream.getSentMessages()

	// Find the PUT event
	var putEvent *pb.Event
	for _, msg := range msgs {
		for _, event := range msg.Events {
			if event.Type == pb.Event_PUT {
				putEvent = event
				break
			}
		}
	}

	require.NotNil(t, putEvent)
	require.NotNil(t, putEvent.PrevKv)
	assert.Equal(t, []byte("initial-value"), putEvent.PrevKv.Value)
	assert.Equal(t, []byte("updated-value"), putEvent.Kv.Value)
}

func TestWatch_CancelWatcher(t *testing.T) {
	server := service.NewKVServer()
	ctx := context.Background()

	stream := newMockWatchStream(ctx)

	// Create watcher
	createReq := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("test-key"),
			},
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- server.Watch(createReq, stream)
	}()

	time.Sleep(100 * time.Millisecond)

	// Get the watch ID from creation response
	msgs := stream.getSentMessages()
	require.Len(t, msgs, 1)
	watchId := msgs[0].WatchId

	// Cancel the watcher
	cancelReq := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CancelRequest{
			CancelRequest: &pb.WatchCancelRequest{
				WatchId: watchId,
			},
		},
	}

	err := server.Watch(cancelReq, stream)
	require.NoError(t, err)

	// Check that cancel response was sent
	msgs = stream.getSentMessages()
	require.Len(t, msgs, 2)
	assert.True(t, msgs[1].Canceled)
	assert.Equal(t, watchId, msgs[1].WatchId)
}

func TestWatch_MultipleWatchers(t *testing.T) {
	server := service.NewKVServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream1 := newMockWatchStream(ctx)
	stream2 := newMockWatchStream(ctx)

	// Create two watchers for the same key
	req := &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("shared-key"),
			},
		},
	}

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)

	go func() {
		done1 <- server.Watch(req, stream1)
	}()

	go func() {
		done2 <- server.Watch(req, stream2)
	}()

	time.Sleep(100 * time.Millisecond)

	// Put a value
	putReq := &pb.PutRequest{
		Key:   []byte("shared-key"),
		Value: []byte("shared-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	cancel()

	<-done1
	<-done2

	// Both streams should have received the event
	msgs1 := stream1.getSentMessages()
	msgs2 := stream2.getSentMessages()

	// Each should have creation + put event
	assert.GreaterOrEqual(t, len(msgs1), 2)
	assert.GreaterOrEqual(t, len(msgs2), 2)

	// Check both received PUT events
	hasEvent1 := false
	hasEvent2 := false

	for _, msg := range msgs1 {
		for _, event := range msg.Events {
			if event.Type == pb.Event_PUT {
				hasEvent1 = true
			}
		}
	}

	for _, msg := range msgs2 {
		for _, event := range msg.Events {
			if event.Type == pb.Event_PUT {
				hasEvent2 = true
			}
		}
	}

	assert.True(t, hasEvent1)
	assert.True(t, hasEvent2)
}
