package unit

import (
	"context"
	"fmt"
	pb "mini-etcd/proto"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPut_WithoutPrevKV(t *testing.T) {
	server := NewTestKvServer(t)
	req := &pb.PutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	resp, err := server.Put(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Header)
	assert.Equal(t, int64(1), resp.Header.Revision)
	assert.Nil(t, resp.PrevKv)
}

func TestPut_WithPrevKV_WhenNewKey(t *testing.T) {
	server := NewTestKvServer(t)
	req := &pb.PutRequest{
		Key:    []byte("key"),
		Value:  []byte("value"),
		PrevKv: true,
	}

	resp, err := server.Put(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Header)
	assert.Equal(t, int64(1), resp.Header.Revision)
	assert.Nil(t, resp.PrevKv)
}

func TestPut_WithPrevKV_WhenExistingKey(t *testing.T) {
	server := NewTestKvServer(t)

	//first request
	req := &pb.PutRequest{
		Key:    []byte("key"),
		Value:  []byte("value"),
		PrevKv: true,
	}
	resp, err := server.Put(context.Background(), req)

	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Header)
	assert.Equal(t, int64(1), resp.Header.Revision)
	assert.Nil(t, resp.PrevKv)

	//second request
	req2 := &pb.PutRequest{
		Key:    []byte("key"),
		Value:  []byte("value2"),
		PrevKv: true,
	}
	resp2, err := server.Put(context.Background(), req2)

	require.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.NotNil(t, resp2.Header)
	assert.Equal(t, int64(2), resp2.Header.Revision)
	assert.Equal(t, resp2.PrevKv.Key, []byte("key"))
	assert.Equal(t, resp2.PrevKv.Value, []byte("value"))
}
func TestPut_IgnoreValue(t *testing.T) {
	server := NewTestKvServer(t)

	req1 := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("initial-value"),
	}
	resp1, err := server.Put(context.Background(), req1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp1.Header.Revision)

	req2 := &pb.PutRequest{
		Key:         []byte("test-key"),
		Value:       []byte("new-value"), // This will be ignored
		IgnoreValue: true,
	}
	resp2, err := server.Put(context.Background(), req2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp2.Header.Revision)

	// Check that value was NOT updated, but version was incremented
	rangeReq := &pb.RangeRequest{
		Key: []byte("test-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)

	// Value should remain the original one
	assert.Equal(t, []byte("initial-value"), rangeResp.Kvs[0].Value)
	// But version should be incremented (this is like "touching" the key)
	assert.Equal(t, int64(2), rangeResp.Kvs[0].Version)
	assert.Equal(t, int64(2), rangeResp.Kvs[0].ModRevision)
}

func TestPut_IgnoreValue_NewKey(t *testing.T) {
	server := NewTestKvServer(t)

	req := &pb.PutRequest{
		Key:         []byte("new-key"),
		Value:       []byte("some-value"),
		IgnoreValue: true,
	}

	_, err := server.Put(context.Background(), req)
	require.NoError(t, err)

	rangeReq := &pb.RangeRequest{
		Key: []byte("new-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)

	assert.Equal(t, []byte("new-key"), rangeResp.Kvs[0].Key)
	assert.Empty(t, rangeResp.Kvs[0].Value)
}

func TestPut_IgnoreLease(t *testing.T) {
	server := NewTestKvServer(t)

	// First, put a key with initial lease
	req1 := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("value"),
		Lease: 123,
	}
	_, err := server.Put(context.Background(), req1)
	require.NoError(t, err)

	// Update with IgnoreLease = true and different lease
	// This should NOT update the lease, but should update the value
	req2 := &pb.PutRequest{
		Key:         []byte("test-key"),
		Value:       []byte("new-value"),
		Lease:       456, // This will be ignored
		IgnoreLease: true,
	}
	_, err = server.Put(context.Background(), req2)
	require.NoError(t, err)

	// Check that lease was NOT updated, but value was updated
	rangeReq := &pb.RangeRequest{
		Key: []byte("test-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)

	// Lease should remain the original one
	assert.Equal(t, int64(123), rangeResp.Kvs[0].Lease)
	// But value should be updated
	assert.Equal(t, []byte("new-value"), rangeResp.Kvs[0].Value)
	assert.Equal(t, int64(2), rangeResp.Kvs[0].Version)
}

func TestPut_IgnoreBoth(t *testing.T) {
	server := NewTestKvServer(t)

	// First, put a key with initial values
	req1 := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("initial-value"),
		Lease: 123,
	}
	_, err := server.Put(context.Background(), req1)
	require.NoError(t, err)

	// Update with both IgnoreValue and IgnoreLease = true
	// This essentially just "touches" the key, updating only metadata
	req2 := &pb.PutRequest{
		Key:         []byte("test-key"),
		Value:       []byte("new-value"), // Ignored
		Lease:       456,                 // Ignored
		IgnoreValue: true,
		IgnoreLease: true,
	}
	_, err = server.Put(context.Background(), req2)
	require.NoError(t, err)

	// Check that neither value nor lease was updated
	rangeReq := &pb.RangeRequest{
		Key: []byte("test-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)

	// Both value and lease should remain original
	assert.Equal(t, []byte("initial-value"), rangeResp.Kvs[0].Value)
	assert.Equal(t, int64(123), rangeResp.Kvs[0].Lease)
	// But version should be incremented (key was "touched")
	assert.Equal(t, int64(2), rangeResp.Kvs[0].Version)
	assert.Equal(t, int64(2), rangeResp.Kvs[0].ModRevision)
}

func TestPut_WithLease(t *testing.T) {
	server := NewTestKvServer(t)

	req := &pb.PutRequest{
		Key:   []byte("lease-key"),
		Value: []byte("lease-value"),
		Lease: 789,
	}
	_, err := server.Put(context.Background(), req)
	require.NoError(t, err)

	// Verify lease was set
	rangeReq := &pb.RangeRequest{
		Key: []byte("lease-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	assert.Equal(t, int64(789), rangeResp.Kvs[0].Lease)
}

func TestPut_ConcurrentWrites(t *testing.T) {
	server := NewTestKvServer(t)
	numGoroutines := 10
	numOperations := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writes to different keys
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", goroutineID, j))
				value := []byte(fmt.Sprintf("value-%d-%d", goroutineID, j))

				req := &pb.PutRequest{
					Key:   key,
					Value: value,
				}
				_, err := server.Put(context.Background(), req)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys were written
	totalKeys := numGoroutines * numOperations
	rangeReq := &pb.RangeRequest{
		Key:      []byte("key-"),
		RangeEnd: []byte("key-z"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(totalKeys), rangeResp.Count)
}

func TestPut_ConcurrentWritesToSameKey(t *testing.T) {
	server := NewTestKvServer(t)
	numGoroutines := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writes to the same key
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			value := []byte(fmt.Sprintf("value-%d", goroutineID))

			req := &pb.PutRequest{
				Key:   []byte("shared-key"),
				Value: value,
			}
			_, err := server.Put(context.Background(), req)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify the key exists and has one of the values
	rangeReq := &pb.RangeRequest{
		Key: []byte("shared-key"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)

	// Version should be equal to number of writes
	assert.Equal(t, int64(numGoroutines), rangeResp.Kvs[0].Version)

	// Revision should be at least numGoroutines
	assert.GreaterOrEqual(t, rangeResp.Kvs[0].ModRevision, int64(numGoroutines))
}
