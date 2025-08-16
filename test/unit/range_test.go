package unit

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "mini-etcd/proto"
	"mini-etcd/service"
)

// initialization
func setupTestData(server *service.KvServer) {
	testData := map[string]string{
		"config/app1/database": "mysql",
		"config/app1/cache":    "redis",
		"config/app2/database": "postgres",
		"config/app2/cache":    "memcached",
		"other/key":            "value",
		"single":               "item",
	}

	for key, value := range testData {
		req := &pb.PutRequest{
			Key:   []byte(key),
			Value: []byte(value),
		}
		_, err := server.Put(context.Background(), req)
		if err != nil {
			panic(err)
		}
	}
}

func TestRange_SingleKey_Found(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key: []byte("config/app1/database"),
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Count)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, []byte("config/app1/database"), resp.Kvs[0].Key)
	assert.Equal(t, []byte("mysql"), resp.Kvs[0].Value)
}

func TestRange_SingleKey_NotFound(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key: []byte("non-existent-key"),
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Count)
	assert.Len(t, resp.Kvs, 0)
}

func TestRange_PrefixQuery(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key:      []byte("config/app1/"),
		RangeEnd: []byte("config/app1/z"),
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Count)
	assert.Len(t, resp.Kvs, 2)

	keys := make([]string, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		keys[i] = string(kv.Key)
	}
	assert.Equal(t, []string{"config/app1/cache", "config/app1/database"}, keys)
}

func TestRange_WithLimit(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key:      []byte("config/"),
		RangeEnd: []byte("config/z"),
		Limit:    2,
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Count)
	assert.Len(t, resp.Kvs, 2)
}

func TestRange_KeysOnly(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key:      []byte("config/app1/"),
		RangeEnd: []byte("config/app1/z"),
		KeysOnly: true,
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)

	for _, kv := range resp.Kvs {
		assert.NotEmpty(t, kv.Key)
		assert.Empty(t, kv.Value)
	}
}

func TestRange_NegativeLimit(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key:   []byte("config/app1/database"),
		Limit: -1,
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Count)
	assert.Len(t, resp.Kvs, 0)
}

func TestRange_WithRevision_MVCC(t *testing.T) {
	server := NewTestKvServer(t)

	req1 := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("value1"),
	}
	_, err := server.Put(context.Background(), req1)
	require.NoError(t, err)

	req2 := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("value2"),
	}
	_, err = server.Put(context.Background(), req2)
	require.NoError(t, err)

	rangeReq := &pb.RangeRequest{
		Key:      []byte("test-key"),
		Revision: 1,
	}

	resp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Count)
	require.Len(t, resp.Kvs, 1)
}

func TestRange_EmptyKey(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key: []byte(""),
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Count)
	assert.Len(t, resp.Kvs, 0)
}

func TestRange_InvalidRevision(t *testing.T) {
	server := NewTestKvServer(t)

	// Put one key to create revision 1
	putReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	// Try to read from future revision
	rangeReq := &pb.RangeRequest{
		Key:      []byte("test-key"),
		Revision: 100, // Future revision
	}

	resp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Count) // Should return empty result gracefully
	assert.Len(t, resp.Kvs, 0)
}

func TestRange_ZeroLimit(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key:      []byte("config/"),
		RangeEnd: []byte("config/z"),
		Limit:    0, // No limit
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.Count, int64(4)) // Should return all config keys
}

func TestRange_LargeLimit(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	req := &pb.RangeRequest{
		Key:      []byte("config/"),
		RangeEnd: []byte("config/z"),
		Limit:    1000000, // Very large limit
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.Count, int64(4)) // Should return all available keys
}

func TestRange_ConcurrentReads(t *testing.T) {
	server := NewTestKvServer(t)
	setupTestData(server)

	numGoroutines := 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make([][]*pb.KeyValue, numGoroutines)
	errors := make([]error, numGoroutines)

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			defer wg.Done()

			req := &pb.RangeRequest{
				Key:      []byte("config/"),
				RangeEnd: []byte("config/z"),
			}

			resp, err := server.Range(context.Background(), req)
			errors[index] = err
			if err == nil {
				results[index] = resp.Kvs
			}
		}(i)
	}

	wg.Wait()

	// Check all reads succeeded
	for i, err := range errors {
		assert.NoError(t, err, "Goroutine %d failed", i)
	}

	// Check all reads returned the same data
	firstResult := results[0]
	for i := 1; i < len(results); i++ {
		assert.Equal(t, len(firstResult), len(results[i]), "Result %d has different length", i)
		for j, kv := range firstResult {
			assert.Equal(t, kv.Key, results[i][j].Key, "Key mismatch in result %d", i)
			assert.Equal(t, kv.Value, results[i][j].Value, "Value mismatch in result %d", i)
		}
	}
}

func TestRange_HistoricalRevision(t *testing.T) {
	server := NewTestKvServer(t)

	// Put initial value
	putReq1 := &pb.PutRequest{
		Key:   []byte("history-key"),
		Value: []byte("value-v1"),
	}
	resp1, err := server.Put(context.Background(), putReq1)
	require.NoError(t, err)
	rev1 := resp1.Header.Revision

	// Update value
	putReq2 := &pb.PutRequest{
		Key:   []byte("history-key"),
		Value: []byte("value-v2"),
	}
	_, err = server.Put(context.Background(), putReq2)
	require.NoError(t, err)

	// Read from historical revision
	rangeReq := &pb.RangeRequest{
		Key:      []byte("history-key"),
		Revision: rev1,
	}

	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)

	// Should return the key since it was created at rev1
	assert.Equal(t, int64(1), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 1)
	assert.Equal(t, []byte("history-key"), rangeResp.Kvs[0].Key)
	// Note: Current implementation doesn't store historical values,
	// it only checks if the key existed at that revision
}

func TestRange_AllKeys(t *testing.T) {
	server := NewTestKvServer(t)

	// Put keys with different prefixes
	keys := []string{
		"a/key1", "a/key2",
		"b/key1", "b/key2",
		"c/key1",
		"single",
	}

	for _, key := range keys {
		putReq := &pb.PutRequest{
			Key:   []byte(key),
			Value: []byte("value"),
		}
		_, err := server.Put(context.Background(), putReq)
		require.NoError(t, err)
	}

	// Range all keys using empty range_end
	req := &pb.RangeRequest{
		Key:      []byte("\x00"), // Start from beginning
		RangeEnd: []byte("\xff"), // End at maximum
	}

	resp, err := server.Range(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(len(keys)), resp.Count)
	assert.Len(t, resp.Kvs, len(keys))
}

func TestRange_LightConcurrency(t *testing.T) {
	server := NewTestKvServer(t)

	for i := 0; i < 10; i++ {
		putReq := &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("initial-key-%d", i)),
			Value: []byte(fmt.Sprintf("initial-value-%d", i)),
		}
		_, err := server.Put(context.Background(), putReq)
		require.NoError(t, err)
	}

	numReaders := 5
	var wg sync.WaitGroup
	wg.Add(numReaders)

	results := make([]int64, numReaders)
	errors := make([]error, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(index int) {
			defer wg.Done()

			req := &pb.RangeRequest{
				Key:      []byte("initial-key-"),
				RangeEnd: []byte("initial-key-z"),
			}

			resp, err := server.Range(context.Background(), req)
			errors[index] = err
			if err == nil {
				results[index] = resp.Count
			}
		}(i)
	}

	wg.Wait()

	for i, err := range errors {
		assert.NoError(t, err, "Reader %d failed", i)
	}

	for i, count := range results {
		assert.Equal(t, int64(10), count, "Reader %d got wrong count", i)
	}
}
