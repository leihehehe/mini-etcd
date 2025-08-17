package unit

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "mini-etcd/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_BasicRecovery(t *testing.T) {

	server1 := NewTestKvServer(t)
	defer server1.Close()

	for i := 0; i < 15; i++ {
		req := &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("test-key-%d", i)),
			Value: []byte(fmt.Sprintf("test-value-%d", i)),
		}
		resp, err := server1.Put(context.Background(), req)
		require.NoError(t, err)
		assert.Greater(t, resp.Header.Revision, int64(0))
	}

	time.Sleep(200 * time.Millisecond)

	for i := 15; i < 20; i++ {
		req := &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("test-key-%d", i)),
			Value: []byte(fmt.Sprintf("test-value-%d", i)),
		}
		_, err := server1.Put(context.Background(), req)
		require.NoError(t, err)
	}
	server1.Close()

	server2 := NewTestKvServer(t)
	defer server2.Close()

	for i := 0; i < 20; i++ {
		req := &pb.RangeRequest{
			Key: []byte(fmt.Sprintf("test-key-%d", i)),
		}
		resp, err := server2.Range(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1, "Key %d should exist", i)
		assert.Equal(t, []byte(fmt.Sprintf("test-value-%d", i)), resp.Kvs[0].Value)
	}

	finalResp, err := server2.Range(context.Background(), &pb.RangeRequest{Key: []byte("test-key-0")})
	require.NoError(t, err)
	assert.Equal(t, int64(20), finalResp.Header.Revision)
}

func TestSnapshot_EmptyRecovery(t *testing.T) {

	server1 := NewTestKvServer(t)
	server1.Close()

	server2 := NewTestKvServer(t)
	defer server2.Close()

	resp, err := server2.Range(context.Background(), &pb.RangeRequest{
		Key:      []byte("a"),
		RangeEnd: []byte("z"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Count)
}

func TestSnapshot_WithDelete(t *testing.T) {

	server1 := NewTestKvServer(t)
	defer server1.Close()

	for i := 0; i < 10; i++ {
		req := &pb.PutRequest{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		_, err := server1.Put(context.Background(), req)
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		req := &pb.DeleteRequest{
			Key: []byte(fmt.Sprintf("key-%d", i)),
		}
		_, err := server1.Delete(context.Background(), req)
		require.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	server1.Close()

	server2 := NewTestKvServer(t)
	defer server2.Close()

	for i := 0; i < 10; i++ {
		req := &pb.RangeRequest{
			Key: []byte(fmt.Sprintf("key-%d", i)),
		}
		resp, err := server2.Range(context.Background(), req)
		require.NoError(t, err)

		if i < 5 {
			assert.Equal(t, int64(0), resp.Count, "Key %d should be deleted", i)
		} else {
			assert.Equal(t, int64(1), resp.Count, "Key %d should exist", i)
			assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), resp.Kvs[0].Value)
		}
	}
}
