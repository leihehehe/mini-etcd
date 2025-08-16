package unit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "mini-etcd/proto"
)

func TestDelete_SingleKey_Found(t *testing.T) {
	server := NewTestKvServer(t)

	putReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	deleteReq := &pb.DeleteRequest{
		Key: []byte("test-key"),
	}
	resp, err := server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Deleted)
	assert.Equal(t, int64(2), resp.Header.Revision)
}

func TestDelete_SingleKey_NotFound(t *testing.T) {
	server := NewTestKvServer(t)

	deleteReq := &pb.DeleteRequest{
		Key: []byte("non-existent"),
	}
	resp, err := server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Deleted)
	assert.Equal(t, int64(1), resp.Header.Revision)
}

func TestDelete_WithPrevKv(t *testing.T) {
	server := NewTestKvServer(t)

	putReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	deleteReq := &pb.DeleteRequest{
		Key:     []byte("test-key"),
		PrevKey: true,
	}
	resp, err := server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Deleted)
	require.Len(t, resp.PrevKvs, 1)
	assert.Equal(t, []byte("test-key"), resp.PrevKvs[0].Key)
	assert.Equal(t, []byte("test-value"), resp.PrevKvs[0].Value)
}

func TestDelete_RangeDelete(t *testing.T) {
	server := NewTestKvServer(t)

	keys := []string{"config/app1/db", "config/app1/cache", "config/app2/db"}
	for _, key := range keys {
		putReq := &pb.PutRequest{
			Key:   []byte(key),
			Value: []byte("value"),
		}
		_, err := server.Put(context.Background(), putReq)
		require.NoError(t, err)
	}

	deleteReq := &pb.DeleteRequest{
		Key:      []byte("config/app1/"),
		RangeEnd: []byte("config/app1/z"),
	}
	resp, err := server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.Deleted)

	rangeReq := &pb.RangeRequest{
		Key:      []byte("config/app1/"),
		RangeEnd: []byte("config/app1/z"),
	}
	rangeResp, err := server.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rangeResp.Count)
}
