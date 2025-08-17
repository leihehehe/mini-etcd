package unit

import (
	"context"
	"testing"

	pb "mini-etcd/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL_PersistencePutGet(t *testing.T) {

	server := NewTestKvServer(t)
	defer server.Close()

	putReq := &pb.PutRequest{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	server.Close()

	server2 := NewTestKvServer(t)
	defer server2.Close()

	rangeReq := &pb.RangeRequest{
		Key: []byte("test-key"),
	}
	resp, err := server2.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, []byte("test-value"), resp.Kvs[0].Value)
}

func TestWAL_PersistenceDelete(t *testing.T) {

	server := NewTestKvServer(t)
	defer server.Close()

	putReq := &pb.PutRequest{
		Key:   []byte("delete-key"),
		Value: []byte("delete-value"),
	}
	_, err := server.Put(context.Background(), putReq)
	require.NoError(t, err)

	deleteReq := &pb.DeleteRequest{
		Key: []byte("delete-key"),
	}
	_, err = server.Delete(context.Background(), deleteReq)
	require.NoError(t, err)

	server.Close()
	server2 := NewTestKvServer(t)
	defer server2.Close()

	rangeReq := &pb.RangeRequest{
		Key: []byte("delete-key"),
	}
	resp, err := server2.Range(context.Background(), rangeReq)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Count)
}
