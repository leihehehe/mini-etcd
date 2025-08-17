package unit

import (
	"fmt"
	"mini-etcd/service"
	"os"
	"testing"
)

func NewTestKvServer(t *testing.T) *service.KvServer {
	testDir := fmt.Sprint("test_data_")

	t.Cleanup(func() {
		os.RemoveAll(testDir)
	})

	return service.NewKVServer(testDir)
}
