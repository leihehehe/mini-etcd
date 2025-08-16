package unit

import (
	"fmt"
	"math/rand"
	"mini-etcd/service"
	"os"
	"testing"
	"time"
)

func NewTestKvServer(t *testing.T) *service.KvServer {
	rand.Seed(time.Now().UnixNano())
	testDir := fmt.Sprintf("test_data_%s_%d", t.Name(), rand.Int63())

	t.Cleanup(func() {
		os.RemoveAll(testDir)
	})

	return service.NewKVServer(testDir)
}
