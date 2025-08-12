package service

import (
	"log"
	pb "mini-etcd/proto"
	"sync"

	"github.com/google/uuid"
)

type WatcherManager struct {
	mu sync.RWMutex
	// id -> watcher
	watchers map[string]*watcher
	// key -> watcher
	keyWatchers map[string][]*watcher
}

type watcher struct {
	id            string
	key           string
	rangeEnd      string
	startRev      int64
	prevKv        bool
	eventChannel  chan *pb.Event
	cancelChannel chan struct{}
	stream        pb.KV_WatchServer
}

func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		watchers:    make(map[string]*watcher),
		keyWatchers: make(map[string][]*watcher),
	}
}

func (wm *WatcherManager) generateWatchId() string {
	return uuid.New().String()
}

func (wm *WatcherManager) notify(event *pb.Event) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	key := string(event.Kv.Key)
	for _, w := range wm.watchers {
		if wm.isKeyInRange(key, w.key, w.rangeEnd) {
			wm.handleEvent(event, w)
		}
	}
}

func (wm *WatcherManager) watch(newWatcher *watcher) *watcher {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.watchers[newWatcher.id] = newWatcher
	wm.keyWatchers[newWatcher.key] = append(wm.keyWatchers[newWatcher.key], newWatcher)

	return newWatcher
}

func (wm *WatcherManager) handleEvent(event *pb.Event, w *watcher) {
	select {
	case w.eventChannel <- event:
		// success
	case <-w.cancelChannel:
		// chanel canceled
	default:
		log.Printf("watcher %s is busy", w.id)
	}
}

func (wm *WatcherManager) isKeyInRange(key, start, end string) bool {
	//exact match
	if end == "" {
		return key == start
	}
	return key >= start && key < end
}

func (wm *WatcherManager) removeWatcher(watcherId string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	w, exists := wm.watchers[watcherId]
	if !exists {
		return
	}
	delete(wm.watchers, watcherId)

	key := w.key
	if watchers, ok := wm.keyWatchers[key]; ok {
		for i, w := range watchers {
			if w.id == watcherId {
				wm.keyWatchers[key] = append(watchers[:i], watchers[i+1:]...)
				break
			}
		}
		if len(wm.keyWatchers[key]) == 0 {
			delete(wm.keyWatchers, key)
		}
	}

	close(w.cancelChannel)
}
