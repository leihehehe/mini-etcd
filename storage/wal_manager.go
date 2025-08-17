package storage

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"mini-etcd/internal"
	"os"
	"path/filepath"
	"sync"
)

type WALManager struct {
	file *os.File
	mu   sync.Mutex
	path string
}

type WALEntry struct {
	Type     string
	Key      []byte
	Value    []byte
	Revision int64
}

func NewWALManager(path string) (*WALManager, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(path, internal.WALFileName)
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WALManager{
		file: file,
		path: walPath,
	}, nil
}

func (w *WALManager) Write(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	length := uint32(len(data))

	//len check
	if err := binary.Write(w.file, binary.BigEndian, length); err != nil {
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *WALManager) Read() ([]*WALEntry, error) {
	file, err := os.Open(w.path)
	if err != nil {
		//we don't have a WALManager file
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	defer file.Close()
	var entries []*WALEntry
	for {
		var length uint32
		err := binary.Read(file, binary.BigEndian, &length)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		//read data
		data := make([]byte, length)
		if _, err := file.Read(data); err != nil {
			return nil, err
		}

		var entry WALEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, &entry)
	}
	return entries, nil
}

func (w *WALManager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
