package redis

import (
	"sync"
	"time"
)

var (
	db = NewDatabase()
)

const (
	EMPTY_DB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

type Database struct {
	sync.RWMutex
	internal map[string]Entry
}

func NewDatabase() *Database {
	return &Database{
		internal: make(map[string]Entry),
	}
}

type Entry struct {
	Value      string
	ExpireDate time.Time
}

func (rm *Database) Load(key string) (value Entry, ok bool) {
	rm.RLock()
	result, ok := rm.internal[key]
	rm.RUnlock()
	return result, ok
}

func (rm *Database) Delete(key string) {
	rm.Lock()
	delete(rm.internal, key)
	rm.Unlock()
}

func (rm *Database) Store(key string, value Entry) {
	rm.Lock()
	rm.internal[key] = value
	rm.Unlock()
}
