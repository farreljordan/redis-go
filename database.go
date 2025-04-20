package main

import (
	"sync"
	"time"
)

type Database struct {
	internal map[string]Entry
	sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		internal: make(map[string]Entry),
	}
}

type Entry struct {
	value      string
	expireDate time.Time
}

func (db *Database) Load(key string) (Entry, bool) {
	db.RLock()
	entry, ok := db.internal[key]
	db.RUnlock()

	if ok && !entry.expireDate.IsZero() && time.Now().After(entry.expireDate) {
		return Entry{}, false
	}

	return entry, ok
}

func (db *Database) Delete(keys []string) int {
	deleted := 0
	db.Lock()
	for _, key := range keys {
		if _, ok := db.internal[key]; ok {
			delete(db.internal, key)
			deleted++
		}
	}
	db.Unlock()
	return deleted
}

func (db *Database) Store(key string, value string, expireTime string) error {
	var expireDate time.Time
	if expireTime != "" {
		duration, err := time.ParseDuration(expireTime + "ms")
		if err != nil {
			return err
		}
		expireDate = time.Now().Add(duration)
	}

	db.Lock()
	db.internal[key] = Entry{value: value, expireDate: expireDate}
	db.Unlock()

	return nil
}
