package kvraft

import "sync"

type Database struct{

	mu      sync.Mutex
	db    map[string]string

}


func (db *Database) Get(key string) string{
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.db[key]
}


func (db *Database) Put(key,value string){
	db.mu.Lock()
	defer db.mu.Unlock()
	db.db[key] = value
}

func (db *Database) Append(key,value string){
	db.mu.Lock()
	defer db.mu.Unlock()
	db.db[key] += value
}