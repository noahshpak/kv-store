package main

import (
	"db/kvstore"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

// merging in separate thread
// both in-memory and on-disk
// recovery

var store kvstore.KvStore

func get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := store.Get(key)
	w.Write([]byte(value))
}

func set(w http.ResponseWriter, r *http.Request) {
	for k, v := range r.URL.Query() {
		// store.Mu.Lock()
		store.Set(k, v[0])
		// store.Mu.Unlock()
	}
	if store.Length() > 3 {
		ckpt()

	}
}

func ckpt() {
	log.Printf("checkpointing")
	dbFile, err := ioutil.TempFile("ckpt", "db")
	if err != nil {
		log.Fatal(err)
		return
	}
	indexFile, err := ioutil.TempFile("ckpt", "index")
	if err != nil {
		log.Fatal(err)
	}
	store.Mu.Lock()
	store.Checkpoint(dbFile.Name(), indexFile.Name())
	err = os.Rename(dbFile.Name(), store.DbPath)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Rename(indexFile.Name(), store.IndexPath)
	if err != nil {
		log.Fatal(err)
	}
	store.Reload()
	defer store.Mu.Unlock()
}

func start_server() {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", get)
	mux.HandleFunc("/set", set)

	log.Print("Starting on 8080")
	ticker := time.NewTicker(20 * time.Second)
	go func() {
		for range ticker.C {
			ckpt()
		}
	}()

	err := http.ListenAndServe(":8080", mux)
	log.Fatal(err)
}

func main() {
	path := os.Args[1]
	store = *kvstore.Load(path)
	start_server()
}
