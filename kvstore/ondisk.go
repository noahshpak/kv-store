package kvstore

import (
	"bufio"
	"encoding/gob"
	"log"
	"os"
	pathlib "path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
		return
	}
}

func read(f *os.File, kv *string) error {
	dec := gob.NewDecoder(f)
	return dec.Decode(kv)
}

// dump checkpoint (in memory ondiskIndex to new file)
// writes need to acquire the lock during checkpointing; reads can continue

type KvStore struct {
	onDiskIndex map[string]int64
	DbPath      string
	IndexPath   string
	Mu          sync.Mutex
}

func Load(path string) *KvStore {
	disk := make(map[string]int64)
	s := KvStore{disk, pathlib.Join(path, "db.gob"), pathlib.Join(path, "index"), sync.Mutex{}}
	s.Reload()
	return &s
}

func (s *KvStore) Set(key, value string) {
	if strings.Contains(key, ":") {
		log.Print("Cannot set key with ':' in name")
		return
	}
	f, err := os.OpenFile(s.DbPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	check(err)
	stats, err := f.Stat()
	check(err)
	pos := stats.Size()
	// write to disk
	enc := gob.NewEncoder(f)
	err = enc.Encode(value)
	check(err)
	// write to memory
	s.set_offset(s.IndexPath, key, pos)
	log.Print("Set!")
}

func (s *KvStore) set_offset(path, key string, offset int64) {
	s.onDiskIndex[key] = offset
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	check(err)
	writer := bufio.NewWriter(f)
	writer.WriteString(key + ":" + strconv.Itoa(int(offset)) + "\n")
	writer.Flush()
}

func (s *KvStore) Get(key string) string {
	if strings.Contains(key, ":") {
		log.Print("Key not supported")
		return ""
	}
	offset, prs := s.onDiskIndex[key]
	if prs {
		return s.read_once_from(offset)
	}
	return ""
}

func (s *KvStore) Reload() {
	f, err := os.OpenFile(s.IndexPath, os.O_CREATE|os.O_RDONLY, 0664)
	check(err)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		pieces := strings.Split(line, ":")
		if len(pieces) > 2 {
			panic("More than 2 pieces found in index file")
		}
		v, err := strconv.Atoi(pieces[1])
		check(err)
		s.onDiskIndex[pieces[0]] = int64(v)
	}
	log.Print(s.onDiskIndex)
}

func (s *KvStore) Length() int {
	return len(s.onDiskIndex)
}

func (s *KvStore) Checkpoint(dbPath, indexPath string) error {
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY, 0664)
	check(err)
	indexWriter := bufio.NewWriter(indexFile)
	check(err)
	dbFile, err := os.OpenFile(dbPath, os.O_CREATE|os.O_WRONLY, 0664)
	check(err)
	enc := gob.NewEncoder(dbFile)
	// write to disk
	keys := make([]string, 0, len(s.onDiskIndex))
	for k, _ := range s.onDiskIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		offset := s.onDiskIndex[k]
		v := s.read_once_from(offset)
		stats, err := dbFile.Stat()
		check(err)
		offset = stats.Size()
		err = enc.Encode(v)
		check(err)
		// write to disk
		indexWriter.WriteString(k + ":" + strconv.Itoa(int(offset)) + "\n")
		indexWriter.Flush()
	}
	return nil
}

func (s *KvStore) read_once_from(offset int64) string {
	fr, err := os.OpenFile(s.DbPath, os.O_RDONLY, 0644)
	check(err)
	fr.Seek(offset, 0)
	var value string
	err = read(fr, &value)
	check(err)
	return value
}
