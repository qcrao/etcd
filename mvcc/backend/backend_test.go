// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	bolt "github.com/coreos/bbolt"
)

// 测试底层数据库的关闭
func TestBackendClose(t *testing.T) {
	b, tmpPath := NewTmpBackend(time.Hour, 10000)
	defer os.Remove(tmpPath)
	fmt.Println("tmpPath: ", tmpPath)

	// check close could work
	done := make(chan struct{})
	go func() {
		err := b.Close()
		if err != nil {
			t.Errorf("close error = %v, want nil", err)
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Errorf("failed to close database in 10s")
	}
}

// 测试快照功能
// 1. 先打开数据库事务，然后往里写入key，提交
// 2. 写快照到文件
// 3. 从2步的文件新建一个数据库后端
// 4. 遍历新数据库，获取写入的key
func TestBackendSnapshot(t *testing.T) {
	b, tmpPath := NewTmpBackend(time.Hour, 10000)
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("test"))
	tx.UnsafePut([]byte("test"), []byte("foo"), []byte("bar"))
	tx.UnsafePut([]byte("test"), []byte("goo"), []byte("bar1"))
	tx.Unlock()
	b.ForceCommit()

	// write snapshot to a new file
	f, err := ioutil.TempFile(os.TempDir(), "etcd_backend_test")
	if err != nil {
		t.Fatal(err)
	}
	snap := b.Snapshot()
	defer snap.Close()
	if _, err := snap.WriteTo(f); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// bootstrap new backend from the snapshot
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = f.Name(), time.Hour, 10000
	nb := New(bcfg)
	defer cleanup(nb, f.Name())

	newTx := b.BatchTx()
	newTx.Lock()
	ks, _ := newTx.UnsafeRange([]byte("test"), []byte("foo"), []byte("goo"), 0)
	fmt.Printf("ks: %+v\n", ks)
	if len(ks) != 1 {
		t.Errorf("len(kvs) = %d, want 1", len(ks))
	}
	newTx.Unlock()
}

// 测试提交时间间隔
func TestBackendBatchIntervalCommit(t *testing.T) {
	// start backend with super short batch interval so
	// we do not need to wait long before commit to happen.
	b, tmpPath := NewTmpBackend(time.Nanosecond, 10000)
	defer cleanup(b, tmpPath)

	pc := b.Commits()

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("test"))
	tx.UnsafePut([]byte("test"), []byte("foo"), []byte("bar"))
	tx.Unlock()

	for i := 0; i < 10; i++ {
		fmt.Println("====")
		if b.Commits() >= pc+1 {
			fmt.Println("break")
			break
		}
		time.Sleep(time.Duration(i*100) * time.Millisecond)
	}

	// check whether put happens via db view
	b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("test"))
		if bucket == nil {
			t.Errorf("bucket test does not exit")
			return nil
		}
		v := bucket.Get([]byte("foo"))
		fmt.Println("v: ", string(v))
		if v == nil {
			t.Errorf("foo key failed to written in backend")
		}
		return nil
	})
}

// 测试磁盘碎片整理
func TestBackendDefrag(t *testing.T) {
	b, tmpPath := NewDefaultTmpBackend()
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("test"))
	for i := 0; i < defragLimit+100; i++ {
		tx.UnsafePut([]byte("test"), []byte(fmt.Sprintf("foo_%d", i)), []byte("bar"))
	}
	tx.Unlock()
	b.ForceCommit()

	// remove some keys to ensure the disk space will be reclaimed after defrag
	tx = b.BatchTx()
	tx.Lock()
	for i := 0; i < 50; i++ {
		tx.UnsafeDelete([]byte("test"), []byte(fmt.Sprintf("foo_%d", i)))
	}
	tx.Unlock()
	b.ForceCommit()

	size := b.Size()

	// shrink and check hash
	oh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}

	// 回收磁盘碎片
	err = b.Defrag()
	if err != nil {
		t.Fatal(err)
	}

	nh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(oh, nh)
	if oh != nh {
		t.Errorf("hash = %v, want %v", nh, oh)
	}

	nsize := b.Size()
	fmt.Println("size, nsize: ", size, nsize)
	if nsize >= size {
		t.Errorf("new size = %v, want < %d", nsize, size)
	}

	// try put more keys after shrink.
	tx = b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("test"))
	tx.UnsafePut([]byte("test"), []byte("more"), []byte("bar"))
	tx.Unlock()
	b.ForceCommit()
	fmt.Println(b.Size())
}

// TestBackendWriteback ensures writes are stored to the read txn on write txn unlock.
// 确保写事务完成后能从读事务读取到
func TestBackendWriteback(t *testing.T) {
	b, tmpPath := NewDefaultTmpBackend()
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("key"))
	tx.UnsafePut([]byte("key"), []byte("abc"), []byte("bar"))
	tx.UnsafePut([]byte("key"), []byte("def"), []byte("baz"))
	tx.UnsafePut([]byte("key"), []byte("overwrite"), []byte("1"))
	tx.Unlock()

	// overwrites should be propagated too
	tx.Lock()
	tx.UnsafePut([]byte("key"), []byte("overwrite"), []byte("2"))
	tx.Unlock()

	keys := []struct {
		key   []byte
		end   []byte
		limit int64

		wkey [][]byte
		wval [][]byte
	}{
		{
			key: []byte("abc"),
			end: nil,

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("def"),

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("deg"),

			wkey: [][]byte{[]byte("abc"), []byte("def")},
			wval: [][]byte{[]byte("bar"), []byte("baz")},
		},
		{
			key:   []byte("abc"),
			end:   []byte("\xff"),
			limit: 1,

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("\xff"),

			wkey: [][]byte{[]byte("abc"), []byte("def"), []byte("overwrite")},
			wval: [][]byte{[]byte("bar"), []byte("baz"), []byte("2")},
		},
	}
	rtx := b.ReadTx()
	for i, tt := range keys {
		rtx.Lock()
		k, v := rtx.UnsafeRange([]byte("key"), tt.key, tt.end, tt.limit)
		rtx.Unlock()
		if !reflect.DeepEqual(tt.wkey, k) || !reflect.DeepEqual(tt.wval, v) {
			t.Errorf("#%d: want k=%+v, v=%+v; got k=%+v, v=%+v", i, tt.wkey, tt.wval, k, v)
		}
	}
}

// TestBackendWritebackForEach checks that partially written / buffered
// data is visited in the same order as fully committed data.
//
// 确保未提交的事务也能按正确的顺序读取到
func TestBackendWritebackForEach(t *testing.T) {
	b, tmpPath := NewTmpBackend(time.Hour, 10000)
	defer cleanup(b, tmpPath)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket([]byte("key"))
	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		tx.UnsafePut([]byte("key"), k, []byte("bar"))
	}
	tx.Unlock()

	// writeback
	b.ForceCommit()

	tx.Lock()
	tx.UnsafeCreateBucket([]byte("key"))
	for i := 5; i < 20; i++ {
		k := []byte(fmt.Sprintf("%04d", i))
		tx.UnsafePut([]byte("key"), k, []byte("bar"))
	}
	tx.Unlock()

	seq := ""
	getSeq := func(k, v []byte) error {
		seq += string(k)
		return nil
	}
	rtx := b.ReadTx()
	rtx.Lock()
	rtx.UnsafeForEach([]byte("key"), getSeq)
	rtx.Unlock()

	partialSeq := seq

	seq = ""
	b.ForceCommit()

	tx.Lock()
	tx.UnsafeForEach([]byte("key"), getSeq)
	tx.Unlock()

	fmt.Println(seq)
	if seq != partialSeq {
		t.Fatalf("expected %q, got %q", seq, partialSeq)
	}
}

func cleanup(b Backend, path string) {
	b.Close()
	os.Remove(path)
}
