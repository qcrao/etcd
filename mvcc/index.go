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

package mvcc

import (
	"sort"
	"sync"

	"github.com/google/btree"
)

type index interface {
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)
	Range(key, end []byte, atRev int64) ([][]byte, []revision)
	Revisions(key, end []byte, atRev int64) []revision
	Put(key []byte, rev revision)
	Tombstone(key []byte, rev revision) error
	RangeSince(key, end []byte, rev int64) []revision
	Compact(rev int64) map[revision]struct{}
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

type treeIndex struct {
	sync.RWMutex
	tree *btree.BTree
}

// 新建一个btree，每个node包含31-33个items和32-34个孩子
func newTreeIndex() index {
	return &treeIndex{
		tree: btree.New(32),
	}
}

// 将一个key加到btree中
// 每个key对应一个btree中的节点
func (ti *treeIndex) Put(key []byte, rev revision) {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	item := ti.tree.Get(keyi)
	if item == nil {
		keyi.put(rev.main, rev.sub)
		ti.tree.ReplaceOrInsert(keyi)
		return
	}
	okeyi := item.(*keyIndex)
	okeyi.put(rev.main, rev.sub)
}

// 获取给定key在给定版本atRev的最后编辑版本，创建版本，以及之后的版本数
func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
	keyi := &keyIndex{key: key}
	ti.RLock()
	defer ti.RUnlock()
	if keyi = ti.keyIndex(keyi); keyi == nil {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	return keyi.get(atRev)
}

func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()
	return ti.keyIndex(keyi)
}

// 根据只有一个key的keyIndex, 找到真正的keyIndex
func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}
	return nil
}

func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex)) {
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end}

	ti.RLock()
	defer ti.RUnlock()

	// 此处会一直调func, 直到返回false
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		// 当endi空非空，且大于endi
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		f(item.(*keyIndex))
		return true
	})
}

// // 获取给定key在给定版本atRev之后的key字典序小于end的版本
func (ti *treeIndex) Revisions(key, end []byte, atRev int64) (revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil
		}
		return []revision{rev}
	}
	// func 函数找到相应的版本添加到revs中
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(atRev); err == nil {
			revs = append(revs, rev)
		}
	})
	return revs
}

// 遍历[key, end)，找到相应的key及最后编辑版本
func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(atRev); err == nil {
			revs = append(revs, rev)
			keys = append(keys, ki.key)
		}
	})
	return keys, revs
}

// 调用keyIndex的tombstone
func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	item := ti.tree.Get(keyi)
	if item == nil {
		return ErrRevisionNotFound
	}

	ki := item.(*keyIndex)
	return ki.tombstone(rev.main, rev.sub)
}

// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
//
// RangeSince返回所有的key范围为[key, end)，版本号大于等于给定rev 的所有版本（主版本号相同的只返回子版本号最大的那个）
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	keyi := &keyIndex{key: key}

	ti.RLock()
	defer ti.RUnlock()

	if end == nil {
		item := ti.tree.Get(keyi)
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		return keyi.since(rev)
	}

	endi := &keyIndex{key: end}
	var revs []revision
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		curKeyi := item.(*keyIndex)
		revs = append(revs, curKeyi.since(rev)...)
		return true
	})
	sort.Sort(revisions(revs))

	return revs
}

func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	var emptyki []*keyIndex
	plog.Printf("store.index: compact %d", rev)
	// TODO: do not hold the lock for long time?
	// This is probably OK. Compacting 10M keys takes O(10ms).
	ti.Lock()
	defer ti.Unlock()
	ti.tree.Ascend(compactIndex(rev, available, &emptyki))
	for _, ki := range emptyki {
		item := ti.tree.Delete(ki)
		if item == nil {
			plog.Panic("store.index: unexpected delete failure during compaction")
		}
	}
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
//
// Keep找到Compaction命令需要保持的版本
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.RLock()
	defer ti.RUnlock()
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})
	return available
}

func compactIndex(rev int64, available map[revision]struct{}, emptyki *[]*keyIndex) func(i btree.Item) bool {
	return func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.compact(rev, available)
		if keyi.isEmpty() {
			*emptyki = append(*emptyki, keyi)
		}
		return true
	}
}

// 比较两个索引是否相等
func (ti *treeIndex) Equal(bi index) bool {
	b := bi.(*treeIndex)

	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

// 向btree中插入一条索引
func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.Lock()
	defer ti.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}
