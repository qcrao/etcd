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
	"sync"
	"time"

	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/backend"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// non-const so modifiable by tests
var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// TODO: find a good buf value. 1024 is just a random one that
	// seems to be reasonable.
	chanBufLen = 1024

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	maxWatchersPerSync = 512
)

type watchable interface {
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	progress(w *watcher)
	rev() int64
}

type watchableStore struct {
	*store

	// mu protects watcher groups and batches. It should never be locked
	// before locking store.mu to avoid deadlock.
	mu sync.RWMutex

	// victims are watcher batches that were blocked on the watch channel
	//
	// victims是一组阻塞了的watcher
	victims []watcherBatch
	victimc chan struct{}

	// contains all unsynced watchers that needs to sync with events that have happened
	unsynced watcherGroup

	// contains all synced watchers that are in sync with the progress of the store.
	// The key of the map is the key that the watcher watches on.
	synced watcherGroup

	stopc chan struct{}
	wg    sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
type cancelFunc func()

func New(b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) ConsistentWatchableKV {
	return newWatchableStore(b, le, ig)
}

func newWatchableStore(b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) *watchableStore {
	s := &watchableStore{
		store:    NewStore(b, le, ig),
		victimc:  make(chan struct{}, 1),
		unsynced: newWatcherGroup(),
		synced:   newWatcherGroup(),
		stopc:    make(chan struct{}),
	}
	s.store.ReadView = &readView{s}
	s.store.WriteView = &writeView{s}
	if s.le != nil {
		// use this store as the deleter so revokes trigger watch events
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write() })
	}
	s.wg.Add(2)
	go s.syncWatchersLoop()
	go s.syncVictimsLoop()
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

func (s *watchableStore) NewWatchStream() WatchStream {
	watchStreamGauge.Inc()
	return &watchStream{
		watchable: s,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.mu.Lock()
	s.revMu.RLock()
	synced := startRev > s.store.currentRev || startRev == 0
	if synced {
		wa.minRev = s.store.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
	}
	if synced {
		s.synced.add(wa)
	} else {
		slowWatcherGauge.Inc()
		s.unsynced.add(wa)
	}
	s.revMu.RUnlock()
	s.mu.Unlock()

	watcherGauge.Inc()

	return wa, func() { s.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()
		if s.unsynced.delete(wa) {
			slowWatcherGauge.Dec()
			break
		} else if s.synced.delete(wa) {
			break
		} else if wa.compacted {
			break
		} else if wa.ch == nil {
			// already canceled (e.g., cancel/close race)
			break
		}

		if !wa.victim {
			panic("watcher not victim but not in watch groups")
		}

		var victimBatch watcherBatch
		for _, wb := range s.victims {
			if wb[wa] != nil {
				victimBatch = wb
				break
			}
		}
		if victimBatch != nil {
			slowWatcherGauge.Dec()
			delete(victimBatch, wa)
			break
		}

		// victim being processed so not accessible; retry
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	watcherGauge.Dec()
	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}

// syncWatchersLoop syncs the watcher in the unsynced map every 100ms.
//
// syncWatchersLoop同步unsynced组里的watcher
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done()

	for {
		s.mu.RLock()
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size()
		s.mu.RUnlock()

		unsyncedWatchers := 0
		if lastUnsyncedWatchers > 0 {
			unsyncedWatchers = s.syncWatchers()
		}
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// more work pending?
		// 如果未同步的watcher数量不为0 且 之前未同步的watcher数量大于当前未同步的watcher数量
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			// be fair to other store operations by yielding time taken
			waitDuration = syncDuration
		}

		select {
		case <-time.After(waitDuration):
		case <-s.stopc:
			return
		}
	}
}

// syncVictimsLoop tries to write precomputed watcher responses to
// watchers that had a blocked watcher channel
//
// syncVictimsLoop同步ch被阻塞的watcher
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()

	for {
		for s.moveVictims() != 0 {
			// try to update all victim watchers
		}
		s.mu.RLock()
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		// 如果isEmpty非空，说明此次调用moveVictims没有同步成功，可能是所有的watcher的ch
		// 均被阻塞，因此隔10ms之后再次重试
		var tickc <-chan time.Time
		if !isEmpty {
			tickc = time.After(10 * time.Millisecond)
		}

		// 等待10ms 或者 有新的victims加入，则会再次尝试同步victims
		select {
		case <-tickc:
		case <-s.victimc:
		case <-s.stopc:
			return
		}
	}
}

// moveVictims tries to update watches with already pending event data
func (s *watchableStore) moveVictims() (moved int) {
	// 首先将s.victims全部拿出来
	s.mu.Lock()
	victims := s.victims
	s.victims = nil
	s.mu.Unlock()

	var newVictim watcherBatch
	for _, wb := range victims {
		// try to send responses again
		// 尝试将victims中的事件发送出去
		for w, eb := range wb {
			// watcher has observed the store up to, but not including, w.minRev
			rev := w.minRev - 1
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
				pendingEventsGauge.Add(float64(len(eb.evs)))
			} else {
				// 同步不成功，将此watcher/eb 添加到新的victims
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			// 事件同步成功，则moved计算加1
			moved++
		}

		// assign completed victim watchers to unsync/sync
		// 将已完成同步的受害者加到已同步或未同步的group中
		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		for w, eb := range wb {
			if newVictim != nil && newVictim[w] != nil {
				// couldn't send watch response; stays victim
				continue
			}
			// 同步成功的victim执行如下操作
			// victim复位
			w.victim = false

			// 如果还未同步完
			if eb.moreRev != 0 {
				w.minRev = eb.moreRev
			}

			// 如果watcher的minRev小于当前版本，加入到unsynced组
			if w.minRev <= curRev {
				s.unsynced.add(w)
			} else {
				// 否则加入到synced组
				slowWatcherGauge.Dec()
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	// 如果还有watcher ch 被阻塞，仍然添加到store的victims中
	if len(newVictim) > 0 {
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	// 返回事件同步成功的（ch未被阻塞的）
	return moved
}

// syncWatchers syncs unsynced watchers by:
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
//
// syncWatchers同步所有的unsynced watchers:
// 1. 从unsynced watcher组里选择一组watcher
// 2. 遍历这组watcher，获取最小的版本，并移除compacted为true的watcher
// 3. 用最小的版本获取所有的kv，转化成事件，将事件发送给这些watcher
// 4. 将这组watcher添加到synced group
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsynced.size() == 0 {
		return 0
	}

	s.store.revMu.RLock()
	defer s.store.revMu.RUnlock()

	// in order to find key-value pairs from unsynced watchers, we need to
	// find min revision index, and these revisions can be used to
	// query the backend store of key-value pairs
	curRev := s.store.currentRev
	compactionRev := s.store.compactMainRev

	// 从unsynced group里选择一组watcher，最小的版本
	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	minBytes, maxBytes := newRevBytes(), newRevBytes()
	revToBytes(revision{main: minRev}, minBytes)
	revToBytes(revision{main: curRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	tx := s.store.b.ReadTx()
	tx.Lock()
	revs, vs := tx.UnsafeRange(keyBucketName, minBytes, maxBytes, 0)
	evs := kvsToEvents(wg, revs, vs)
	tx.Unlock()

	var victims watcherBatch
	wb := newWatcherBatch(wg, evs) // watcher -> events
	for w := range wg.watchers {
		w.minRev = curRev + 1

		// 此watcher没有需要同步的事件，移动synced监听组
		eb, ok := wb[w]
		if !ok {
			// bring un-notified watcher to synced
			s.synced.add(w)
			s.unsynced.delete(w)
			continue
		}

		// 此watcher有更多的事件没有收到
		if eb.moreRev != 0 {
			w.minRev = eb.moreRev
		}

		// 向watcher发送监听事件，如果被阻塞，则加入到victims
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else {
			if victims == nil {
				victims = make(watcherBatch)
			}
			w.victim = true
		}

		if w.victim {
			victims[w] = eb
		} else {
			// 没有同步完
			if eb.moreRev != 0 {
				// stay unsynced; more to read
				// 还有更多的事件没有向watcher发送
				continue
			}
			// 同步完了，将watcher加到synced组中
			s.synced.add(w)
		}
		// 从未同步的组里删掉此watcher
		s.unsynced.delete(w)
	}
	s.addVictim(victims)

	vsz := 0
	for _, v := range s.victims {
		vsz += len(v)
	}
	// "未跟上进度的watcher总数"=未同步完的watcher + 阻塞了的watcher
	slowWatcherGauge.Set(float64(s.unsynced.size() + vsz))

	return s.unsynced.size()
}

// kvsToEvents gets all events for the watchers from all key-value pairs
//
// kvsToEvents获取被关注的事件
func kvsToEvents(wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	for i, v := range vals {
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(v); err != nil {
			plog.Panicf("cannot unmarshal event: %v", err)
		}

		if !wg.contains(string(kv.Key)) {
			continue
		}

		ty := mvccpb.PUT
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE
			// patch in mod revision so watchers won't skip
			kv.ModRevision = bytesToRev(revs[i]).main
		}
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})
	}
	return evs
}

// notify notifies the fact that given event at the given rev just happened to
// watchers that watch on the key of the event.
//
// notify将给定版本的事件同步给synced watchers
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	for w, eb := range newWatcherBatch(&s.synced, evs) {
		if eb.revs != 1 {
			plog.Panicf("unexpected multiple revisions in notification")
		}
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else {
			// move slow watcher to victims
			w.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			w.victim = true
			victim[w] = eb
			s.synced.delete(w)
			slowWatcherGauge.Inc()
		}
	}
	s.addVictim(victim)
}

// 添加victim，其中的watcher的ch被阻塞
func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	s.victims = append(s.victims, victim)
	select {
	case s.victimc <- struct{}{}:
	default:
	}
}

func (s *watchableStore) rev() int64 { return s.store.Rev() }

// 当前watcher监听到的版本
func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.synced.watchers[w]; ok {
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

// 监听者
type watcher struct {
	// the watcher key
	key []byte
	// end indicates the end of the range to watch.
	// If end is set, the watcher is on a range.
	end []byte

	// victim is set when ch is blocked and undergoing victim processing
	// victim为true时表示ch阻塞了并且正在执行victim的操作
	victim bool

	// compacted is set when the watcher is removed because of compaction
	// compacted在watcher被移除之后会被置为true
	compacted bool

	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	//
	// restore在wathcer正从leader的快照文件中恢复的情况下置为true。
	// 而这也意味着此watcher刚从synced组移到unsynced组，所有就有可能在第一次加入到synced组后有一个高的版本。
	// unsynced组的watcher版本总是要小于等于当前版本，除非此watcher将要被移动到synced组。
	restore bool

	// minRev is the minimum revision update the watcher will accept
	minRev int64
	id     WatchID

	fcs []FilterFunc
	// a chan to send out the watch response.
	// The chan might be shared with other watchers.
	ch chan<- WatchResponse
}

// send将WatchResponse通过ch发送出去
func (w *watcher) send(wr WatchResponse) bool {
	progressEvent := len(wr.Events) == 0

	if len(w.fcs) != 0 {
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		for i := range wr.Events {
			filtered := false
			for _, filter := range w.fcs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			if !filtered {
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// if all events are filtered out, we should send nothing.
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	select {
	case w.ch <- wr:
		return true
	default:
		return false
	}
}
