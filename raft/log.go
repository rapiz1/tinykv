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

package raft

import (
	"errors"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hi, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	lo, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	ents := make([]pb.Entry, 0)
	sents, err := storage.Entries(lo, hi+1)
	if err == nil {
		ents = append(ents, sents...)
	} else {
		panic(err)
	}

	//hs, _, _ := storage.InitialState()

	return &RaftLog{
		storage: storage,
		entries: ents,
		stabled: hi,
		applied: lo - 1,
	}
}

func validateEntries(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}
	for i := range entries {
		if i != 0 {
			if entries[i].Index != entries[i-1].Index+1 {
				log.Panic("invalid entries", entries)
			}
		}
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	n := len(l.entries)
	if n == 0 {
		return
	}
	idx := sort.Search(n, func(i int) bool { return l.entries[i].Index > l.stabled })
	l.entries = l.entries[idx:]
}

func (l *RaftLog) convertIdx(i int) int {
	return i - int(l.entries[0].Index)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.Entries(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.Entries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var idx uint64
	if len(l.entries) == 0 {
		idx, _ = l.storage.LastIndex()
	} else {
		idx = l.entries[len(l.entries)-1].Index
	}
	if !IsEmptySnap(l.pendingSnapshot) {
		idx = max(idx, l.pendingSnapshot.Metadata.Index)
	}
	return idx
}

func (l *RaftLog) FirstIndex() uint64 {
	fi, _ := l.storage.FirstIndex()
	if len(l.entries) != 0 {
		fi = min(l.entries[0].Index, fi)
	}
	return fi
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if !IsEmptySnap(l.pendingSnapshot) {
		if l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			return 0, ErrCompacted
		}
	}
	if len(l.entries) != 0 && i >= l.entries[0].Index {
		idx := i - l.entries[0].Index
		if idx >= uint64(len(l.entries)) {
			return 0, errors.New("out of range")
		}
		return l.entries[idx].Term, nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) LastTerm() uint64 {
	t, _ := l.Term(l.LastIndex())
	return t
}

// [lo, hi)
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	ret := make([]pb.Entry, 0)
	if hi <= lo || lo > l.LastIndex() {
		return ret
	}
	if lo < l.FirstIndex() {
		t, err := l.Term(lo)
		log.Info(t, err, l.entries)
		log.Panic("try to get entries [", lo, ",", hi, ") with ", l.FirstIndex(), l.LastIndex())
	}

	hi = min(l.LastIndex()+1, hi)

	if len(l.entries) == 0 || lo < l.entries[0].Index {
		hiInStr := hi
		if len(l.entries) != 0 {
			hiInStr = min(hiInStr, l.entries[0].Index)
		}
		ents, err := l.storage.Entries(lo, hiInStr)
		if err != nil {
			panic(err)
		}
		ret = append(ret, ents...)
	}

	if len(l.entries) != 0 {
		var loInEnt, hiInEnt uint64
		if lo >= l.entries[0].Index {
			loInEnt = lo - l.entries[0].Index
		}
		if hi >= l.entries[0].Index {
			hiInEnt = hi - l.entries[0].Index
		}
		if hiInEnt > uint64(len(l.entries)) {
			log.Panic(hi, l.LastIndex(), l.entries)
		}

		ret = append(ret, l.entries[loInEnt:hiInEnt]...)
	}

	if ret[0].Index != lo {
		log.Info(l.entries)
		log.Panic(lo, hi, ret)
	}

	validateEntries(ret)
	return ret
}

func (l *RaftLog) EntriesWithPointers(lo, hi uint64) []*pb.Entry {
	e := l.Entries(lo, hi)
	ents := make([]*pb.Entry, 0)
	for k := range e {
		ents = append(ents, &e[k])
	}
	return ents
}

func (l *RaftLog) Append(entries ...*pb.Entry) {
	for _, ent := range entries {
		if l.LastIndex() < ent.Index {
			if ent.Index != l.LastIndex()+1 {
				log.Info(l.Entries(l.FirstIndex(), l.LastIndex()+1))
				log.Panic(l.LastIndex(), ent.Index, l.entries, ent)
			}
			l.entries = append(l.entries, *ent)
		} else {
			// ent.Index <= l.LastIndex
			t, err := l.Term(ent.Index)
			if err == nil && ent.Term == t {
				continue
			}
			if len(l.entries) == 0 {
				l.entries = append(l.entries, *ent)
			} else {
				var idx uint64
				if ent.Index > l.entries[0].Index {
					idx = ent.Index - l.entries[0].Index
				}
				if idx >= uint64(len(l.entries)) {
					log.Panic(idx, len(l.entries), ent, l.entries)
				}
				l.entries[idx] = *ent
				l.entries = l.entries[:idx+1]
			}
			l.stabled = min(ent.Index-1, l.stabled)
		}
	}
}
