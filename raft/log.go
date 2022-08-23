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
	"fmt"

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
	var head, rear uint64
	entries := make([]pb.Entry, 0)
	head, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	rear, err = storage.LastIndex()
	if err != nil {
		panic(err)
	}
	if head <= rear {
		entries, _ = storage.Entries(head, rear+1)
	}
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         0,
		stabled:         rear,
		entries:         entries,
		pendingSnapshot: &pb.Snapshot{},
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ret := make([]pb.Entry, 0, len(l.entries))
	for _, ent := range l.entries {
		if ent.Index <= l.stabled {
			continue
		}
		ret = append(ret, ent)
	}
	return ret
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	for _, ent := range l.entries {
		if ent.Index > l.committed {
			// assure the slice is ordered
			return
		}
		if ent.Index <= l.applied {
			continue
		}
		ents = append(ents, ent)
	}

	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		// todo
		return l.committed
	}

	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if len(l.entries) == 0 {
		return l.storage.Term(i)
	}
	if i < l.entries[0].Index {
		return l.storage.Term(i)
	}

	for _, ent := range l.entries {
		if ent.Index == i {
			return ent.Term, nil
		}
	}

	return 0, fmt.Errorf("not found index: %v", i)
}

// getEntByIndex return ent in the given index
func (l *RaftLog) getEntByIndex(i uint64) *pb.Entry {
	if i == 0 {
		return nil
	}
	for _, ent := range l.entries {
		if ent.Index == i {
			return &ent
		}
	}

	return nil
}

func (l *RaftLog) Append(e *pb.Entry) {
	l.entries = append(l.entries, *e)
}
