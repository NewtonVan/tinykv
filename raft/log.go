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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	firstOffset uint64
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
		firstOffset:     head,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// todo limit size
// !important maybe use copy
func (l *RaftLog) sliceWithLimitSize(lo, hi uint64) ([]pb.Entry, error) {
	if lo == hi {
		return nil, nil
	}
	ents := l.slice(lo, hi)

	return ents, nil
}

// todo return entries with limit size
func (l *RaftLog) getEntries(i uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}

	return l.sliceWithLimitSize(i, l.LastIndex()+1)
}

// todo check whether pendingSnapshot match [etcd]unstable.snapshot
func (l *RaftLog) maybeFirstIndex() (uint64, bool) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		return l.pendingSnapshot.Metadata.Index + 1, true
	}

	return 0, false
}

func (l *RaftLog) maybeLastIndex() (uint64, bool) {
	if s := len(l.entries); s != 0 {
		return l.firstOffset + uint64(s) - 1, true
	}

	return 0, false
}

func (l *RaftLog) firstIndex() uint64 {
	if i, ok := l.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	return index
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	ents, err := l.getEntries(l.firstIndex())
	if err == nil {
		return ents
	}

	panic(err)
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
	if i, ok := l.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return i
}

func (l *RaftLog) maybeTerm(i uint64) (uint64, bool) {
	if i < l.firstOffset {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, true
		}
		return 0, false
	}

	if last, ok := l.maybeLastIndex(); !ok || i > last {
		return 0, false
	}

	return l.entries[i-l.firstOffset].Term, true
}

// todo handle error return by storage
// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.LastIndex() {
		return 0, nil
	}

	if t, ok := l.maybeTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	// todo handle error
	panic(err)
}

func (l *RaftLog) Append(e *pb.Entry) {
	l.entries = append(l.entries, *e)
}

// truncate entries since idx
func (l *RaftLog) truncate(idx uint64) {
	for i, ent := range l.entries {
		if ent.Index < idx {
			continue
		}
		l.entries = l.entries[:i]
		return
	}
}

func (l *RaftLog) matchTerm(idx, term uint64) bool {
	t, err := l.Term(idx)
	if err != nil {
		return false
	}

	return t == term
}

func (l *RaftLog) maybeAppend(m pb.Message) (lastIdx uint64, ok bool) {
	if l.matchTerm(m.Index, m.LogTerm) {
		lastIdx = m.Index + uint64(len(m.Entries))
		ci := l.findConflict(ptrSlice2entSlice(m.Entries))
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Errorf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := m.Index + 1
			if ci-offset > uint64(len(m.Entries)) {
				log.Errorf("index, %d, is out of range [%d]", ci-offset, len(m.Entries))
			}
			l.append(ptrSlice2entSlice(m.Entries)...)
		}
		l.commitTo(min(m.Commit, lastIdx))

		return lastIdx, true
	}

	return 0, false
}

func (l *RaftLog) stableTo(i uint64) {
	l.stabled = i
}

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			return ne.Index
		}
	}

	return 0
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}

	return false
}

func (l *RaftLog) commitTo(toCommit uint64) {
	if l.committed >= toCommit {
		return
	}

	if l.LastIndex() < toCommit {
		log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
	}

	l.committed = toCommit
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index; after < l.committed {
		log.Errorf("after(%d) is out of range [committed(%d)]", after, l.committed)
		panic("new entry index can't less than the committed index")
	}

	l.truncateAndAppend(ents)

	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	case after == l.firstOffset+uint64(len(l.entries)):
		l.entries = append(l.entries, ents...)
	case after < l.firstOffset:
		log.Errorf("[RaftLog.truncateAndAppend] after: %v should not less than commit: %v", after, l.firstOffset)
		panic("[RaftLog.truncateAndAppend] after should not less than commit")
	default:
		if after <= l.stabled {
			// unlike etcd, tiny's offset include both
			// stabled and unstable parts
			l.stableTo(after - 1)
		}

		l.entries = append([]pb.Entry{}, l.slice(l.firstOffset, after)...)
		l.entries = append(l.entries, ents...)
	}
}

// todo used as unstable slice
// return unstable entries [lo,hi)
func (l *RaftLog) slice(lo, hi uint64) []pb.Entry {
	l.mustCheckOutOfBound(lo, hi)
	return l.entries[lo-l.firstOffset : hi-l.firstOffset]
}

func (l *RaftLog) mustCheckOutOfBound(lo, hi uint64) {
	if lo > hi {
		log.Errorf("invalid unstable.slice %d > %d", lo, hi)
		panic("[RaftLog.mustCheckOutofBound] lo bigger than hi")
	}
	upper := l.firstOffset + uint64(len(l.entries))
	if lo < l.firstOffset || hi > upper {
		log.Errorf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, l.firstOffset, upper)
		panic("[RaftLog.mustCheckOutofBound] out of bounds")
	}
}

func (l *RaftLog) lastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("[RaftLog.lastTerm] unexpected error when getting the last term (%v)", err)
	}

	return t
}

func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.LastIndex())
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected err: %v", err)

	return 0
}
