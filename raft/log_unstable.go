package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// [etcd.unstable] u.offset <= lo <= hi <= u.offset+len(u.offset)
func (l *RaftLog) mustCheckOutOfBoundUnstable(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("[RaftLog.mustCheckOutOfBoundUnstable] invalid unstable.slice %d > %d", lo, hi)
	}
	// todo in etcd upper should be u.offset + len of entries
	// and u.offset == l.stabled+1
	upper := l.firstOffset + uint64(len(l.entries))
	if lo < l.firstOffset || hi > upper {
		//sfi, _ := l.storage.FirstIndex()
		//sli, _ := l.storage.LastIndex()
		//log.Errorf("[RaftLog.mustCheckOutOfBoundUnstable] storage.FirstIndex: %d, storage.LastIndex: %d, \nstorage.applied: %d, storage.committed: %d storage.stabled: %d",
		//	sfi, sli, l.applied, l.committed, l.stabled)
		//if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		//	log.Errorf("[RaftLog.mustCheckOutOfBoundUnstable] pending snap: %v", l.pendingSnapshot.Metadata)
		//}
		log.Panicf("[RaftLog.mustCheckOutOfBoundUnstable] unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, l.firstOffset, upper)
	}

	return nil
}

func (l *RaftLog) restoreUnstable(s *pb.Snapshot) {
	l.pendingSnapshot = s
	l.entries = l.entries[:0]
	l.firstOffset = s.Metadata.Index + 1
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}
