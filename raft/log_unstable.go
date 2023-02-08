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
	upper := l.firstOffset + uint64(len(l.entries))
	if lo < l.firstOffset || hi > upper {
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
