package raft

import "github.com/pingcap-incubator/tinykv/log"

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
