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
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"go.uber.org/zap"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next  uint64
	RecentActive bool
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// random election time out
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id:      c.ID,
		Term:    hardState.Term,
		Vote:    hardState.Vote,
		RaftLog: newLog(c.Storage),
		Prs:     map[uint64]*Progress{},
		State:   StateFollower,
		votes:   map[uint64]bool{},
		//msgs:             []pb.Message{},
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// todo
	if c.peers == nil {
		c.peers = confState.GetNodes()
	}
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[id] = &Progress{
				Next: r.RaftLog.LastIndex() + 1,
			}
		}
	}
	r.becomeFollower(r.Term, None)
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}

	return r
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		// All {pre-,}campaign messages need to have the term set when
		// sending.
		// - MsgRequestVote: m.Term is the term the node is campaigning for,
		//   non-zero as we increment the term when campaigning.
		// - MsgRequestVoteResp: m.Term is the new r.Term if the MsgVote was
		//   granted, non-zero for the same reason MsgVote is
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MsgProp
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return r.maybeSendAppend(to, true)
}

// todo
// 1. handle err
// 2. update peer state
func (r *Raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.Prs[to]
	// in normal case Next = Match + 1,
	// but there exists anomaly case in which it doesn't establish
	// so to get index and term for sync, we have to use Next to generate
	idx := pr.Next - 1
	logTerm, errt := r.RaftLog.Term(idx)
	ents, erre := r.RaftLog.getEntries(pr.Next)
	m := pb.Message{}
	m.To = to

	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	if errt != nil || erre != nil {
		if !pr.RecentActive {
			return false
		}
		m.MsgType = pb.MessageType_MsgSnapshot
		snapShot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false

			}
			log.Panicf("[Raft.maybeSendAppend] meet snap err: %v", err)
		}
		if IsEmptySnap(&snapShot) {
			log.Panicf("[Raft.maybeSendAppend] nil snap")
		}
		m.Snapshot = &snapShot
		// todo peer
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = idx
		m.LogTerm = logTerm
		m.Entries = entSlice2ptrSlice(ents)
		m.Commit = r.RaftLog.committed
		// todo
	}
	r.send(m)

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  commit,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartBeat()
	default:
		log.Warn("undefined state")
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed < r.randomElectionTimeout {
		return
	}

	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.checkQuorum()
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeadTransfer()
		}
	}
	if r.State != StateLeader {
		return
	}
	// todo refactor
	//if r.leadTransferee != None {
	//	r.electionElapsed++
	//	if r.electionElapsed < r.randomElectionTimeout {
	//		return
	//	}
	//	r.abortLeadTransfer()
	//}
	if r.heartbeatElapsed < r.heartbeatTimeout {
		return
	}

	r.heartbeatElapsed = 0
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.PendingConfIndex = 0
	r.abortLeadTransfer()
	r.resetRandomElectionTimeout()

	r.ResetVotes()
	r.Visit(func(id uint64, pr *Progress) {
		*pr = Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	})
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.PendingConfIndex = r.RaftLog.LastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		log.Panic("empty entry was dropped")
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// handle relationship of msg and node
	switch {
	case m.Term == 0:
	case m.Term > r.Term:
		log.Debugf("%x [term: %d] received a %s message with higher term from %x [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// ignore followed with early etcd
		if r.State == StateLeader && (m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend) {
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
		}
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgRequestVote:
		r.handleReqVote(m)
	default:
		switch r.State {
		case StateFollower:
			r.stepFollower(m)
		case StateCandidate:
			r.stepCandidate(m)
		case StateLeader:
			r.stepLeader(m)
		}
	}

	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Debugf("%x no leader at term %x; stop transfer leader", r.id, r.Term)
			return
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			log.Debugf("%x no leader at term %x; dropping proposal", r.id, r.Term)
			return
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeoutNow(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Debugf("%x no leader at term %x; dropping proposal", r.id, r.Term)
		return
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		res := r.poll(m.From, !m.Reject)
		switch res {
		case VoteWon:
			r.becomeLeader()
			r.bcastAppend()
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeoutNow(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("leader %d meet empty propose", r.id)
		}
		// leader & membership change check
		if _, ok := r.Prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return
		}
		if r.leadTransferee != None {
			log.Infof("[Raft.stepLeader] %v is transferring lead to %v", r.id, r.leadTransferee)
			return
		}
		r.activePeer(m.From)
		for i, e := range m.Entries {
			if e.EntryType != pb.EntryType_EntryConfChange {
				continue
			}
			if r.PendingConfIndex > r.RaftLog.applied {
				m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
			} else {
				r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
			}
		}
		r.appendEntry(ptrSlice2entSlice(m.Entries)...)
		r.bcastAppend()
	case pb.MessageType_MsgAppendResponse:
		r.activePeer(m.From)
		r.handleAppendResp(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.activePeer(m.From)
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
}

func (r *Raft) bcastAppend() {
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendAppend(to)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.RaftLog.committed > m.Index {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   r.RaftLog.committed,
		})

		return
	}

	if mlastIndex, ok := r.RaftLog.maybeAppend(m); ok {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   mlastIndex,
		})

		return
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Index:   m.Index,
		Reject:  true,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// The election occurs in the term after the one we loaded with
	// lead's term and committed index setted up above.
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[r.quorum()-1]

	return r.RaftLog.maybeCommit(mci, r.Term)
}

func (r *Raft) syncCommit(m pb.Message) {
	if m.Commit <= r.RaftLog.committed {
		return
	}

	//lastNewEntryIndex := m.Index + uint64(len(m.Entries))
	lastNewEntryIndex := r.RaftLog.LastIndex()
	// if m.Commit > lastNewEntryIndex {
	// 	if lastNewEntryIndex < r.RaftLog.committed {
	// 		log.Errorf("fkking msg: %+v", m)
	// 		panic(fmt.Sprintf("violate monotonicity of committed index, commit: %v, idx: %v, len entries: %v, lastNewEntryIdx: %v", m.Commit, m.Index, len(m.Entries), lastNewEntryIndex))
	// 	}
	// 	r.RaftLog.committed = lastNewEntryIndex
	// } else {
	// 	r.RaftLog.committed = m.Commit
	// }
	r.RaftLog.committed = min(lastNewEntryIndex, m.Commit)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		log.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   r.RaftLog.LastIndex(),
		})
	} else {
		log.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   r.RaftLog.committed,
		})
	}

}

func (r *Raft) restore(s *pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		log.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}

	log.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	r.RaftLog.restore(s)

	r.Prs = make(map[uint64]*Progress)
	for _, pid := range s.Metadata.ConfState.Nodes {
		match, next := uint64(0), r.RaftLog.LastIndex()+1
		if pid == r.id {
			match = next - 1
		}
		r.setProgress(pid, match, next)
	}

	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = 0
	if _, ok := r.Prs[id]; ok {
		// Ignore any redundant addNode calls (which can happen because the
		// initial bootstrapping entries are applied twice).
		return
	}

	r.setProgress(id, uint64(0), r.RaftLog.LastIndex()+1)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).

	r.delProgress(id)
	r.PendingConfIndex = 0
	// do not try to commit or abort transferring
	// if there is no nodes in the cluster.
	if len(r.Prs) == 0 {
		return
	}

	// pending entries maybe commited
	// when a config change reduces the quorum requirements.
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// Abort leader transfer
	// If the removed node is the leadTransferee
	// Designed for liveness
	if r.State == StateLeader && r.leadTransferee == id {
		r.abortLeadTransfer()
	}
}

func (r *Raft) bcastReqVote() {
	li := r.RaftLog.LastIndex()
	lastLogTerm := r.RaftLog.lastTerm()
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		log.Debugf("%x [logterm: %d, index: %d] sent MessageType_MsgRequestVote request to %x at term %d",
			r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), to, r.Term)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      to,
			Term:    r.Term,
			LogTerm: lastLogTerm,
			Index:   li,
		})
	}
}

func (r *Raft) handleMsgHup() {
	if r.State == StateLeader {
		log.Debugf("%d ignoring MsgHup because already leader", r.id)
		return
	}
	if !r.promotable() {
		log.Warningf("%d is unpromotable and can not campaign", r.id)
		return
	}
	log.Debugf("%d is starting a new election at term %d", r.id, r.Term)
	r.becomeCandidate()

	if VoteWon == r.poll(r.id, true) {
		r.becomeLeader()
		return
	}
	r.bcastReqVote()
}

func (r *Raft) handleReqVote(m pb.Message) {
	canVote := r.Vote == m.From ||
		(r.Vote == None && r.Lead == None)
	if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Term:    m.Term,
		})
		r.electionElapsed = 0
		r.Vote = m.From
	} else {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
	}
}

func (r *Raft) readBallots(m pb.Message) (int, error) {
	if m.Term < r.Term {
		return 0, fmt.Errorf("ineffective ballot, msg term: %v, node term: %v", m.Term, r.Term)
	}
	if m.Term > r.Term {
		log.Error("invalid case while handling MessageType_MsgRequestVoteResponse",
			zap.Uint64("message term", m.Term),
			zap.Uint64("node term", r.Term),
		)
		// todo whether return error
		return 0, fmt.Errorf("invalid case while handling MessageType_MsgRequestVoteResponse, msg term: %v, node term: %v", m.Term, r.Term)
	}

	r.votes[m.From] = !m.Reject
	var cnt int
	for _, v := range r.votes {
		if v {
			cnt++
		}
	}

	return cnt, nil
}

func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) handleAppendResp(m pb.Message) {
	// todo what happened when meet failure
	pr := r.Prs[m.From]
	if m.Reject {
		if pr.maybeDecrTo(m.Index) {
			r.sendAppend(m.From)
		}
		return
	}

	if !pr.maybeUpdate(m.Index) {
		return
	}

	if r.maybeCommit() {
		r.bcastAppend()
	} else {
		r.maybeSendAppend(m.From, false)
		if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
			r.sendMsgTimeOutNow(m.From)
		}
	}
	// 	pr.Match = m.Index
	// 	pr.Next = m.Index + 1
	// 	if pr.Next < r.RaftLog.LastIndex() {
	// 		r.sendAppend(m.From)
	// 	}

	// // todo commit
	// commitFlag := r.commitLog(m)
	//
	//	if commitFlag {
	//		r.Step(pb.Message{
	//			MsgType: pb.MessageType_MsgPropose,
	//			To:      r.id,
	//			From:    r.id,
	//			Term:    r.Term,
	//		})
	//	}
}

func (r *Raft) commitLog(m pb.Message) bool {
	if m.Index <= r.RaftLog.committed {
		return false
	}

	matchIndex := make([]uint64, len(r.Prs))
	for _, proc := range r.Prs {
		matchIndex = append(matchIndex, proc.Match)
	}
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] > matchIndex[j]
	})
	index := matchIndex[len(r.Prs)>>1]
	if index <= r.RaftLog.committed {
		return false
	}
	if term, err := r.RaftLog.Term(index); err != nil || term != r.Term {
		return false
	}

	r.RaftLog.committed = index
	return true
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) RecordVote(id uint64, v bool) {
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = v
	}
}

func (r *Raft) TallyVotes() (result VoteResult) {
	var granted, missed, rejected int

	for id := range r.Prs {
		v, voted := r.votes[id]
		if !voted {
			missed++
		} else if v {
			granted++
		} else {
			rejected++
		}
	}
	q := len(r.Prs) >> 1
	if granted > q {
		return VoteWon
	}
	if granted+missed > q {
		return VotePending
	}

	return VoteLost
}

// manage peers progress

func (r *Raft) Visit(f func(id uint64, pr *Progress)) {
	n := len(r.Prs)
	// We need to sort the IDs and don't want to allocate since this is hot code.
	// The optimization here mirrors that in `(MajorityConfig).CommittedIndex`,
	// see there for details.
	var sl [7]uint64
	var ids []uint64
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]uint64, n)
	}
	for id := range r.Prs {
		n--
		ids[n] = id
	}
	insertionSort(ids)
	for _, id := range ids {
		f(id, r.Prs[id])
	}
}

func (r *Raft) poll(id uint64, v bool) (result VoteResult) {
	r.RecordVote(id, v)

	return r.TallyVotes()
}

func (r *Raft) promotable() bool {
	pr := r.Prs[r.id]

	return pr != nil
}

func (r *Raft) setProgress(id, match, next uint64) {
	r.Prs[id] = &Progress{Next: next, Match: match}
}

func (r *Raft) delProgress(id uint64) {
	delete(r.Prs, id)
}

func (r *Raft) ResetVotes() {
	r.votes = map[uint64]bool{}
}

// todo
func (r *Raft) appendEntry(es ...pb.Entry) (accepted bool) {
	offset := r.RaftLog.LastIndex() + 1
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = offset + uint64(i)
	}
	r.RaftLog.append(es...)

	li := r.RaftLog.LastIndex()
	if r.Prs[r.id].MaybeUpdate(li) {
		r.maybeCommit()
	}

	return true
}

// following fucntion about leader transfer
func (r *Raft) handleTransferLeader(m pb.Message) {
	leadTransferee := m.From
	prevLeadTransferee := r.leadTransferee
	if prevLeadTransferee != None {
		if leadTransferee == prevLeadTransferee {
			return
		}
		// todo
		r.abortLeadTransfer()
	}
	if leadTransferee == r.id {
		return
	}
	if _, ok := r.Prs[leadTransferee]; !ok {
		log.Warnf("[Raft.handleTransferLeader] %v not exists in current leader-%v peers", leadTransferee, r.id)
		return
	}

	r.leadTransferee = leadTransferee
	r.electionElapsed = 0

	if r.Prs[leadTransferee].Match == r.RaftLog.LastIndex() {
		r.sendMsgTimeOutNow(leadTransferee)
	} else {
		r.sendAppend(leadTransferee)
	}
}

func (r *Raft) abortLeadTransfer() {
	r.leadTransferee = None
}

func (r *Raft) sendMsgTimeOutNow(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
	})
}

func (r *Raft) handleMsgTimeoutNow(m pb.Message) {
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) checkQuorum() {
	if !r.checkQuorumActive() {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) checkQuorumActive() bool {
	var act int
	for id := range r.Prs {
		if id == r.id {
			act++
			continue
		}
		if r.Prs[id].RecentActive {
			act++
		}
		r.Prs[id].RecentActive = false
	}

	return act >= r.quorum()
}

func (r *Raft) activePeer(uid uint64) {
	if uid == 0 {
		return
	}
	pr, pok := r.Prs[uid]
	if !pok {
		log.Warnf("[Raft.activePeer] %d no peer %d", r.id, uid)
		return
	}
	pr.RecentActive = true
}
