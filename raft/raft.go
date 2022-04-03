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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
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
	Match, Next uint64
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
	r := &Raft{
		id: c.ID,
		RaftLog: &RaftLog{
			storage:         c.Storage,
			committed:       0,
			applied:         0,
			stabled:         0,
			entries:         []pb.Entry{},
			pendingSnapshot: &pb.Snapshot{},
		},
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		msgs:             []pb.Message{},
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	for _, peer := range c.peers {
		r.votes[peer] = false
	}

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: 0,
		Index:   0,
		Entries: []*pb.Entry{
			{
				EntryType: 0,
				Term:      0,
				Index:     0,
				Data:      nil,
			},
		},
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: 0,
		Index:   0,
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
	if r.heartbeatElapsed < r.heartbeatTimeout {
		return
	}

	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
	})
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term = term
	r.Lead = lead
	// todo whether add if
	if lead == None {
		r.Vote = lead
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term++
	r.Lead = None
	r.Vote = r.id

	for peer := range r.votes {
		r.votes[peer] = false
	}
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	// TODO noop entry
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleReqVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleReqVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		cnt, err := r.readBallots(m)
		if err != nil {
			return
		}
		if (cnt << 1) > len(r.votes) {
			r.becomeLeader()
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleMsgBeat()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleReqVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return
	}
	r.becomeFollower(m.Term, m.From)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return
	}
	r.becomeFollower(m.Term, m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) bcastReqVote() {
	for to := range r.votes {
		if to == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      to,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

func (r *Raft) handleMsgHup() {
	r.becomeCandidate()
	if len(r.votes) == 1 {
		r.becomeLeader()
		return
	}
	r.bcastReqVote()
}

func (r *Raft) handleReqVote(m pb.Message) {
	var reject bool
	if m.Term < r.Term {
		reject = true
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Vote != None && r.Vote != m.From {
		reject = true
	}

	if r.Vote == None {
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
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

func (r *Raft) handleMsgBeat() {
	r.heartbeatElapsed = 0
	for peer := range r.votes {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}
