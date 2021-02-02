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
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
	electionTimeout         int
	originalElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	transferElapsed int

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
		log.Panic(err.Error())
	}

	hs, cfs, _ := c.Storage.InitialState()

	peers := c.peers

	if peers == nil {
		peers = cfs.Nodes
	}

	raft := Raft{
		id:                      c.ID,
		Prs:                     make(map[uint64]*Progress),
		Term:                    hs.Term,
		State:                   StateFollower,
		Vote:                    hs.Vote,
		votes:                   make(map[uint64]bool),
		heartbeatTimeout:        c.HeartbeatTick,
		electionTimeout:         c.ElectionTick + rand.Intn(c.ElectionTick),
		originalElectionTimeout: c.ElectionTick,
		RaftLog:                 newLog(c.Storage),
	}

	raft.RaftLog.committed = hs.Commit

	fi, _ := raft.RaftLog.storage.FirstIndex()
	li := raft.RaftLog.LastIndex()
	for _, v := range peers {
		raft.Prs[v] = &Progress{fi - 1, li}
	}

	if c.Applied != 0 {
		raft.RaftLog.applied = c.Applied
	}

	return &raft
}
func (r *Raft) send(m pb.Message) {
	/*
		if m.To == 0 {
			fmt.Println(m)
			log.Panic("invalid msg to 0")
		}
	*/
	r.msgs = append(r.msgs, m)
}

func (r *Raft) GetId() uint64 {
	return r.id
}
func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendSnapshot(to uint64) {
	s, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.send(pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Index:    s.Metadata.Index,
		LogTerm:  s.Metadata.Term,
		Snapshot: &s,
	})
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	////fmt.Println("push msg to ", to)
	//debug.PrintStack()
	// Your Code Here (2A).
	next := r.Prs[to].Next
	index := next - 1
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		} else {
			log.Panic(err, r.RaftLog.FirstIndex(), next, r.RaftLog.LastIndex())
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		LogTerm: logTerm,
		Commit:  r.RaftLog.committed,
		Entries: r.RaftLog.EntriesWithPointers(next, r.RaftLog.LastIndex()+1),
	}
	r.send(msg)
	return true
}

func (r *Raft) bcastAppend() {
	for to := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) sendRequestVote(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
			if len(r.votes) < (len(r.Prs)+1)/2 {
				log.Info("stale leader on leader")
				r.becomeFollower(r.Term, 0)
			} else {
				r.votes = make(map[uint64]bool)
				r.votes[r.id] = true
			}
		}
		if r.leadTransferee != 0 {
			r.transferElapsed++
			if r.transferElapsed >= 2*r.originalElectionTimeout {
				r.leadTransferee = 0
				r.transferElapsed = 0
			}
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if r.State == StateLeader {
		log.Debug(r.id, "become follower")
		//debug.PrintStack()
	}
	// Your Code Here (2A).
	if r.State == StateFollower && r.Term == term && r.Lead == lead {
		return
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.leadTransferee = 0
	r.Vote = lead
	r.electionTimeout = r.originalElectionTimeout + rand.Intn(r.originalElectionTimeout)
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.electionTimeout = r.originalElectionTimeout + rand.Intn(r.originalElectionTimeout)
	r.electionElapsed = 0

	r.votes = make(map[uint64]bool)

	r.votes[r.id] = true
	r.Vote = r.id
	r.Lead = 0
	r.leadTransferee = 0
}

func (r *Raft) bcastRequstVote() {
	for to := range r.Prs {
		if to != r.id {
			r.sendRequestVote(to)
		}
	}
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	} else {
		r.bcastRequstVote()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	log.Debug(r.id, "become leader")
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	for to := range r.Prs {
		r.votes[to] = true // avoid become follower at the first beat
	}

	r.Lead = r.id
	r.leadTransferee = 0
	for k := range r.Prs {
		if k == r.id {
			r.Prs[k] = &Progress{r.RaftLog.LastIndex(), r.RaftLog.LastIndex() + 1}
		} else {
			r.Prs[k].Next = r.RaftLog.LastIndex() + 1
		}
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	if !IsLocalMsg(m.MsgType) {
		/*
			if _, ok := r.Prs[m.From]; !ok && m.From != 0 {
				return nil
			}
		*/
		if m.Term < r.Term {
			// should reject
		} else if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}

		if m.Commit > r.RaftLog.committed && m.From == r.Lead {
			r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
			log.Debug(r.id, "commit", r.RaftLog.committed)
		}
	}

	if m.MsgType == pb.MessageType_MsgSnapshot {
		r.handleSnapshot(m)
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHup:
			r.campaign()
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransfer(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHup:
			r.campaign()
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransfer(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.handleBeat()
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m) // must reject
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
			r.sendAppend(m.From)
		case pb.MessageType_MsgAppend:
			r.rejectMessage(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransfer(m)
		}
	}
	return nil
}

func (r *Raft) handleTransfer(m pb.Message) {
	if r.State != StateLeader {
		m.To = r.Lead
		r.send(m)
		return
	}
	if m.From == r.id {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}

	r.leadTransferee = m.From
	r.transferElapsed = 0
	r.sendAppend(m.From)
}

func (r *Raft) handleBeat() {
	for to := range r.Prs {
		if to != r.id {
			r.sendHeartbeat(to)
			/*
				if r.Prs[to].Next == 1 {
					r.sendSnapshot(to)
				}
			*/
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.rejectMessage(m)
		return
	}
	if r.Lead == 0 {
		r.becomeFollower(m.Term, m.From)
	}
	r.electionElapsed = 0

	rsp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Index:   m.Index,
		Term:    r.Term,
		Reject:  m.Term < r.Term,
	}

	t, err := r.RaftLog.Term(m.Index)

	if err != nil || t != m.LogTerm {
		rsp.Reject = true
	} else {
		r.RaftLog.Append(m.Entries...)
		rsp.Index = r.RaftLog.LastIndex()

		for _, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				cc := &pb.ConfChange{}
				cc.Unmarshal(e.Data)
			}
		}

		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		}
	}

	r.send(rsp)
}

func (r *Raft) maybeCommit() {
	if r.State != StateLeader {
		return
	}
	// Update commited
	mr := make([]int, 0, len(r.Prs))
	for _, v := range r.Prs {
		mr = append(mr, int(v.Match))
	}

	sort.Slice(mr, func(i, j int) bool {
		return mr[i] > mr[j]
	})
	commit := uint64(mr[len(mr)/2])
	/*
		if r.RaftLog.committed > commit {
			fmt.Println(r.RaftLog.entries)
			fmt.Println(m, mr, len(r.Prs), commit)
			fmt.Println(r.RaftLog.committed, commit)
			log.Panic("commit regression")
		}
	*/
	log.Debug("commit:", commit, mr)

	currentTerm, _ := r.RaftLog.Term(commit)
	if currentTerm == r.Term && commit > r.RaftLog.committed {
		r.RaftLog.committed = commit
		r.bcastAppend()
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if m.Reject == true {
		r.Prs[m.From].Next = max(m.Index, r.Prs[m.From].Match+1)
		if r.Prs[m.From].Next <= 0 {
			log.Panic(&m, "invalid match")
		}
		r.sendAppend(m.From)
	} else {
		if m.Index < r.Prs[m.From].Match {
			log.Info("stale rsp", m.Index, r.Prs[m.From].Next)
			return
		}
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.maybeCommit()
		if r.leadTransferee == m.From && r.Prs[m.From].Next == r.RaftLog.LastIndex()+1 {
			r.sendTimeoutNow(m.From)
			r.leadTransferee = 0
		}
	}
}

func (r *Raft) rejectMessage(m pb.Message) {
	var mt pb.MessageType
	if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
		mt = pb.MessageType_MsgAppendResponse
	} else if m.MsgType == pb.MessageType_MsgHeartbeat {
		mt = pb.MessageType_MsgHeartbeatResponse
	} else if m.MsgType == pb.MessageType_MsgRequestVote {
		mt = pb.MessageType_MsgRequestVoteResponse
	} else {
		log.Warn("can't reject msg ", m)
	}

	r.send(pb.Message{
		MsgType: mt,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  true,
	})
}

func (r *Raft) handlePropose(m pb.Message) {
	//fmt.Println("propose change")
	if r.leadTransferee != 0 {
		return
	}
	for i, ent := range m.Entries {
		ent.Index = r.RaftLog.LastIndex() + uint64(i) + 1
		ent.Term = r.Term
		if ent.EntryType == pb.EntryType_EntryConfChange {
			if len(m.Entries) != 1 {
				log.Panic("long confchange")
			}
			if r.RaftLog.applied >= r.PendingConfIndex {
				r.PendingConfIndex = ent.Index
			} else {
				// assume there's only one entry and it's confchange
				log.Info("deny conf")
				return
			}
		}
	}
	r.RaftLog.Append(m.Entries...)
	r.Prs[r.id] = &Progress{r.RaftLog.LastIndex(), r.RaftLog.LastIndex() + 1}
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed++
	}
}

// handleHeartbeat handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	rsp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}

	if r.State == StateFollower {
		upToDate := false
		if m.Term >= r.Term {
			if m.LogTerm > r.RaftLog.LastTerm() {
				upToDate = true
			} else if m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex() {
				upToDate = true
			}
		}

		if (r.Vote == 0 || r.Vote == m.From) && upToDate {
			r.Vote = m.From
			rsp.Reject = false
			//r.electionElapsed = 0
		}
	}
	r.send(rsp)
}

func (r *Raft) maybeEndCampaign() {
	if r.State != StateCandidate {
		return
	}
	count := 0
	for _, v := range r.votes {
		if v == true {
			count++
		}
	}
	if count > len(r.Prs)/2 {
		r.becomeLeader()
	} else if len(r.Prs)-len(r.votes)+count <= len(r.Prs)/2 {
		r.becomeFollower(r.Term, 0)
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}

	r.votes[m.From] = !m.Reject
	r.maybeEndCampaign()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//log.Info(r.id, "<-", m.From)
	if m.Term < r.Term {
		r.rejectMessage(m)
		return
	}
	if r.Lead == 0 {
		r.becomeFollower(m.Term, m.From)
	}

	r.electionElapsed = 0

	rsp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      r.Lead,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}

	r.send(rsp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if r.State == StateLeader {
		r.votes[m.From] = true
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term < r.Term || m.Index < r.RaftLog.committed {
		r.rejectMessage(m)
		return
	}
	if m.Index < r.RaftLog.committed {
		r.rejectMessage(m)
		return
	}
	s := m.Snapshot
	sm := s.Metadata
	r.becomeFollower(max(r.Term, sm.Term), m.From)
	r.RaftLog.entries = r.RaftLog.entries[:0]
	r.RaftLog.committed = sm.Index
	r.RaftLog.applied = sm.Index
	r.RaftLog.stabled = sm.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range sm.ConfState.Nodes {
		if uint64(peer) == r.id {
			r.Prs[uint64(peer)] = &Progress{r.RaftLog.LastIndex(), r.RaftLog.LastIndex() + 1}
		} else {
			r.Prs[uint64(peer)] = &Progress{sm.Index, r.RaftLog.LastIndex() + 1}
		}
	}
	r.RaftLog.pendingSnapshot = s
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	log.Debug(r.id, "add", id)
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{0, 1}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	log.Debug(r.id, "remove", id)
	r.maybeCommit()
	r.maybeEndCampaign()
}
