package raft

type VoteResult uint8

const (
	VotePending VoteResult = 1 + iota
	VoteLost
	VoteWon
)

type ProgressTracker struct {
	// log replication progress of each peers
	Progress map[uint64]*Progress
	// votes records
	Votes map[uint64]bool
}

func (p *ProgressTracker) RecordVote(id uint64, v bool) {
	_, ok := p.Votes[id]
	if !ok {
		p.Votes[id] = v
	}
}

func (p *ProgressTracker) TallyVotes() (result VoteResult) {
	var granted, missed, rejected int

	for id := range p.Progress {
		v, voted := p.Votes[id]
		if !voted {
			missed++
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	q := len(p.Progress) >> 1
	if granted > q {
		return VoteWon
	}
	if granted+missed > q {
		return VotePending
	}

	return VoteLost
}

func (p *Progress) MaybeUpdate(n uint64) (updated bool) {
	if p.Match < n {
		p.Match = n
		updated = true
	}
	p.Next = max(p.Next, n+1)

	return updated
}

func (p *Progress) maybeDecrTo(rejected uint64) bool {
	if p.Next-1 != rejected {
		return false
	}

	p.Next = max(rejected, 1)

	return true
}

func (p *Progress) maybeUpdate(n uint64) (updated bool) {
	if p.Match < n {
		p.Match = n
		updated = true
	}
	if p.Next < n+1 {
		p.Next = n + 1
	}

	return
}
