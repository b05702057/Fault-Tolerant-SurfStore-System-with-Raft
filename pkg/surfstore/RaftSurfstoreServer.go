package surfstore

import (
	context "context"
	"math"
	"sync"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	// isLeaderCond  *sync.Cond

	// volatile leader state
	nextIndex  []int
	matchIndex []int

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	// notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func IsLeader(s *RaftSurfstore) bool {
	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return false
	}
	s.isLeaderMutex.Unlock()
	return true
}

func MajorityWork(s *RaftSurfstore, ctx context.Context, empty *emptypb.Empty) bool {
	workNum := 0
	for idx := range s.ipList {
		client := s.rpcClients[idx]
		crashedState, _ := client.IsCrashed(ctx, empty)
		if !crashedState.IsCrashed {
			workNum += 1
		}
	}
	return workNum > len(s.ipList)/2
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// check if the server is the leader
	if !IsLeader(s) {
		return nil, ERR_NOT_LEADER
	}

	// The server is crashed.
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()
	// Note that a server may crash and still consider itself a leader.

	// check if a mjority of the nodes work
	for {
		// block until a majority of the servers work
		if MajorityWork(s, ctx, empty) {
			return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// check if the server is the leader
	if !IsLeader(s) {
		return nil, ERR_NOT_LEADER
	}

	// The server is crashed.
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()
	// Note that a server may crash and still consider itself a leader.

	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// check if the server is the leader
	if !IsLeader(s) {
		return nil, ERR_NOT_LEADER
	}

	// The server is crashed.
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()
	// Note that a server may crash and still consider itself a leader.

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	commited := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, commited)

	// Try commiting the entries
	go s.CommitEntries() // use go routine because the below channel would block until it is received (dead lock)

	// The code resumes when it get the message.
	success := <-commited // block until a majority of nodes work
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different terms),
// delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	// The follower is crashed.
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return output, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	// Reply false if term < currentTerm (wrong leader)
	if input.Term < s.term {
		// Another leader is selected.
		return output, nil
	}

	// Make sure the server is a follower
	s.isLeaderMutex.Lock()
	s.isLeader = false
	s.isLeaderMutex.Unlock()

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if input.PrevLogIndex >= 0 { // no need to inspect otherwise
		// The follower's log is too short or the previos terms are not identical.
		if input.PrevLogIndex >= int64(len(s.log)) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			return output, nil
		}
	}

	// The previous log entries are identical, so we should:
	// (1) Delete the following entries
	if int64(len(s.log)) > input.PrevLogIndex+1 {
		s.log = s.log[:input.PrevLogIndex+1]
	}
	// (2) Append the new entries
	s.log = append(s.log, input.Entries[input.PrevLogIndex+1:]...)

	// Update the term of the follower
	s.term = input.Term

	// Update the commited index for the follower
	if s.commitIndex < input.LeaderCommit {
		newCommitIndex := int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
		// Apply the entries to the state machine
		for i := s.commitIndex + 1; i <= newCommitIndex; i++ {
			s.metaStore.UpdateFile(ctx, s.log[i].FileMetaData)
		}
		// Update the commit index
		s.commitIndex = newCommitIndex
	}

	// Update the output
	output.ServerId = s.serverId
	output.Term = s.term
	output.Success = true
	output.MatchedIndex = int64(len(s.log) - 1) // same entries
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	success := &Success{Flag: false}

	// check if the server is crashed
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return success, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	// Make it the leader
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	s.term++ // a new term starts with a new leader

	// If the server is a new leader, its pendingCommit may be empty, but the commitIdx may > -1
	for i := len(s.pendingCommits); i < int(s.commitIndex)+1; i++ {
		commited := make(chan bool)
		s.pendingCommits = append(s.pendingCommits, commited) // make pendingCommits long enough
	}

	// Initialize with empty lists
	var nextIndex [0]int
	var matchIndex [0]int
	s.nextIndex = nextIndex[:]
	s.matchIndex = matchIndex[:]
	for range s.ipList {
		s.nextIndex = append(s.nextIndex, len(s.log)) // initialized to the leader's last log index + 1
		s.matchIndex = append(s.matchIndex, -1)       // initialized to -1 (increases monotonically)
	}
	success.Flag = true
	return success, nil
}

// Send a 'Heartbeat" to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	success := &Success{Flag: false}

	// The server is crashed.
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return success, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	// The server is not a leader.
	if !IsLeader(s) {
		return success, ERR_NOT_LEADER
	}

	for idx := range s.ipList {
		if int64(idx) == s.serverId {
			continue // skip the leader itself
		}
		client := s.rpcClients[idx]

		// Create correct AppendEntryInput from s.nextIndex, etc
		prevLogIndex := int64(s.matchIndex[idx])
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = int(s.log[prevLogIndex].Term)
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  int64(prevLogTerm),
			PrevLogIndex: prevLogIndex,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output != nil && output.Success { // nil if the follower crashes
			s.nextIndex[idx] += int(output.MatchedIndex + 1)
			s.matchIndex[idx] = int(output.MatchedIndex)
		}
	}
	success.Flag = true
	return success, nil
}

// Commit all available entries
func (s *RaftSurfstore) CommitEntries() {
	// Replicate until all entries are commited
	for {
		// There is no new entries to replicate.
		if s.commitIndex == int64(len(s.log)-1) {
			break
		}

		targetIdx := s.commitIndex + 1 // the next index
		commitChan := make(chan *AppendEntryOutput, len(s.ipList))
		for idx := range s.ipList {
			// No need to commit the leader it self
			if idx == int(s.serverId) {
				continue // cause metastore version error if the UpdateFile function is called twice
			}

			// There are entries to be commited
			if targetIdx > int64(s.matchIndex[idx]) { // targetIdx >= server's nextIndex
				go s.commitEntry(int64(idx), targetIdx, commitChan) // check if the entry is committed with the channel
			}
		}

		commitCount := 1 // the leader itself
		for {
			commit := <-commitChan // get AppendEntryOutput
			if commit != nil && commit.Success {
				commitCount++
			}

			if commitCount > len(s.ipList)/2 { // committed
				s.pendingCommits[targetIdx] <- true
				s.commitIndex = targetIdx
				break // go for the next entry
			}
		}
	}

}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	// Set the target index
	s.nextIndex[serverIdx] = int(entryIdx)

	// Try until the entry is replicated
	for {
		// Get the client (The grpc.Dial part is set up in the testing code.)
		client := s.rpcClients[serverIdx]

		// Create correct AppendEntryInput from s.nextIndex, etc
		prevLogIndex := int64(s.nextIndex[serverIdx]) - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 { // prevLogIndex is -1 initially
			prevLogTerm = int(s.log[prevLogIndex].Term)
		}

		input := &AppendEntryInput{
			Term:         s.term,             // the current term of the leader
			PrevLogIndex: prevLogIndex,       // the log index immediately preceding new ones
			PrevLogTerm:  int64(prevLogTerm), // the log term of the prvious entry
			Entries:      s.log,              // the log entries of the leader
		}

		// Perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)

		// Update the state (s.nextIndex, etc)
		if output != nil && output.Success { // nil if the follower crashes
			commitChan <- output
			s.nextIndex[serverIdx] += int(output.MatchedIndex + 1)
			s.matchIndex[serverIdx] = int(output.MatchedIndex)
			return
		} else {
			if err == ERR_SERVER_CRASHED {
				continue // try until the server restore
			}
			s.nextIndex[serverIdx] -= 1 // encounter a conflict
		}
	}
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	// s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	crashedState := &CrashedState{}
	s.isCrashedMutex.Lock()
	crashedState.IsCrashed = s.isCrashed
	s.isCrashedMutex.Unlock()

	return crashedState, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
