package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
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

	lastApplied int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	next_indices []int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) checkUp(server_ip string, up_channel chan bool) {
	for {
		_, err := grpc.Dial(server_ip, grpc.WithInsecure())
		if err != nil {
			continue
		}
		up_channel <- true
		return
	}
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	up_channel := make(chan bool)
	if s.isLeader {
		up_count := 1
		for _, ip := range s.ipList {
			if ip != s.ip {
				go s.checkUp(ip, up_channel)
			}
		}
		success := <-up_channel
		if success {
			up_count++
		}
		if up_count > len(s.ipList)/2 {
			return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
		}
	} else {
		return nil, ERR_NOT_LEADER
	}

	return nil, nil

}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	if s.isLeader {
		return s.metaStore.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	} else {
		return nil, ERR_NOT_LEADER
	}

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isLeader {
		op := UpdateOperation{
			Term:         s.term,
			FileMetaData: filemeta,
		}

		s.log = append(s.log, &op)
		committed := make(chan bool)
		s.pendingCommits = append(s.pendingCommits, committed)

		go s.attemptCommit()

		success := <-committed
		if success {
			s.lastApplied++
			return s.metaStore.UpdateFile(ctx, filemeta)
		}

		return nil, nil
	} else {
		return nil, ERR_NOT_LEADER
	}
}

func (s *RaftSurfstore) attemptCommit() {
	targetIdx := s.commitIndex + 1
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		// TODO handle crashed nodes
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			s.pendingCommits[targetIdx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		entryIdx = s.next_indices[serverIdx]
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		var prevlogterm int64
		if entryIdx-1 < 0 {
			prevlogterm = 0
		} else {
			prevlogterm = s.log[entryIdx-1].Term
		}
		// TODO create correct AppendEntryInput from s.nextIndex, etc
		fmt.Println(s.log[entryIdx:])
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevlogterm,
			PrevLogIndex: entryIdx - 1,
			Entries:      s.log[entryIdx:],
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output.Success {
			commitChan <- output
			return
		}
		// TODO update state. s.nextIndex, etc
		if !output.Success {
			s.next_indices[serverIdx] = s.next_indices[serverIdx] - 1
			continue
		}

		// TODO handle crashed/ non success cases
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	if input.Term > s.term {
		s.term = input.Term
		s.isLeader = false

	}

	if len(input.Entries) != 0 {
		if int64(len(s.log)) <= input.PrevLogIndex {
			return output, nil
		}

		//1. Reply false if term < currentTerm (§5.1)
		if input.Term < s.term {
			return output, nil
		}
		//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		//matches prevLogTerm (§5.3)
		if input.PrevLogIndex > 0 {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				return output, nil
			}
		}
		//3. If an existing entry conflicts with a new one (same index but different
		//terms), delete the existing entry and all that follow it (§5.3)
		s.log = s.log[:input.PrevLogIndex+1]
		//4. Append any new entries not already in the log
		s.log = append(s.log, input.Entries...)
	}
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	// TODO only do this if leaderCommit > commitIndex
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	output.Success = true

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.term++
	s.isLeader = true
	for id := 0; id < len(s.ipList); id++ {
		if id != int(s.serverId) {
			s.next_indices[id] = s.commitIndex + 1
		}
	}
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isLeader {
		for idx, addr := range s.ipList {
			if int64(idx) == s.serverId {
				continue
			}
			entryIdx := s.next_indices[idx]
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return nil, nil
			}
			client := NewRaftSurfstoreClient(conn)
			var prevlogterm int64
			if entryIdx-1 < 0 {
				prevlogterm = 0
			} else {
				prevlogterm = s.log[entryIdx-1].Term
			}
			// TODO create correct AppendEntryInput from s.nextIndex, etc
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevlogterm,
				PrevLogIndex: entryIdx - 1,
				// TODO figure out which entries to send
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			output, err := client.AppendEntries(ctx, input)
			if output != nil {
				// server is alive
			}
		}

		return &Success{Flag: true}, nil
	} else {
		return &Success{Flag: false}, nil
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
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
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
