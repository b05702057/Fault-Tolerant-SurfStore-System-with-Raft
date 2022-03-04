package SurfTest

import (
	context "context"
	"cse224/proj5/pkg/surfstore"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}
	// Only the leader would send the heartbeat eventually.

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(1) {
			t.Logf("Server %d should be in term %d", idx, 1)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state.Term != int64(2) {
			t.Logf("Server should be in term %d", 2)
			t.Fail()
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Logf("Server %d should be the leader", idx)
				t.Fail()
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Logf("Server %d should not be the leader", idx)
				t.Fail()
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)

	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

// leader1 gets a request while all other nodes are crashed.
// the crashed nodes recover.
func TestRaftRecoverable(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)

	//TEST
	leaderIdx := 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// leader1 gets a request while all other nodes are crashed.
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)

	// the crashed nodes recover.
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// The log should be replicated and applied
	goldenMeta := surfstore.NewMetaStore("")
	goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	for _, server := range test.Clients {
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Log(state.Log)
		if !SameLog(goldenLog, state.Log) {
			t.Log("Logs do not match")
			t.Fail()
		}
		if !SameMeta(goldenMeta.FileMetaMap, state.MetaMap.FileInfoMap) {
			t.Log("MetaStore state is not correct")
			t.Fail()
		}
	}
}

// leader1 gets several requests while all other nodes are crashed.
// leader1 crashes.
// all other nodes are restored.
// leader2 gets a request.
// leader1 is restored.
// Result: Leader should have 2 log entries
// func TestRaftLogsCorrectlyOverwritte(t *testing.T) {
// 	//Setup
// 	cfgPath := "./config_files/3nodes.txt"
// 	test := InitTest(cfgPath, "8080")
// 	defer EndTest(test)

// 	//TEST
// 	leaderIdx := 1
// 	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
// 	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

// 	// leader1 gets several requests while all other nodes are crashed.
// 	filemeta1 := &surfstore.FileMetaData{
// 		Filename:      "testFile1",
// 		Version:       1,
// 		BlockHashList: nil,
// 	}
// 	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta1)

// 	filemeta2 := &surfstore.FileMetaData{
// 		Filename:      "testFile2",
// 		Version:       1,
// 		BlockHashList: nil,
// 	}
// 	go test.Clients[leaderIdx].UpdateFile(context.Background(), filemeta2)

// 	// leader1 crashes.
// 	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

// 	// all other nodes are restored.
// 	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

// }
