package SurfTest

import (
	"os"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp() // clean the directory when the test is done
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt" // test0/multi_file1.txt
	file2 := "multi_file1.txt" // test1/multi_file1.txt
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

func TestSyncTwoClientsDeleteConflict(t *testing.T) {
	t.Logf("client1 creates and syncs. client2 syncs, deletes and syncs. client1 modifies and syncs. client2 syncs.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//client1 creates file
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 deletes file
	worker2.DeleteFile(file1)

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client1 modifies file
	err = worker1.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	workingDir, _ := os.Getwd()

	//check client1 index.txt
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	// Load metadata from index.txt
	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}

	// check we only have 1 file
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}

	// check that file is version 2
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	// check file has tombstone hash
	if !IsTombHashList(fileMeta1[file1].BlockHashList) {
		t.Fatalf("File should have tombstone hash")
	}

	// TODO maybe better to check that the file does not exist in the map
	// check that file actually got deleted
	fileList1 := worker1.ListAllFile()
	t.Log(fileList1)
	if len(fileList1) != 1 {
		t.Fatalf("Directory should have no files")
	}

	//check client2 index.txt
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	// Load metadata from index.txt
	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}

	// check we only have 1 file
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}

	// check that file is version 2
	if fileMeta2[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	// check file has tombstone hash
	if !IsTombHashList(fileMeta2[file1].BlockHashList) {
		t.Fatalf("File should have tombstone hash")
	}

	// TODO maybe better to check that the file does not exist in the map
	// check that file actually got deleted
	fileList2 := worker2.ListAllFile()
	if len(fileList2) != 1 {
		t.Fatalf("Directory should have no files")
	}
}

func TestSyncTwoClientsFileUpdateLeaderFailure(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs. leader change. client2 sync with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath, "8080")
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp() // clean the directory when the test is done
	defer worker2.CleanUp()

	//client1 add a file
	file1 := "multi_file1.txt" // test0/multi_file1.txt
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}

	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// leader change
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs (different content)
	err = worker2.UpdateFile(file1, "update text")
	if err != nil {
		t.FailNow()
	}
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client1 syncs again
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromMetaFile(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromMetaFile(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta1[file1].Version != 2 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}
}
