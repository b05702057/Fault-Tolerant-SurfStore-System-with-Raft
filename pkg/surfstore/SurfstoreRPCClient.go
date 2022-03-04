package surfstore

import (
	context "context"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

// This is already implemented as an example.
func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// Connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// Perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // get the context to manually release the resource
	defer cancel()                                                        // release the resource before the timeout
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})                // get the block with the block hash
	if err != nil {
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	return nil
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// Connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// Perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.PutBlock(ctx, block)
	return err
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// Connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// Perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input := BlockHashes{}
	input.Hashes = blockHashesIn
	output, err := c.HasBlocks(ctx, &input)
	if err == nil {
		*blockHashesOut = output.Hashes
	}
	return err
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// Connect to the leader
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// Perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			continue // try next server
		}
		*serverFileInfoMap = fileInfoMap.FileInfoMap // get the map successfully
		return nil
	}
	return ERR_NOT_LEADER // cannot find a leader at all (not possible in the test cases)
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// Connect to the leader
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// Perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			continue // try next server (not leader/crashed)
		}
		return nil
	}
	return ERR_NOT_LEADER // cannot find a leader at all (not possible in the test cases)
}

// GetBlockStoreAddr is a method from the MetaStore service
func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// Connect to the server
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// Perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if err != nil {
			continue // try next server
		}
		*blockStoreAddr = addr.Addr
		return nil
	}
	return ERR_NOT_LEADER
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
