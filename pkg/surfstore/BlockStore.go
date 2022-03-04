package surfstore

import (
	context "context"
	"errors"
	"log"
	sync "sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mu sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	log.Println("GetBlock")
	bs.mu.Lock()
	defer bs.mu.Unlock()
	block, ok := bs.BlockMap[blockHash.Hash] // able to use "." for pointers in Go
	if ok {
		return block, nil
	}
	return block, errors.New("wrong hash")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	log.Println("PutBlock")
	bs.mu.Lock()
	defer bs.mu.Unlock()
	hashString := GetBlockHashString(block.BlockData)
	bs.BlockMap[hashString] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”,
// returns a list containing the subset of "in" that are stored in the key-value store.
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	log.Println("HasBlocks")
	bs.mu.Lock()
	defer bs.mu.Unlock()
	output := BlockHashes{}
	for _, hashString := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[hashString]
		if ok {
			output.Hashes = append(output.Hashes, hashString)
		}
	}
	return &output, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
