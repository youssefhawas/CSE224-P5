package surfstore

import (
	context "context"
	"fmt"
	"sync"
)

type BlockStore struct {
	blockmap_lock sync.Mutex
	BlockMap      map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.blockmap_lock.Lock()
	if data, ok := bs.BlockMap[blockHash.Hash]; ok {
		bs.blockmap_lock.Unlock()
		return data, nil
	} else {
		bs.blockmap_lock.Unlock()
		return nil, fmt.Errorf("Block does not exist in blockmap")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashString := GetBlockHashString(block.BlockData)
	bs.blockmap_lock.Lock()
	bs.BlockMap[hashString] = block
	bs.blockmap_lock.Unlock()
	// WHEN WILL SUCCESS BE FALSE
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	block_hashes := []string{}
	for _, blockhash := range blockHashesIn.Hashes {
		bs.blockmap_lock.Lock()
		_, ok := bs.BlockMap[blockhash]
		bs.blockmap_lock.Unlock()
		if ok {
			block_hashes = append(block_hashes, blockhash)
		}
	}
	return &BlockHashes{Hashes: block_hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
