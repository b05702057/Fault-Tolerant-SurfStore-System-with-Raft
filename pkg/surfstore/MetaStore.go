package surfstore

import (
	context "context"
	"errors"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap                  map[string]*FileMetaData
	BlockStoreAddr               string
	UnimplementedMetaStoreServer // required line by grpc
	mu                           sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("GetFileInfoMap")
	m.mu.Lock()
	defer m.mu.Unlock()
	output := FileInfoMap{}
	output.FileInfoMap = m.FileMetaMap
	PrintMetaMap(output.FileInfoMap)
	return &output, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Println("UpdateFile")
	log.Println(fileMetaData.Filename)
	m.mu.Lock()
	defer m.mu.Unlock()
	serverMetaData, ok := m.FileMetaMap[fileMetaData.Filename]
	clientVersion := fileMetaData.Version
	output := Version{Version: clientVersion}

	// The file already exists on the server.
	if ok {
		serverVersion := serverMetaData.Version
		// The server has a more updated version

		if clientVersion <= serverVersion { // wrong version
			output.Version = serverVersion
			return &output, errors.New("wrong version")
		}
	}
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData // update the meta data
	PrintMetaMap(m.FileMetaMap)

	return &output, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	log.Println("GetBlockStoreAddr")
	m.mu.Lock()
	defer m.mu.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
