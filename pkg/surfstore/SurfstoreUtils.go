package surfstore

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Pass an empty file to write the remote content
func DownloadFile(filePath string, client RPCClient, remoteVal *FileMetaData, localMap map[string]*FileMetaData, fileName string) {
	// Open the empty file
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	// Note that Create() truncates the file if it already exists.

	// Store the address in the varaible
	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Write blocks into the file
	blockHashes := remoteVal.BlockHashList
	var block Block
	for _, blockHash := range blockHashes {
		err := client.GetBlock(blockHash, blockStoreAddr, &block)
		if err != nil {
			log.Fatal(err)
		}

		_, err = f.Write(block.BlockData) // write the blocks into the file
		if err != nil {
			log.Fatal(err)
		}
	}

	// Update the local map
	localMap[fileName] = &FileMetaData{
		Filename:      fileName,
		Version:       remoteVal.Version,
		BlockHashList: blockHashes,
	}
}

// PutBlock() only adds the blocks to the server.
// UpdateFile then links the new blocks to the file.
func UploadFile(client RPCClient, fileName string, version int32, baseBlockMap map[string][]*Block, baseVal []string) error {
	var blockStoreAddr string
	succ := false // doesn't matter

	// Update the blocks
	err := client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Fatal(err)
	}
	blocks := baseBlockMap[fileName]
	for i := 0; i < len(blocks); i++ {
		block := blocks[i]
		err := client.PutBlock(block, blockStoreAddr, &succ)
		if err != nil {
			log.Fatal(err) // fail to put the block
		}
	}

	// Update the remote index
	metaData := &FileMetaData{
		Filename:      fileName,
		Version:       version,
		BlockHashList: baseVal,
	}
	err = client.UpdateFile(metaData, &metaData.Version)
	return err
}

// Test cases
// Base   Local   Remote   Result
// X      X       X        Do nothing
// X      X       O        Download the remote version and update index.txt
// X      O       X        Another client has already deleted the file. => Update local
// O      X       X        Add the file to the server and update index.txt
// X      O       O        New remote version => download it; same version => delete it
// O      X       O        Download the remote version and update index.txt
// O      O       X        Not possible
// O      O       O        New remote version => download it; same version => delete it

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	// Get BaseDir information
	baseDir := client.BaseDir
	baseMap := make(map[string][]string)      // fileName : hashList
	baseBlockMap := make(map[string][]*Block) // fileName : Block
	blockSize := client.BlockSize
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Subdirectories are not allowed.
		if file.IsDir() {
			log.Fatal("invalid subdirectory")
		}

		// The index file should not be included
		fileName := file.Name()
		filePath := ConcatPath(baseDir, fileName)
		if fileName == DEFAULT_META_FILENAME {
			continue
		}

		// Process the buffer
		buf, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatal(err)
		}
		byteNum := len(buf)
		blockNum := int(math.Ceil(float64(byteNum) / float64(blockSize))) // number of blocks needed

		// Initialize for each file to avoid shallow copy
		var hashStringList []string
		var blockList []*Block
		for i := 0; i < blockNum; i++ {
			lowerBound := i * blockSize
			upperBound := int(math.Min(float64((i+1)*blockSize), float64(byteNum)))
			curBlock := buf[lowerBound:upperBound]

			// Store the blocks of a file
			var baseBlock Block
			baseBlock.BlockSize = int32(blockSize)
			baseBlock.BlockData = curBlock
			blockList = append(blockList, &baseBlock)

			// Store the hashes of a file
			hashString := GetBlockHashString(curBlock)
			hashStringList = append(hashStringList, hashString)
		}
		baseMap[fileName] = hashStringList
		baseBlockMap[fileName] = blockList
	}

	// Get local index information
	localMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatal(err)
	}

	// Get remote index information
	remoteMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteMap)
	if err != nil {
		log.Fatal(err)
	}

	// Upload the files when:
	var version int32
	for fileName, baseVal := range baseMap {
		if fileName == DEFAULT_META_FILENAME {
			continue // ignore index.txt
		}

		localVal, localOk := localMap[fileName]
		// (1) The file is not in the local index.
		isNewFile := !localOk
		// (2) The client has an uncommited latest version.
		isModifiedFile := (localOk && !reflect.DeepEqual(localVal.BlockHashList, baseVal))

		if isNewFile || isModifiedFile {
			if isNewFile {
				version = 1 // the first version
			}
			if isModifiedFile {
				version = localVal.Version + 1 // the next version
			}
			// Note that if localOk, then remoteOk because the local index follows the remote index.

			// Upload the file
			err := UploadFile(client, fileName, version, baseBlockMap, baseVal)
			// Note that the version might be too old.

			// Update local information since the update is successful
			if err == nil {
				if !localOk { // new file
					var metaData FileMetaData
					localMap[fileName] = &metaData
				}
				localMap[fileName].Filename = fileName
				localMap[fileName].Version = version
				localMap[fileName].BlockHashList = baseVal
			}
			// We don't handle the error here.
			// Instead, we simply download the new version later.
		}
	}
	// Update index.txt and the remote map
	WriteMetaFile(localMap, baseDir)
	err = client.GetFileInfoMap(&remoteMap) // keep it updated with the server
	if err != nil {
		log.Fatal(err)
	}

	// Delete the remote file when the file is only in the local index
	for fileName, localVal := range localMap {
		_, baseOk := baseMap[fileName]

		// The hash list of the file and the local index is different.
		if !baseOk && !reflect.DeepEqual(localVal.BlockHashList, []string{"0"}) {
			baseBlockMap[fileName] = []*Block{}
			version = localVal.Version + 1 // may fail
			err = UploadFile(client, fileName, version, baseBlockMap, []string{"0"})
			if err == nil {
				localVal.Version = version
				localVal.BlockHashList = []string{"0"}
			}
		}
		// Note that we view []string{"0"} as the hash list of a deleted file.
	}
	// Update index.txt and the remote map
	WriteMetaFile(localMap, baseDir)
	err = client.GetFileInfoMap(&remoteMap)
	if err != nil {
		log.Fatal(err)
	}

	// Download the files when:
	for fileName, remoteVal := range remoteMap {
		filePath := ConcatPath(baseDir, fileName)
		_, baseOk := baseMap[fileName]
		localVal, localOk := localMap[fileName]

		// (1) The file is not in index.txt.
		isNewFile := !localOk
		// (2) The lastest version is on the server.
		isModifiedFile := localOk && remoteVal.Version > localVal.Version

		if isNewFile || isModifiedFile {
			// The server has a deleted version.
			if reflect.DeepEqual(remoteVal.BlockHashList, []string{"0"}) {
				if baseOk {
					err := os.Remove(filePath)
					if err != nil {
						log.Fatal(err)
					}
				}

				// Update the local index
				localMap[fileName] = &FileMetaData{
					Filename:      fileName,
					Version:       remoteVal.Version,
					BlockHashList: []string{"0"},
				}
			} else {
				// Download the remote file and update the index file
				DownloadFile(filePath, client, remoteVal, localMap, fileName)
			}
		}
	}
	// Update index.txt
	WriteMetaFile(localMap, baseDir)
}
