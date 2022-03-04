package surfstore

import (
	"bufio"
	"net"

	//	"google.golang.org/grpc"
	"io"
	"log"

	//	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	// Read until the end of the file
	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("Client: ", "Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount) // no need to specify when len(x) == cap(x)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here
	isCrashedMutex := &sync.RWMutex{}

	clients := make([]RaftSurfstoreClient, 0)
	for _, ip := range ips {
		conn, err := grpc.Dial(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal("Error connecting to clients ", err)
		}
		client := NewRaftSurfstoreClient(conn)
		clients = append(clients, client)
	}

	server := RaftSurfstore{
		// TODO initialize any fields you add here
		ip:       ips[id],
		ipList:   ips,
		serverId: id,

		commitIndex: -1,

		isLeader:  false,
		term:      0,
		metaStore: NewMetaStore(blockStoreAddr),
		log:       make([]*UpdateOperation, 0),
		isCrashed: false,
		// notCrashedCond: sync.NewCond(isCrashedMutex),
		isCrashedMutex: isCrashedMutex,

		rpcClients: clients,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	l, e := net.Listen("tcp", server.ip)
	if e != nil {
		return e
	}

	// no need to run the background process anymore
	// go server.commitWorker()
	return s.Serve(l)
}
