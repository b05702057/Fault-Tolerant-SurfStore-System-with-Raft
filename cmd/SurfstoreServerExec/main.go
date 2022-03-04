package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Get the error line
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output() // returns the destination for usage and error messages (default: os.Stderr)
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) { // show the user all flag usages
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags (flag, deafult, usage)
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both") // the address of a string
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddr := ""
	if len(args) == 1 { // the number of arguments after the flag
		blockStoreAddr = args[0]
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok { // should be meta, block, or both.
		flag.Usage() // print the usage message
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// Start a server with the arguments
	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddr))
}

func startServer(hostAddr string, serviceType string, blockStoreAddr string) error {
	// Create a new RPC server that has no service registered and hasn't started to accept requests
	grpcServer := grpc.NewServer()

	// Register RPC services
	if serviceType == "both" || serviceType == "block" {
		blocklStore := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(grpcServer, blocklStore)
	}
	if serviceType == "both" || serviceType == "meta" {
		metaStore := surfstore.NewMetaStore(blockStoreAddr)
		surfstore.RegisterMetaStoreServer(grpcServer, metaStore)
	}

	// Start listening and serving
	lis, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	if err := grpcServer.Serve(lis); err != nil { // fail to accept incoming connections
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
