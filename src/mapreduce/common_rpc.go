package mapreduce

import (
	"fmt"
	"net/rpc"
)

// What follows are RPC types and methods.
// Field names must start with capital letters, otherwise RPC will break.

// 
type MapFileLocation struct {
	Address string
	File    string
}

// DoTaskArgs holds the arguments that are passed to a worker when a job is
// scheduled on it.
type DoTaskArgs struct {
	JobName    string
	File       string   // only for map, the input file
	Phase      jobPhase // are we in mapPhase or reducePhase?
	TaskNumber int      // this task's index in the current phase

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int

	// The map about the location of intermediate file the reduce risk required
	MapFileAddressByFile map[string]string
}

// return the location of intermediate files to master
type TaskReply struct {
	Files []string 
	MapOrReduceFail bool  // map: 0, reduce: 1
	address string // address of the probably failed worker
}

type FileArgs struct {
	File string
}

type FileReply struct {
	File    string
	Content []byte
}


// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string // the worker's UNIX-domain socket name, i.e. its RPC address
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// call() returns true if the server responded, and false if call()
// received no reply from the server. reply's contents are valid if
// and only if call() returned true.
//
// you should assume that call() will time out and return
// false after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs. please don't change this
// function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type FileFetcher struct {
}

// pull files by this method   RPC
func (f *FileFetcher) FetchFile(args *FileArgs, reply *FileReply) error {
	content, err := ioutil.ReadFile(args.File)
	if err != nil {
		return err
	}
	reply.Content = content
	reply.File = args.File
	return nil
}

// call to pull files from master or worker
func (f *FileFetcher) pullFile(role, address, file string) error {
	reply := &FileReply{}
	if call(address, role+".FetchFile", &FileArgs{File: file}, reply) {
		log.Printf("pull %s from %s success", file, address)
		err := ioutil.WriteFile(file, reply.Content, 0600)
		if err != nil {
			return errors.Wrap(err, "WriteFile")
		}
		return nil

	} else {
		log.Printf("pull %s from %s error", file, address)
		return fmt.Errorf("failed to pull file from %s", address)
	}
}

