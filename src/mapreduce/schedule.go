package mapreduce

import (
	"fmt"
	"sync"
)
import "net/rpc"
import "net"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func (mr *Master) schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup
    wg.Add(ntasks)
    fixChannel := make(chan bool)
    for i := 0; i < ntasks; i++ {
        go func(i int) {
            defer wg.Done()
            file := ""
            if i <= len(mapFiles) {
                file = mapFiles[i]
            }
            taskArgs := DoTaskArgs{
                JobName:       jobName,
                File:          file,
                Phase:         phase,
                TaskNumber:    i,
                NumOtherPhase: nOther,
            }

            taskFinished := false
            reply := &TaskReply{}

            for  {
                workAddr := <-registerChan
                // if the worker is failed, get another
                if _, ok := mr.FailWorker[workAddr]; !ok{  
                	workAddr = <-registerChan
                }

                taskFinished = call(workAddr, "Worker.DoTask", taskArgs, reply)
                go func() { registerChan <- workAddr }()
                if taskFinished != false {
                	if phase == mapPhase {
                		for _, file := range reply.Files {
                			mr.Lock()
							mr.mapFileAddressByFile[file] = workAddr
							if _, ok := mr.FilesOfWorker[workAddr]; !ok {
								mr.FilesOfWorker[workAddr] = make([]string,0)
								mr.TaskIndexesOfWorker[workAddr] = make([]int,0)
							}
							mr.FilesOfWorker[workAddr] = append(mr.FilesOfWorker[workAddr],file)
							mr.TaskIndexesOfWorker[workAddr] = append(mr.TaskIndexesOfWorker[workAddr],i)
							mr.Unlock()
						}
		            }
		            // if it run on GFS, this part is not necessary according to the paper,
					// as the the intermediate files are store on GFS
		            if phase == reducePhase {
		            	mr.Lock()
						mr.reduceFileAddressByFile[reply.Files[0]] = workAddr	
						mr.Unlock()
		            }
		            //
		            break
                }else{
                	if phase == reducePhase {
                		address := reply.address // address of the probably failed worker
                		mr.Lock()
                		_, ok := mr.FailWorker[address]
                		if ok == false {
                			HangTaskNumber[address] = 0
                			checkWorker(address, fixChannel, taskArgs, registerChan )
                		}
                		mr.Unlock()
                		// HangTaskNumber[address] = HangTaskNumber[address]+1
                		// value, ok <- fixChannel
                		
                		
                	}
                }


            }


        }(i)

    }
    wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}

func (mr *Master) checkWorker(address string, fixChannel chan bool, taskArgs DoTaskArgs, registerChan chan string, nReduce int) {
	c, errx := rpc.Dial("tcp", address)
	defer c.Close()
	if errx != nil {
		// mr.Lock()
		mr.FailWorker[address] = true
		// mr.Unlock()

		workAddr := <-registerChan
		taskArgs.Phase = mapPhase
		taskArgs.NumOtherPhase = nReduce

		restartTaskNum := len(mr.TaskIndexesOfWorker[address] )

		var wg sync.WaitGroup
	    wg.Add(restartTaskNum)

	    fmt.Printf("restart map tasks in %s", address)

	    for i := 0; i < restartTaskNum; i++ {
	    	go func(i int) {
	    		taskArgs.File = mr.FilesOfWorker[address][i]
				taskArgs.TaskNumber = mr.TaskIndexesOfWorker[address][i]
				reply := &TaskReply{}
				taskFinished := call(workAddr, "Worker.DoTask", taskArgs, reply)
				wg.Done()
	    	}
	    }

	    // go func() { registerChan <- workAddr }()

		wg.Wait()

		fmt.Printf("map tasks in %s are done in other workers ", address)

		return
	}


}
