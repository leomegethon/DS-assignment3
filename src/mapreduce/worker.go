package mapreduce

import (
"container/list"
"fmt"
)
type WorkerInfo struct {
address string
}
func AssignJobsToWorkers(mr *MapReduce, doneChannel chan int, job JobType, nJobs int, nJobsOther int) {
for i := 0; i < nJobs; i++ {
go func(jobNum int) {
for {
worker := <-mr.registerChannel
args := &DoJobArgs{mr.file, job, jobNum, nJobsOther}
var reply DoJobReply
ok := call(worker, "Worker.DoJob", args, &reply)
if ok {

doneChannel <- 1
mr.registerChannel <- worker
break
} else {
fmt.Printf("Worker %s failed on job %d. Retrying...\n", worker, jobNum)
}
}
}(i)
}
}

func (mr *MapReduce) KillWorkers() *list.List {
l := list.New()
for _, w := range mr.Workers {
DPrintf("DoWork: shutdown %s\n", w.address)
args := &ShutdownArgs{}
var reply ShutdownReply
ok := call(w.address, "Worker.Shutdown", args, &reply)
if !ok {
fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
} else {
l.PushBack(reply.Njobs)
}
}
return l
}

func (mr *MapReduce) RunMaster() *list.List {

mapDoneChannel := make(chan int, mr.nMap)
reduceDoneChannel := make(chan int, mr.nReduce)


AssignJobsToWorkers(mr, mapDoneChannel, Map, mr.nMap, mr.nReduce)

for i := 0; i < mr.nMap; i++ {
<-mapDoneChannel
}

AssignJobsToWorkers(mr, reduceDoneChannel, Reduce, mr.nReduce, mr.nMap)

for i := 0; i < mr.nReduce; i++ {
<-reduceDoneChannel
}

return mr.KillWorkers()
}
