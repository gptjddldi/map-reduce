package mapreducerpc

import (
	"fmt"
	"map-reduce/mapreduce-rpc/rpc"
)

type WorkerHandler interface {
	Heartbeat(args rpc.HeartbeatArgs, reply *rpc.HeartbeatReply) error
	Map(args rpc.MapArgs, reply *rpc.MapReply) error
	Reduce(args rpc.ReduceArgs, reply *rpc.ReduceReply) error
	GetIntermediateFiles(args rpc.GetIntermediateFilesArgs, reply *rpc.GetIntermediateFilesReply) error
	Shutdown(args struct{}, reply struct{}) error
}

type MasterHandler interface {
	DoneMapTask(args rpc.DoneMapTaskArgs, reply *rpc.DoneMapTaskReply) error
}

type Cluster struct {
	master        *Master
	masterServer  *Server
	workerServers []*Server
	workers       []*Worker
}

func StartCluster(inputFilePath string, numWorkers int, numReduceTasks int, mapFn MapFunc, reduceFn ReduceFunc) (*Cluster, error) {
	master := NewMaster(inputFilePath, numWorkers)
	masterServer := NewServer("master", master)
	master.SetServer(masterServer)

	masterServer.Serve()

	workerServers := make([]*Server, 0, numWorkers)
	workers := make([]*Worker, 0, numWorkers)
	for i := 1; i <= numWorkers; i++ {
		outputDir := fmt.Sprintf("./intermediate/worker-%d", i)
		w := NewWorker(i, 0, numReduceTasks, outputDir) // master ID는 0
		w.MapFn = mapFn
		w.ReduceFn = reduceFn

		ws := NewServer(fmt.Sprintf("worker-%d", i), w)
		w.SetServer(ws)
		ws.Serve()
		workerServers = append(workerServers, ws)
		workers = append(workers, w)

		if err := master.RegisterWorker(i, ws.Listener().Addr()); err != nil {
			return nil, err
		}

		if err := w.RegisterWorker(0, masterServer.Listener().Addr()); err != nil {
			return nil, err
		}
	}

	return &Cluster{
		master:        master,
		masterServer:  masterServer,
		workerServers: workerServers,
		workers:       workers,
	}, nil
}

func (c *Cluster) Run() {
	c.master.Run()
}

func (c *Cluster) Shutdown() {
	// 먼저 모든 worker의 버퍼를 디스크에 저장
	for _, w := range c.workers {
		w.Shutdown(struct{}{}, struct{}{})
	}

	c.masterServer.Shutdown()
	for _, ws := range c.workerServers {
		ws.Shutdown()
	}
}
