package mapreducerpc

import (
	"fmt"
)

type Cluster struct {
	Master        *Master
	MasterServer  *Server
	WorkerServers []*Server
}

func StartCluster(inputFilePath string, numWorkers int, numReduceTasks int, mapFn MapFunc, reduceFn ReduceFunc) (*Cluster, error) {
	masterServer := NewServer(numWorkers, "master", true)
	master := NewMaster(inputFilePath, masterServer, numWorkers)

	// Master를 RPC 서버로 등록
	masterServer.AttachMaster(master)

	// RPC 등록 후 서버 시작
	masterServer.Serve()

	workerServers := make([]*Server, 0, numWorkers)
	for i := 1; i <= numWorkers; i++ {
		ws := NewServer(numWorkers, fmt.Sprintf("worker-%d", i), false)
		outputDir := fmt.Sprintf("./intermediate/worker-%d", i)
		w := NewWorker(ws, i, 0, numReduceTasks, outputDir) // master ID는 0
		w.MapFn = mapFn
		w.ReduceFn = reduceFn
		ws.AttachWorker(w)
		ws.Serve()
		workerServers = append(workerServers, ws)

		if err := master.RegisterWorker(i, ws.Listener().Addr()); err != nil {
			return nil, err
		}

		if err := w.RegisterWorker(0, masterServer.Listener().Addr()); err != nil {
			return nil, err
		}
	}

	return &Cluster{
		Master:        master,
		MasterServer:  masterServer,
		WorkerServers: workerServers,
	}, nil
}

func (c *Cluster) Run() {
	c.Master.Run()
}

func (c *Cluster) Shutdown() {
	c.MasterServer.Shutdown()
	for _, ws := range c.WorkerServers {
		ws.Shutdown()
	}
}
