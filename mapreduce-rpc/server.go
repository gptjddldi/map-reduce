package mapreducerpc

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	serverId      string
	NumMapWorkers int

	rpcServer *rpc.Server
	listener  net.Listener
	rpcProxy  *RPCProxy

	peerClients map[int]*rpc.Client

	mu   sync.Mutex
	wg   sync.WaitGroup
	quit chan struct{}

	isMaster bool

	worker *Worker
	master *Master
}

func NewServer(numMapWorkers int, serverId string, isMaster bool) *Server {
	return &Server{
		NumMapWorkers: numMapWorkers,
		rpcServer:     rpc.NewServer(),
		serverId:      serverId,
		quit:          make(chan struct{}),
		peerClients:   make(map[int]*rpc.Client),
		isMaster:      isMaster,
	}
}

func (s *Server) Serve() {

	s.mu.Lock()
	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	// Register RPC handlers
	s.mu.Lock()
	if s.worker != nil {
		s.rpcProxy = &RPCProxy{worker: s.worker}
		s.rpcServer.RegisterName("MapReduce", s.rpcProxy)
	} else if s.master != nil {
		s.rpcProxy = &RPCProxy{master: s.master}
		s.rpcServer.RegisterName("MapReduce", s.rpcProxy)
	}
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error", err)
				}
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()
}
func (s *Server) AttachWorker(w *Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.worker = w
}

func (s *Server) AttachMaster(m *Master) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.master = m
}
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.quit)
}

func (s *Server) Shutdown() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer != nil {
		return peer.Call(serviceMethod, args, reply)
	} else {
		return fmt.Errorf("call client %d after it's closed", id)
	}
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			log.Printf("[%v] failed to connect to peer %d at %s: %v", s.serverId, peerId, addr, err)
			return err
		}
		s.peerClients[peerId] = client

		log.Printf("[%v] connected to peer %d at %s", s.serverId, peerId, addr)
	}
	return nil
}

func (s *Server) Listener() net.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener
}

type RPCProxy struct {
	worker *Worker
	master *Master
}

func (rpp *RPCProxy) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	if rpp.worker != nil {
		return rpp.worker.Heartbeat(args, reply)
	}
	return fmt.Errorf("Heartbeat not supported on master")
}

func (rpp *RPCProxy) Map(args MapArgs, reply *MapReply) error {
	if rpp.worker != nil {
		return rpp.worker.Map(args, reply)
	}
	return fmt.Errorf("Map not supported on master")
}

func (rpp *RPCProxy) Reduce(args ReduceArgs, reply *ReduceReply) error {
	if rpp.worker != nil {
		return rpp.worker.Reduce(args, reply)
	}
	return fmt.Errorf("Reduce not supported on master")
}

func (rpp *RPCProxy) GetIntermediateFiles(args GetIntermediateFilesArgs, reply *GetIntermediateFilesReply) error {
	if rpp.worker != nil {
		return rpp.worker.GetIntermediateFiles(args, reply)
	}
	return fmt.Errorf("GetIntermediateFiles not supported on master")
}

func (rpp *RPCProxy) DoneMapTask(args DoneMapTaskArgs, reply *DoneMapTaskReply) error {
	if rpp.master != nil {
		return rpp.master.DoneMapTask(args, reply)
	}
	return fmt.Errorf("DoneMapTask not supported on worker")
}
