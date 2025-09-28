package mapreducerpc

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	serverId string

	rpcServer *rpc.Server
	listener  net.Listener
	handler   interface{}

	peerClients map[int]*rpc.Client

	mu   sync.Mutex
	wg   sync.WaitGroup
	quit chan struct{}
}

func NewServer(serverId string, handler interface{}) *Server {
	return &Server{
		rpcServer:   rpc.NewServer(),
		serverId:    serverId,
		quit:        make(chan struct{}),
		peerClients: make(map[int]*rpc.Client),
		handler:     handler,
	}
}

func (s *Server) Serve() {

	s.mu.Lock()
	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	s.mu.Unlock()

	s.mu.Lock()
	s.rpcServer.RegisterName("MapReduce", s.handler)
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
