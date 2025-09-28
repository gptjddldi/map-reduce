package mapreducerpc

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const ChunkSize = 1024 * 1 // 1KB

type MapChunk struct {
	StartIndex int
	Length     int
	State      ChunkState
}

type ChunkState int

const (
	Ready ChunkState = iota
	Done
)

type Master struct {
	Id int

	Server *Server

	Chunks    map[int]*MapChunk
	ChunkChan chan *MapChunk

	NumMapWorkers int
	WorkerStates  []WorkerState

	inputFilePath string

	// 완료된 task 추적
	completedChunks map[int]bool  // chunk index -> 완료 여부
	allMapTasksDone chan struct{} // 모든 map task 완료 신호

	stopChan chan struct{}

	wg sync.WaitGroup
	mu sync.Mutex
}

type HeartbeatArgs struct {
}
type HeartbeatReply struct {
	State WorkerState
	Task  *MapChunk
}

type MapArgs struct {
	Chunk         *MapChunk
	InputFilePath string
}
type MapReply struct {
	isSuccess bool
}
type ReduceArgs struct {
}
type ReduceReply struct {
}

type GetIntermediateFilesArgs struct {
}
type GetIntermediateFilesReply struct {
	Files   map[int]string // reduce task ID -> 파일 경로
	Success bool
}

type DoneMapTaskArgs struct {
	WorkerId int
	Chunk    *MapChunk
}
type DoneMapTaskReply struct {
	Success bool
}

func NewMaster(inputFilePath string, numMapWorkers int) *Master {
	return &Master{
		inputFilePath:   inputFilePath,
		NumMapWorkers:   numMapWorkers,
		WorkerStates:    make([]WorkerState, numMapWorkers),
		completedChunks: make(map[int]bool),
		allMapTasksDone: make(chan struct{}),
		stopChan:        make(chan struct{}),
		mu:              sync.Mutex{},
	}
}

func (m *Master) SetServer(server *Server) {
	m.Server = server
}

// RegisterWorker connects the master's RPC client to a worker's address for the given id.
func (m *Master) RegisterWorker(id int, addr net.Addr) error {
	return m.Server.ConnectToPeer(id, addr)
}

func (m *Master) Run() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-m.stopChan:
				return
			default:
				m.sendHeartbeats()
			}
			<-ticker.C
		}
	}()

	// 1. Split InputFile into N parts (file's metadata)
	m.Chunks = make(map[int]*MapChunk)

	file, err := os.Stat(m.inputFilePath)
	if err != nil {
		log.Fatalf("cannot open %v: %v", m.inputFilePath, err)
		return
	}

	fileSize := file.Size()

	numChunks := fileSize / int64(ChunkSize)
	m.ChunkChan = make(chan *MapChunk, int(numChunks))
	log.Printf("numChunks: %d", numChunks)
	// Chunk 생성 및 채널 전송을 별도 고루틴으로 처리
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(m.ChunkChan)

		for i := 0; i < int(numChunks); i++ {
			chunk := &MapChunk{
				StartIndex: i * ChunkSize,
				Length:     int(ChunkSize),
			}
			m.Chunks[i] = chunk
			m.ChunkChan <- chunk
		}
	}()

	// Worker에게 chunk 할당하는 고루틴
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.assignChunksToWorkers()
	}()

	// 모든 map task 완료 대기
	<-m.allMapTasksDone
	log.Printf("All map tasks completed, proceeding to next phase...")

	// Wait for all goroutines to complete
	m.wg.Wait()

	// 여기에 다음 로직 (예: reduce phase) 추가 가능
	m.Shutdown()
}

// 채널에서 chunk를 받아서 idle worker에게 할당
func (m *Master) assignChunksToWorkers() {
	for chunk := range m.ChunkChan {
		for workerId := 0; workerId < m.NumMapWorkers; workerId++ {
			m.mu.Lock()
			if m.WorkerStates[workerId] == Idle {
				// Worker에게 Map 작업 요청 (비동기)
				go func(workerId int, chunk *MapChunk) {
					m.mu.Lock()
					if m.WorkerStates[workerId] == Idle {
						m.WorkerStates[workerId] = Mapping
						request := MapArgs{Chunk: chunk, InputFilePath: m.inputFilePath}
						var reply MapReply
						m.RequestMap(workerId, request, reply)
					}
					m.mu.Unlock()
				}(workerId, chunk)
			}
			m.mu.Unlock()

		}
	}
}

func (m *Master) sendHeartbeats() {
	for workerId := 0; workerId < m.NumMapWorkers; workerId++ {
		go func(id int) {
			var args HeartbeatArgs
			var reply HeartbeatReply
			if err := m.Server.Call(id, "MapReduce.Heartbeat", args, &reply); err == nil {
				m.mu.Lock()
				m.WorkerStates[id] = reply.State
				m.mu.Unlock()
			} else {
				// Only log errors if the connection is not intentionally closed
				select {
				case <-m.stopChan:
					// Master is shutting down, don't log connection errors
					return
				default:
					log.Printf("failed to send heartbeat to worker %d: %v", id, err)
				}
			}
		}(workerId)
	}
}

func (m *Master) RequestMap(workerId int, request MapArgs, reply MapReply) MapReply {
	if err := m.Server.Call(workerId, "MapReduce.Map", request, &reply); err == nil {
		reply.isSuccess = true
	} else {
		reply.isSuccess = false
	}
	return reply
}

func (m *Master) RequestReduce(workerId int, request ReduceArgs, reply ReduceReply) {
	m.Server.Call(workerId, "MapReduce.Reduce", request, reply)
}

// Worker로부터 중간 파일 위치 정보를 가져옴
func (m *Master) GetIntermediateFiles(workerId int) (map[int]string, error) {
	var args GetIntermediateFilesArgs
	var reply GetIntermediateFilesReply

	if err := m.Server.Call(workerId, "MapReduce.GetIntermediateFiles", args, &reply); err != nil {
		return nil, err
	}

	if !reply.Success {
		return nil, fmt.Errorf("failed to get intermediate files from worker %d", workerId)
	}

	return reply.Files, nil
}

func (m *Master) Shutdown() {
	select {
	case <-m.stopChan:
		// Already closed
	default:
		close(m.stopChan)
	}
	m.Server.Shutdown()
}

// DoneMapTask RPC 메서드: worker가 map task 완료를 알림
func (m *Master) DoneMapTask(args DoneMapTaskArgs, reply *DoneMapTaskReply) error {
	log.Printf("Map task completed by worker %d for chunk %d", args.WorkerId, args.Chunk.StartIndex)

	m.mu.Lock()
	defer m.mu.Unlock()

	// chunk 완료 표시
	chunkIndex := args.Chunk.StartIndex / ChunkSize
	m.completedChunks[chunkIndex] = true

	// worker 상태를 Idle로 변경
	if args.WorkerId < len(m.WorkerStates) {
		m.WorkerStates[args.WorkerId] = Idle
	}

	// 모든 chunk가 완료되었는지 확인
	if len(m.completedChunks) == len(m.Chunks) {
		log.Printf("All map tasks completed!")
		close(m.allMapTasksDone)
	} else {
		log.Printf("left chunks: %d", len(m.Chunks)-len(m.completedChunks))
	}

	reply.Success = true
	return nil
}
