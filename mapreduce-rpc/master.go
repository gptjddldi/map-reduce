package mapreducerpc

import (
	"fmt"
	"log"
	"map-reduce/mapreduce-rpc/rpc"
	"net"
	"os"
	"sync"
	"time"
)

const ChunkSize = 1024 * 1 // 1KB

type Master struct {
	Id int

	Server *Server

	Chunks    map[int]*rpc.MapChunk
	ChunkChan chan *rpc.MapChunk

	NumMapWorkers int
	WorkerStates  []rpc.WorkerState

	inputFilePath string

	// 완료된 task 추적
	completedChunks map[int]bool  // chunk index -> 완료 여부
	allMapTasksDone chan struct{} // 모든 map task 완료 신호

	// 워커별 타이머 관리
	workerTimers     []*time.Timer // 워커별 하트비트 타이머
	workerTimeouts   []time.Time   // 워커별 마지막 하트비트 시간
	heartbeatTimeout time.Duration // 하트비트 타임아웃 시간

	stopChan chan struct{}

	wg sync.WaitGroup
	mu sync.Mutex
}

func NewMaster(inputFilePath string, numMapWorkers int) *Master {
	master := &Master{
		inputFilePath:    inputFilePath,
		NumMapWorkers:    numMapWorkers,
		WorkerStates:     make([]rpc.WorkerState, numMapWorkers+1),
		completedChunks:  make(map[int]bool),
		allMapTasksDone:  make(chan struct{}),
		stopChan:         make(chan struct{}),
		heartbeatTimeout: 200 * time.Millisecond,
		mu:               sync.Mutex{},
	}

	master.workerTimers = make([]*time.Timer, numMapWorkers+1)
	master.workerTimeouts = make([]time.Time, numMapWorkers+1)

	for i := 1; i <= numMapWorkers; i++ {
		master.WorkerStates[i] = rpc.Idle
		master.workerTimeouts[i] = time.Now()
		master.startWorkerTimer(i)
	}

	return master
}

func (m *Master) SetServer(server *Server) {
	m.Server = server
}

func (m *Master) startWorkerTimer(workerId int) {
	if workerId >= len(m.workerTimers) {
		return
	}

	if m.workerTimers[workerId] != nil {
		m.workerTimers[workerId].Stop()
	}

	m.workerTimers[workerId] = time.AfterFunc(m.heartbeatTimeout, func() {
		m.handleWorkerTimeout(workerId)
	})
}

func (m *Master) resetWorkerTimer(workerId int) {
	if workerId >= len(m.workerTimers) {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.WorkerStates[workerId] == rpc.Dead {
		return
	}

	m.workerTimeouts[workerId] = time.Now()
	m.startWorkerTimer(workerId)
}

func (m *Master) handleWorkerTimeout(workerId int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.WorkerStates[workerId] == rpc.Dead {
		return
	}

	log.Printf("Worker %d timed out, marking as dead", workerId)

	m.WorkerStates[workerId] = rpc.Dead
	m.handleDeadWorker(workerId)
}

// TODO: 죽은 worker 가 하던 작업을 다른 worker 에게 재할당
func (m *Master) handleDeadWorker(workerId int) {
	log.Printf("Handling dead worker %d", workerId)
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
	m.Chunks = make(map[int]*rpc.MapChunk)

	file, err := os.Stat(m.inputFilePath)
	if err != nil {
		log.Fatalf("cannot open %v: %v", m.inputFilePath, err)
		return
	}

	fileSize := file.Size()

	numChunks := fileSize / int64(ChunkSize)
	m.ChunkChan = make(chan *rpc.MapChunk, int(numChunks))
	log.Printf("numChunks: %d", numChunks)
	// Chunk 생성 및 채널 전송을 별도 고루틴으로 처리
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(m.ChunkChan)

		for i := 0; i < int(numChunks); i++ {
			chunk := &rpc.MapChunk{
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
		for workerId := 1; workerId <= m.NumMapWorkers; workerId++ {
			m.mu.Lock()
			if m.WorkerStates[workerId] == rpc.Idle {
				// Worker에게 Map 작업 요청 (비동기)
				go func(workerId int, chunk *rpc.MapChunk) {
					m.mu.Lock()
					if m.WorkerStates[workerId] == rpc.Idle {
						m.WorkerStates[workerId] = rpc.Mapping
						request := rpc.MapArgs{Chunk: chunk, InputFilePath: m.inputFilePath}
						var reply rpc.MapReply
						m.RequestMap(workerId, request, reply)
					}
					m.mu.Unlock()
				}(workerId, chunk)
				m.mu.Unlock()
				break // 작업을 할당했으므로 루프 종료
			} else {
				m.mu.Unlock()
			}
		}
	}
}

func (m *Master) sendHeartbeats() {
	for workerId := 1; workerId <= m.NumMapWorkers; workerId++ {
		go func(id int) {
			// Dead 워커에게는 하트비트를 보내지 않음
			m.mu.Lock()
			if m.WorkerStates[id] == rpc.Dead {
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()

			var args rpc.HeartbeatArgs
			var reply rpc.HeartbeatReply
			if err := m.Server.Call(id, "MapReduce.Heartbeat", args, &reply); err == nil {
				m.resetWorkerTimer(id)
			} else {
				select {
				case <-m.stopChan:
					return
				default:
					log.Printf("failed to send heartbeat to worker %d: %v", id, err)
					// 하트비트 실패 시에도 타이머는 계속 실행되어 타임아웃 감지
				}
			}
		}(workerId)
	}
}

func (m *Master) RequestMap(workerId int, request rpc.MapArgs, reply rpc.MapReply) rpc.MapReply {
	if err := m.Server.Call(workerId, "MapReduce.Map", request, &reply); err == nil {
		reply.IsSuccess = true
	} else {
		reply.IsSuccess = false
	}
	return reply
}

func (m *Master) RequestReduce(workerId int, request rpc.ReduceArgs, reply rpc.ReduceReply) {
	m.Server.Call(workerId, "MapReduce.Reduce", request, reply)
}

// Worker로부터 중간 파일 위치 정보를 가져옴
func (m *Master) GetIntermediateFiles(workerId int) (map[int]string, error) {
	var args rpc.GetIntermediateFilesArgs
	var reply rpc.GetIntermediateFilesReply

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

	// 모든 워커 타이머 정리
	for i := 0; i < len(m.workerTimers); i++ {
		if m.workerTimers[i] != nil {
			m.workerTimers[i].Stop()
		}
	}

	m.Server.Shutdown()
}

// DoneMapTask RPC 메서드: worker가 map task 완료를 알림
func (m *Master) DoneMapTask(args rpc.DoneMapTaskArgs, reply *rpc.DoneMapTaskReply) error {
	log.Printf("Map task completed by worker %d for chunk %d", args.WorkerId, args.Chunk.StartIndex)

	m.mu.Lock()
	defer m.mu.Unlock()

	// chunk 완료 표시
	chunkIndex := args.Chunk.StartIndex / ChunkSize
	m.completedChunks[chunkIndex] = true

	// worker 상태를 Idle로 변경
	if args.WorkerId < len(m.WorkerStates) {
		m.WorkerStates[args.WorkerId] = rpc.Idle
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
