package mapreducerpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(KeyValue) []KeyValue

type ReduceFunc func(key string, values []string) KeyValue

type Worker struct {
	Id int

	State WorkerState
	Task  *MapChunk

	MapFn    MapFunc
	ReduceFn ReduceFunc

	Server   *Server
	MasterId int // master의 ID

	// 작업 결과 저장
	LastResult []KeyValue
	LastError  error

	// MapReduce 논문에 따른 중간 결과 버퍼링
	intermediateBuffer map[int][]KeyValue // R개 영역으로 분할된 버퍼
	bufferMutex        sync.RWMutex
	numReduceTasks     int    // R 값
	outputDir          string // 중간 결과 저장 디렉토리

	// 파일 위치 정보
	intermediateFiles map[int]string // reduce task ID -> 파일 경로
}

type WorkerState int

const (
	Idle WorkerState = iota
	Mapping
	Reducing
	Dead
)

func NewWorker(id int, masterId int, numReduceTasks int, outputDir string) *Worker {
	return &Worker{
		Id:                 id,
		State:              Idle,
		Task:               nil,
		intermediateBuffer: make(map[int][]KeyValue),
		numReduceTasks:     numReduceTasks,
		outputDir:          outputDir,
		intermediateFiles:  make(map[int]string),
		MasterId:           masterId,
	}
}

func (w *Worker) SetServer(server *Server) {
	w.Server = server
}

func (w *Worker) RegisterWorker(id int, addr net.Addr) error {
	return w.Server.ConnectToPeer(id, addr)
}

func (w *Worker) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	if w.State == Idle {
		reply.State = Idle
		reply.Task = nil
	} else {
		log.Printf("[%v] Heartbeat Received. State: %v, Task: %v", w.Id, w.State, w.Task)
		reply.State = w.State
		reply.Task = w.Task
	}
	return nil
}

func (w *Worker) Map(args MapArgs, reply *MapReply) error {
	log.Printf("[%v] Map Received", w.Id)

	// 이미 작업 중이면 실패
	if w.State != Idle {
		reply.isSuccess = false
		return nil
	}

	w.State = Mapping
	w.Task = args.Chunk

	// 비동기로 Map 작업 실행
	go func() {
		defer func() {
			w.doneMapTask()
		}()

		f, err := os.Open(args.InputFilePath)
		if err != nil {
			log.Printf("[%v] Failed to open file: %v", w.Id, err)
			w.LastError = err
			return
		}
		defer f.Close()

		buf := make([]byte, args.Chunk.Length)
		_, err = io.ReadFull(io.NewSectionReader(f, int64(args.Chunk.StartIndex), int64(args.Chunk.Length)), buf)
		if err != nil {
			log.Printf("[%v] Failed to read chunk: %v", w.Id, err)
			w.LastError = err
			return
		}

		// Map 함수 실행
		result := w.MapFn(KeyValue{Key: args.InputFilePath, Value: string(buf)})

		// 중간 결과를 R개 영역으로 분할하여 버퍼에 저장
		w.bufferIntermediateResults(result)

		// 주기적으로 버퍼를 디스크에 저장
		w.flushBufferToDisk()

		w.LastError = nil
		log.Printf("[%v] Map completed, intermediate files: %v", w.Id, w.intermediateFiles)
	}()

	reply.isSuccess = true

	return nil
}

func (w *Worker) doneMapTask() {
	args := DoneMapTaskArgs{
		WorkerId: w.Id,
		Chunk:    w.Task,
	}
	var reply DoneMapTaskReply
	err := w.Server.Call(w.MasterId, "MapReduce.DoneMapTask", args, &reply)
	if err != nil {
		log.Printf("[%v] Failed to notify master of map task completion: %v", w.Id, err)
	} else {
		log.Printf("[%v] Successfully notified master of map task completion", w.Id)
	}
	w.State = Idle
	w.Task = nil
}

// hash 함수: key를 R개 영역으로 분할
func (w *Worker) hash(key string) int {
	hash := 0
	for _, c := range key {
		hash = (hash*31 + int(c)) % w.numReduceTasks
	}
	return hash
}

// 중간 결과를 R개 영역으로 분할하여 버퍼에 저장
func (w *Worker) bufferIntermediateResults(results []KeyValue) {
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	for _, kv := range results {
		reduceTaskId := w.hash(kv.Key)
		w.intermediateBuffer[reduceTaskId] = append(w.intermediateBuffer[reduceTaskId], kv)
	}
}

// 버퍼를 디스크에 저장
func (w *Worker) flushBufferToDisk() {
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	// 출력 디렉토리 생성
	if err := os.MkdirAll(w.outputDir, 0755); err != nil {
		log.Printf("[%v] Failed to create output directory: %v", w.Id, err)
		return
	}

	// 각 reduce task별로 파일에 저장
	for reduceTaskId, kvs := range w.intermediateBuffer {
		if len(kvs) == 0 {
			continue
		}

		// 파일명: workerId_mapTaskId_reduceTaskId.json
		filename := fmt.Sprintf("worker_%d_map_%d_reduce_%d.json",
			w.Id, w.Task.StartIndex, reduceTaskId)
		filepath := filepath.Join(w.outputDir, filename)

		// JSON으로 저장
		file, err := os.Create(filepath)
		if err != nil {
			log.Printf("[%v] Failed to create file %s: %v", w.Id, filepath, err)
			continue
		}

		encoder := json.NewEncoder(file)
		if err := encoder.Encode(kvs); err != nil {
			log.Printf("[%v] Failed to encode data to %s: %v", w.Id, filepath, err)
			file.Close()
			continue
		}

		file.Close()
		w.intermediateFiles[reduceTaskId] = filepath
		log.Printf("[%v] Saved %d key-value pairs to %s", w.Id, len(kvs), filepath)
	}

	// 버퍼 초기화
	w.intermediateBuffer = make(map[int][]KeyValue)
}

// 중간 파일 위치 정보를 반환 (내부용)
func (w *Worker) getIntermediateFiles() map[int]string {
	w.bufferMutex.RLock()
	defer w.bufferMutex.RUnlock()

	// 복사본 반환
	files := make(map[int]string)
	for k, v := range w.intermediateFiles {
		files[k] = v
	}
	return files
}

// 중간 파일 위치를 반환하는 RPC 메서드
func (w *Worker) GetIntermediateFiles(args GetIntermediateFilesArgs, reply *GetIntermediateFilesReply) error {
	log.Printf("[%v] GetIntermediateFiles Received", w.Id)

	reply.Files = w.getIntermediateFiles()
	reply.Success = true

	return nil
}

func (w *Worker) Reduce(args ReduceArgs, reply *ReduceReply) error {
	log.Printf("[%v] Reduce Received", w.Id)
	w.State = Reducing

	return nil
}
