package rpc

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

type WorkerState int

const (
	Idle WorkerState = iota
	Mapping
	Reducing
	Dead
)

type HeartbeatArgs struct {
}
type HeartbeatReply struct {
}

type MapArgs struct {
	Chunk         *MapChunk
	InputFilePath string
}
type MapReply struct {
	IsSuccess bool
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
