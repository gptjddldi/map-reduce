package mapreduce

type KeyValue struct {
	Key   string
	Value string
}

type mapFn func(key, value string) []KeyValue
type reduceFn func(key string, values []string) string

type MapReduce struct {
	NumMapWorkers    int
	NumReduceWorkers int

	InputFilePath string

	MapFunc    mapFn
	ReduceFunc reduceFn

	mapTasksChan     <-chan int
	intermediateChan <-chan int
}

func NewMapReduce(mapF mapFn, reduceF reduceFn, nMap, nReduce int, inputFilePath string) *MapReduce {
	m := &MapReduce{
		NumMapWorkers:    nMap,
		NumReduceWorkers: nReduce,

		InputFilePath: inputFilePath,

		MapFunc:    mapF,
		ReduceFunc: reduceF,
	}

	return m
}

func (mr *MapReduce) Run() {

}

func (mr *MapReduce) Split() {

}
