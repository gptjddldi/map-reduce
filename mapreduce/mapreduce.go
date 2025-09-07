package mapreduce

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
)

type KeyValue struct {
	Key, Value string
}

type MapReduce struct {
	NumMapWorkers    int
	NumReduceWorkers int

	mapFn    func(kv KeyValue) []KeyValue
	reduceFn func(key string, values []string) KeyValue

	inputFilePath string

	mapTasksChan    chan string
	reduceTasksChan []chan KeyValue
	resultsChan     chan KeyValue
}

func NewMapReduce(inputFilePath string, numMapWorkers int, numReduceWorkers int) *MapReduce {
	mr := &MapReduce{
		inputFilePath:    inputFilePath,
		mapTasksChan:     make(chan string, numMapWorkers),
		NumMapWorkers:    numMapWorkers,
		NumReduceWorkers: numReduceWorkers,
		resultsChan:      make(chan KeyValue, numReduceWorkers),
	}
	return mr
}

func (mr *MapReduce) SetMapFn(mapFn func(kv KeyValue) []KeyValue) {
	mr.mapFn = mapFn
}

func (mr *MapReduce) SetReduceFn(reduceFn func(key string, values []string) KeyValue) {
	mr.reduceFn = reduceFn
}

func (mr *MapReduce) Run() {
	mr.reduceTasksChan = make([]chan KeyValue, mr.NumReduceWorkers)
	for i := 0; i < mr.NumReduceWorkers; i++ {
		mr.reduceTasksChan[i] = make(chan KeyValue, mr.NumReduceWorkers) // 각 리듀스 채널도 초기화
	}

	mapWg := &sync.WaitGroup{}
	reduceWg := &sync.WaitGroup{}

	// ======================= MAP PHASE START =======================
	go mr.split()

	mapWg.Add(mr.NumMapWorkers)
	for i := 0; i < mr.NumMapWorkers; i++ {
		go func() {
			defer mapWg.Done()
			for task := range mr.mapTasksChan {
				kvs := mr.mapFn(KeyValue{Key: mr.inputFilePath, Value: task})
				for _, kv := range kvs {
					hash := fnv.New32a()
					hash.Write([]byte(kv.Key))
					index := int(hash.Sum32() % uint32(mr.NumReduceWorkers))
					mr.reduceTasksChan[index] <- kv
				}
			}
		}()
	}

	go func() {
		mapWg.Wait()
		for i := 0; i < mr.NumReduceWorkers; i++ {
			close(mr.reduceTasksChan[i])
		}
	}()

	// ======================= REDUCE PHASE START =======================
	reduceWg.Add(mr.NumReduceWorkers)
	for i := 0; i < mr.NumReduceWorkers; i++ {
		go func(reduceIndex int) {
			defer reduceWg.Done()

			keyValuesMap := make(map[string][]string)
			for kv := range mr.reduceTasksChan[reduceIndex] {
				keyValuesMap[kv.Key] = append(keyValuesMap[kv.Key], kv.Value)
			}

			for key, values := range keyValuesMap {
				result := mr.reduceFn(key, values)
				mr.resultsChan <- result
			}
		}(i)
	}
	go func() {
		reduceWg.Wait()
		close(mr.resultsChan)
	}()

	// ======================= RESULTS PHASE =======================

	results := []KeyValue{}
	for kv := range mr.resultsChan {
		results = append(results, kv)
	}
	fmt.Println(results)
}

func (mr *MapReduce) split() {
	file, err := os.Open(mr.inputFilePath)
	if err != nil {
		log.Fatalf("cannot open %v: %v", mr.inputFilePath, err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		mr.mapTasksChan <- line
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("error while reading file: %v", err)
	}
	close(mr.mapTasksChan)
}
