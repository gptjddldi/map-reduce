package main

import (
	"map-reduce/mapreduce"
	"strconv"
	"strings"
)

// type KeyValue struct {
// 	Key   string
// 	Value string
// }

// type mapFn func(key, value string) []KeyValue
// type reduceFn func(key string, values []string) KeyValue

// type MapReduce struct {
// 	NumMapWorkers    int
// 	NumReduceWorkers int

// 	InputFilePath string

// 	MapFunc    mapFn
// 	ReduceFunc reduceFn

// 	mapTasksChan     chan string
// 	intermediateChan chan KeyValue
// 	reduceChan       []chan KeyValue
// 	resultsChan      chan KeyValue
// }

// func NewMapReduce(mapF mapFn, reduceF reduceFn, nMap, nReduce int, inputFilePath string) *MapReduce {
// 	m := &MapReduce{
// 		NumMapWorkers:    nMap,
// 		NumReduceWorkers: nReduce,

// 		InputFilePath: inputFilePath,

// 		MapFunc:    mapF,
// 		ReduceFunc: reduceF,
// 	}

// 	return m
// }

// func (mr *MapReduce) Run() {
// 	mr.mapTasksChan = make(chan string, 100)
// 	mr.intermediateChan = make(chan KeyValue, 100)
// 	mr.resultsChan = make(chan KeyValue, 100)
// 	mr.reduceChan = make([]chan KeyValue, mr.NumReduceWorkers)
// 	for i := 0; i < mr.NumReduceWorkers; i++ {
// 		mr.reduceChan[i] = make(chan KeyValue, 100)
// 	}

// 	go mr.Split()

// 	var mapWg sync.WaitGroup
// 	for i := 0; i < mr.NumMapWorkers; i++ {
// 		mapWg.Add(1)
// 		go func() {
// 			defer mapWg.Done()
// 			for task := range mr.mapTasksChan {
// 				kvs := mr.MapFunc(mr.InputFilePath, task)
// 				for _, kv := range kvs {
// 					mr.intermediateChan <- kv
// 				}
// 			}
// 		}()
// 	}

// 	go func() {
// 		mapWg.Wait()
// 		close(mr.intermediateChan)
// 	}()

// 	go func() {
// 		for kv := range mr.intermediateChan {
// 			mr.shuffle(kv)
// 		}

// 		for _, c := range mr.reduceChan {
// 			close(c)
// 		}
// 	}()

// 	var reduceWg sync.WaitGroup
// 	for i := 0; i < mr.NumReduceWorkers; i++ {
// 		reduceWg.Add(1)
// 		go func(reduceIndex int) {
// 			defer reduceWg.Done()
// 			mr.reduce(reduceIndex)
// 		}(i)
// 	}

// 	go func() {
// 		reduceWg.Wait()
// 		close(mr.resultsChan)
// 	}()

// 	reduceResults := []KeyValue{}
// 	for kv := range mr.resultsChan {
// 		reduceResults = append(reduceResults, kv)
// 	}

// 	fmt.Println(reduceResults)
// }

// func (mr *MapReduce) Split() {
// 	defer close(mr.mapTasksChan)

// 	file, err := os.Open(mr.InputFilePath)
// 	if err != nil {
// 		log.Fatalf("cannot open %v: %v", mr.InputFilePath, err)
// 		return
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		mr.mapTasksChan <- line
// 	}

// 	if err := scanner.Err(); err != nil {
// 		log.Fatalf("error while reading file: %v", err)
// 	}
// }

// // shuffle: 맵 단계에서 생성된 모든 KeyValue 쌍들을 수집하고,
// // 같은 Key를 가진 데이터가 같은 리듀스 워커에게 전달되도록 그룹화합니다.
// func (mr *MapReduce) shuffle(kv KeyValue) {
// 	// hash(key) % NumReduceWorkers 와 같은 방식으로 데이터를 분배
// 	hash := fnv.New32a()
// 	hash.Write([]byte(kv.Key))
// 	index := int(hash.Sum32() % uint32(mr.NumReduceWorkers))
// 	mr.reduceChan[index] <- kv

// }

// func (mr *MapReduce) reduce(reduceIndex int) {
// 	intermediate := make(map[string][]string)
// 	for kv := range mr.reduceChan[reduceIndex] {
// 		intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
// 	}

// 	for key, values := range intermediate {
// 		output := mr.ReduceFunc(key, values)
// 		mr.resultsChan <- output
// 	}
// }

// func main() {

// 	mapFn := func(key, value string) []KeyValue {
// 		lis := strings.Split(value, " ")
// 		kvs := []KeyValue{}
// 		for _, l := range lis {
// 			kvs = append(kvs, KeyValue{Key: l, Value: "1"})
// 		}
// 		return kvs
// 	}

// 	reduceFn := func(key string, values []string) KeyValue {
// 		sum := 0
// 		for _, v := range values {
// 			val, _ := strconv.Atoi(v)
// 			sum += val
// 		}
// 		return KeyValue{Key: key, Value: strconv.Itoa(sum)}
// 	}

// 	mr := NewMapReduce(mapFn, reduceFn, 10, 10, "large_file.txt")
// 	mr.Run()
// }

func main() {
	mr := mapreduce.NewMapReduce("large_file.txt", 10, 10)
	mr.SetMapFn(func(kv mapreduce.KeyValue) []mapreduce.KeyValue {
		lis := strings.Split(kv.Value, " ")
		kvs := []mapreduce.KeyValue{}
		for _, l := range lis {
			kvs = append(kvs, mapreduce.KeyValue{Key: l, Value: "1"})
		}
		return kvs
	})
	mr.SetReduceFn(func(key string, values []string) mapreduce.KeyValue {
		sum := 0
		for _, v := range values {
			val, _ := strconv.Atoi(v)
			sum += val
		}
		return mapreduce.KeyValue{Key: key, Value: strconv.Itoa(sum)}
	})
	mr.Run()
}
