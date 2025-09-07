package mapreduce

import (
	"bufio"
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

	mapFn    func() KeyValue
	reduceFn func() KeyValue

	inputFilePath string

	wg *sync.WaitGroup

	mapTasksChan chan string
}

func NewMapReduce(inputFilePath string) *MapReduce {
	mapChan := make(chan string, 100)
	mr := &MapReduce{
		inputFilePath: inputFilePath,

		mapTasksChan: mapChan,
	}

	return mr
}

func (mr *MapReduce) Run() {
	go mr.split()

	for i := 0; i < mr.NumMapWorkers; i++ {
		mr.wg.Add(1)
		go func() {
			defer mr.wg.Done()
			mr.mapFn()
		}()
	}

	mr.wg.Wait()
	close(mr.mapTasksChan)

	for i := 0; i < mr.NumReduceWorkers; i++ {
		mr.wg.Add(1)
		go func() {
			defer mr.wg.Done()
			mr.reduceFn()
		}()
	}

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
}
