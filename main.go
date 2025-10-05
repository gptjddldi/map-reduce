package main

import (
	"fmt"
	"log"
	mapreducerpc "map-reduce/mapreduce-rpc"
	"os"
	"plugin"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: go run main.go xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef, err := loadPlugin(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	cluster, err := mapreducerpc.StartCluster("large_file.txt", 10, 3, mapf, reducef)
	if err != nil {
		log.Fatal(err)
	}

	defer cluster.Shutdown()

	cluster.Run()

	log.Println("MapReduce job completed successfully!")
}

func loadPlugin(path string) (mapreducerpc.MapFunc, mapreducerpc.ReduceFunc, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, nil, err
	}

	mapFn, err := p.Lookup("Map")
	if err != nil {
		return nil, nil, err
	}
	reduceFn, err := p.Lookup("Reduce")
	if err != nil {
		return nil, nil, err
	}

	// 타입 캐스팅을 명시적으로 수행
	mapFunc, ok := mapFn.(func(mapreducerpc.KeyValue) []mapreducerpc.KeyValue)
	if !ok {
		return nil, nil, fmt.Errorf("Map function has wrong signature")
	}

	reduceFunc, ok := reduceFn.(func(string, []string) mapreducerpc.KeyValue)
	if !ok {
		return nil, nil, fmt.Errorf("Reduce function has wrong signature")
	}

	return mapFunc, reduceFunc, nil
}
