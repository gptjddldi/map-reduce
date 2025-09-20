package main

import (
	"log"
	mapreducerpc "map-reduce/mapreduce-rpc"
	"strings"
)

func main() {
	mapFn := func(data mapreducerpc.KeyValue) []mapreducerpc.KeyValue {
		// Delegate to defaultWordCount in worker if nil; here implement a simple splitter
		counts := make([]mapreducerpc.KeyValue, 0)
		start := -1
		lower := func(b byte) byte {
			if b >= 'A' && b <= 'Z' {
				return b - 'A' + 'a'
			}
			return b
		}
		isAlnum := func(b byte) bool {
			return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
		}
		for i := 0; i <= len(data.Value); i++ {
			if i < len(data.Value) && isAlnum(data.Value[i]) {
				if start == -1 {
					start = i
				}
				continue
			}
			if start != -1 {
				wordBytes := make([]byte, i-start)
				for j := start; j < i; j++ {
					wordBytes[j-start] = lower(data.Value[j])
				}
				counts = append(counts, mapreducerpc.KeyValue{Key: string(wordBytes), Value: data.Key})
				start = -1
			}
		}
		return counts
	}

	reduceFn := func(key string, values []string) mapreducerpc.KeyValue {
		counts := make([]mapreducerpc.KeyValue, 0)
		for _, v := range values {
			counts = append(counts, mapreducerpc.KeyValue{Key: key, Value: v})
		}
		return mapreducerpc.KeyValue{Key: key, Value: strings.Join(values, ",")}
	}

	cluster, err := mapreducerpc.StartCluster("large_file.txt", 10, 3, mapreducerpc.MapFunc(mapFn), mapreducerpc.ReduceFunc(reduceFn))
	if err != nil {
		log.Fatal(err)
	}

	defer cluster.Shutdown()

	// Run the cluster and wait for completion
	cluster.Run()

	log.Println("MapReduce job completed successfully!")
}
