package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int,    // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {

	file, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("read file: ", err)
	}

	files := make([] *os.File, nReduce) // create intermediate files
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTask, i)
		files[i], err = os.Create(fileName)
		if err != nil {
			log.Fatal("creating file: ", err)
		}
	}

	keyValuePairs := mapF(inFile, string(file))

	for _, kv := range keyValuePairs {
		_ = json.NewEncoder(files[ihash(kv.Key)%nReduce]).Encode(kv)
	}

	for _, file := range files {
		file.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
