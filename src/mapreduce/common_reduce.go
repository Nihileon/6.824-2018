package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//首先已经map好的文件中读出数据到一个中间数据结构中
	//然后将中间数据结构的key排好序
	//最后重新将中间数据结构序列化输出为文件
	files := make([] *os.File, nMap)
	for i := 0; i < nMap; i++ {
		files[i], _ = os.Open(reduceName(jobName, i, reduceTask))
	}

	interKVs := make(map[string][]string)
	for _, inputFile := range files {
		defer inputFile.Close()
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			interKVs[kv.Key] = append(interKVs[kv.Key], kv.Value)
		}
	}
	keys := make([]string, 0, len(interKVs))
	for key := range interKVs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal("create file", outFile)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, interKVs[key])})
	}
}
