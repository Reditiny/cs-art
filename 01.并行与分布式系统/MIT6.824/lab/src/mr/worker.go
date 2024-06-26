package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	currentDir, _ := os.Getwd()

	wg := sync.WaitGroup{}
	mapNum := 0
	for {
		if isMapJobDone() {
			break
		}
		wg.Add(1)
		mapNum++
		go func(mapNum int) {
			mapWork(mapf, mapNum, currentDir)
			wg.Done()
		}(mapNum)
	}
	wg.Wait()

	fmt.Println("Map Job Done")

	for {
		if isReduceJobDone() {
			break
		}
		wg.Add(1)
		go func() {
			reduceWork(reducef, currentDir)
			wg.Done()
		}()
	}
	wg.Wait()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func isReduceJobDone() bool {
	args := IsJobFinishedArgs{}
	reply := IsJobFinishedReply{}
	call("Coordinator.IsReduceJobFinished", &args, &reply)
	return reply.Finished
}

func reduceWork(reducef func(string, []string) string, currentDir string) {
	// 获取 Reduce 任务
	reply := getReduceJob()
	if !reply.HasJob {
		return
	}
	// 处理 Reduce 任务
	tempFileName := "mr-*-%d"
	wordCount := make(map[string][]string)
	fileNames := make([]string, 0)
	matches, _ := filepath.Glob(filepath.Join(currentDir, fmt.Sprintf(tempFileName, reply.ReduceJobKey)))
	for _, fileName := range matches {
		fileContent, err := os.ReadFile(fileName)
		if err != nil {
			fmt.Printf("read file %v failed %s\n", fileName, err.Error())
			continue
		}
		lines := strings.Split(string(fileContent), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.Split(line, " ")
			if _, ok := wordCount[kv[0]]; !ok {
				wordCount[kv[0]] = make([]string, 0)
			}
			wordCount[kv[0]] = append(wordCount[kv[0]], kv[1])
		}
		fileNames = append(fileNames, fileName)
	}

	// 完成 Reduce 任务，任务成功则保存 k-v 结果，并删除临时文件
	ok := finishReduceJob(reply.ReduceJobKey)
	if ok {
		ofile, _ := os.Create(filepath.Join(currentDir, fmt.Sprintf("mr-out-%d", reply.ReduceJobKey-1)))
		strs := make([][]string, 0)
		for k, v := range wordCount {
			strs = append(strs, []string{k, reducef(k, v)})
		}
		sort.Slice(strs, func(i, j int) bool {
			return strs[i][0] < strs[j][0]
		})
		for _, kv := range strs {
			fmt.Fprintf(ofile, "%v %v\n", kv[0], kv[1])
		}
		for _, fileName := range fileNames {
			os.Remove(fileName)
		}
		fmt.Println("Reduce Job Done")
	}
}

func finishReduceJob(key int) bool {
	args := FinishJobArgs{
		ReduceJobKey: key,
	}
	reply := FinishJobReply{}
	call("Coordinator.FinishReduceJob", &args, &reply)
	return reply.Success

}

func isMapJobDone() bool {
	args := IsJobFinishedArgs{}
	reply := IsJobFinishedReply{}
	call("Coordinator.IsMapJobFinished", &args, &reply)
	return reply.Finished
}

func mapWork(mapf func(string, string) []KeyValue, mapNum int, currentDir string) {
	// 获取 Map 任务
	reply := getMapJob()
	if !reply.HasJob {
		return
	}
	// 处理 Map 任务，生成临时文件
	fileName := reply.MapJobKey
	fileContent, _ := os.ReadFile(fileName)
	values := mapf("", string(fileContent))
	tmepFileMap := make(map[string]*os.File)
	for _, kv := range values {
		tempFileName := fmt.Sprintf("mr-%d-%d", mapNum, ihash(kv.Key)%10+1)
		var curFile *os.File
		if val, ok := tmepFileMap[tempFileName]; !ok {
			curFile, _ = os.OpenFile(filepath.Join(currentDir, tempFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			tmepFileMap[tempFileName] = curFile
		} else {
			curFile = val
		}
		curFile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
	}
	// 完成 Map 任务，若失败则删除临时文件
	ok := finishMapJob(fileName)
	if !ok {
		for _, file := range tmepFileMap {
			file.Close()
			os.Remove(file.Name())
		}
	}
}

func getMapJob() GetJobReply {
	args := GetJobArgs{}
	reply := GetJobReply{}
	call("Coordinator.GetMapJob", &args, &reply)
	if reply.HasJob {
		fmt.Printf("GetMapJob: %v\n", reply.MapJobKey)
	}
	return reply
}

func getReduceJob() GetJobReply {
	args := GetJobArgs{}
	reply := GetJobReply{}
	call("Coordinator.GetReduceJob", &args, &reply)
	if reply.HasJob {
		fmt.Printf("GetReduceJob: %v\n", reply.ReduceJobKey)
	}
	return reply
}

func finishMapJob(MapFileName string) bool {

	args := FinishJobArgs{
		MapJobKey: MapFileName,
	}
	reply := FinishJobReply{}
	call("Coordinator.FinishMapJob", &args, &reply)
	return reply.Success
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.TempFunc", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
