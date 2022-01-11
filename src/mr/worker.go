package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "math/rand"

import "os"
import "io/ioutil"

import "sort"
import "path"
import "encoding/json"
import "context"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type KeyValuelist struct{
	Key string
	Valuelist []string
}
type KeyValuenum struct{
	Key string
	Valuenum int
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// for sorting by key.
type ByKeyValuelist []KeyValuelist

// for sorting by key.
func (a ByKeyValuelist) Len() int           { return len(a) }
func (a ByKeyValuelist) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeyValuelist) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
type workerInfo struct{
	machineIP string
	//input files
	mapTasks []string
	//keys
	reduceTasks []string
}

func (worker * workerInfo)dowork(mapf func(string, string) []KeyValue,
reducef func(string, []string) string){
//	fmt.Println("in the new routine!")
	 //should we need a single goroutine to keep alive?
	ctx,cancel := context.WithCancel(context.Background())
	 defer cancel()
 go worker.sendHeartBeat(ctx)
	 for true {
	//	r := HeartBeatReply{}
	//call("Coordinator.HeartBeat",worker.machineIP,&r)
	//r := HeartBeatReply{}
	//call("Coordinator.HeartBeat",worker.machineIP,&r)
	reply := worker.askForTask()
		if(worker.resolveAskTaskReply(&reply,mapf,reducef) == 3) {
			break
		}
	 }
	 //ch <- 3
}
func (worker* workerInfo)sendHeartBeat(ctx context.Context){

	reply := HeartBeatReply{}
for{
	ticker := time.NewTicker(1*time.Second)
		select{
	case <-ctx.Done():
		fmt.Println("!!!"+worker.machineIP + "exit")
		os.Exit(0)
	default:
		<-ticker.C
			call("Coordinator.HeartBeat",worker.machineIP,&reply)
	}
}
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		//dowork停止的时候heartbeat也停止
		worker := workerInfo{}
		if worker.firstHeartBeat() == true {	
		worker.dowork(mapf,reducef)
		}
}


/*
type WorkerState struct{
	MachineIP string
	CarryTask int
	TaskName string
	TaskState int
}
*/
//save to local
func(worker* workerInfo) doReduce(keypartition string,
	reducef func(string, []string) string){
	
		files, _ := ioutil.ReadDir("/root/mit6.824/src/main/")
		filenames := []string{}
		match := fmt.Sprintf("*-%v",keypartition)
		for _,file := range files {
			match,_ := path.Match(match,file.Name())
			if(match){
				filenames = append(filenames,"../"+file.Name())
			}
		}

		//open files and collect keyvaluelists
		keyvaluelists := []KeyValuelist{}
		for _,filename := range filenames {
		//fmt.Println("reducetask's file\n")
		//everytime open a file read a line and dispach it into (string,stringlist) then call reduce
		//convert it into (string,string)
		//convert in into type KeyValuenum
		//sort 
		//convert it to string,nums
		//file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		//这里不能用append打开吧 要从头开始读
		//os.Open(filename)
			//file, err := os.OpenFile(filename, os.O_RDONLY|os.O_WRONLY, 0644)			
			file, err := os.Open(filename)
					if err != nil {
						log.Fatal(err)
					}
			dc := json.NewDecoder(file)
			partkeyvaluelists := []KeyValuelist{}
			dc.Decode(&partkeyvaluelists)
			keyvaluelists = append(keyvaluelists,partkeyvaluelists...)
			file.Close()
		}

		sort.Sort(ByKeyValuelist(keyvaluelists))
		compresskeyvaluelists := []KeyValuelist{}
		keyindex := 0
		nextkeyindex:=0
		for keyindex < len(keyvaluelists){
			if(nextkeyindex < len(keyvaluelists) && keyvaluelists[nextkeyindex].Key == keyvaluelists[keyindex].Key){
				nextkeyindex++
			}else{
				compressvalue := []string{}
				cpl := keyindex
				for cpl < nextkeyindex{
					compressvalue = append(compressvalue,keyvaluelists[cpl].Valuelist...)
					cpl++
				}
				compresskeyvaluelists = append(compresskeyvaluelists,KeyValuelist{keyvaluelists[keyindex].Key,compressvalue})
				keyindex = nextkeyindex
			}
		}

				//write reduceout to file
		oname := fmt.Sprintf("mr-out-%v",keypartition)
				//trancate:func Create(name string) (*File, error)
		ofile,_ := os.Create(oname)
		keyindex = 0
		//fmt.Println(len(compresskeyvaluelists))
		for keyindex < len(compresskeyvaluelists){
			fmt.Fprintf(ofile,"%v %v\n",compresskeyvaluelists[keyindex].Key,reducef(compresskeyvaluelists[keyindex].Key,compresskeyvaluelists[keyindex].Valuelist))
			//fmt.Println(compresskeyvaluelists[keyindex].Key+reducef(compresskeyvaluelists[keyindex].Key,compresskeyvaluelists[keyindex].Valuelist))
			keyindex++
		}
		//close
			if err := ofile.Close(); err != nil {
				log.Fatal(err)
			}else{
				fmt.Println("out!")
			}
	}
func(worker* workerInfo) doMap(filename string,
	mapf func(string, string) []KeyValue){
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", filename)
					}
					file.Close()
					//save as : keyvaluelists
					//savedata := []KeyValuelist{}
					//we need a json encoder
					//enc := json.NewEncoder(file) every time 
					//intermidateJson := []byte{}
					kva := mapf(filename, string(content))
					//有点东西啊？
					sort.Sort(ByKey(kva))
					//kva为有序格式
					if len(kva) > 0{
					processKeyIndex := 0
					nextKeyIndex := 0;
					reducePart := 0
					//memory is big enough!
					particulerReducePart:= make([][]KeyValuelist,10)
					

				// If the file doesn't exist, create it, or append to the file

					for processKeyIndex < len(kva) {
						
						if nextKeyIndex < len(kva) && kva[nextKeyIndex].Key == kva[processKeyIndex].Key {
							//nextKayIndex == len 或者出现下一个key 都会在else里改变ProcessKeyIndex
							nextKeyIndex++
						} else{
							//processKey ~ nextKeyIndex-1 全部为processKey分区
							//写入到对应ReducePart文件
							wl := processKeyIndex
							//merge string
							output :=[]string{}
							for wl < nextKeyIndex {
								output = append(output,kva[wl].Value)	
								wl++
							}

						//output: a key's valuelist
						//define as keyvaluelist type
						//encode json then write to file
						akeyvluelist := KeyValuelist{kva[processKeyIndex].Key,output}
						//we dont encode here just put it into paticuler list
						reducePart = ihash(kva[processKeyIndex].Key)%10
						particulerReducePart[reducePart] = append(particulerReducePart[reducePart],akeyvluelist)
						processKeyIndex = nextKeyIndex
						}
					}


					//do encode here
					reducePart = 0
					for reducePart < 10{
						//save to file
						oname := fmt.Sprintf("%v-%d",filename,reducePart)
													//trancate:func Create(name string) (*File, error)
						ofile,err := os.Create(oname)
						os.Chmod(oname,0644)
						//ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
						if err != nil {
								log.Fatal(err)
									}
						ec := json.NewEncoder(ofile)
						ec.Encode(&(particulerReducePart[reducePart]))		
						//close
						if err := ofile.Close(); err != nil {
								log.Fatal(err)
							}

							reducePart++
					}
					} 
	}
func(worker* workerInfo) resolveAskTaskReply(reply *AskForTaskReply,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) int{
	if reply.Instruc == 0{
		//new machine
		worker.firstHeartBeat()
	}else if reply.Instruc == 1{
		filename := reply.InputFileName
		worker.doMap(filename,mapf)
		//the map done should told coordinator
		worker.tellMapDone(filename)
	 } else if reply.Instruc == 2{
		keypartition := reply.InputFileName
		worker.doReduce(keypartition,reducef)
		worker.tellReduceDone(keypartition)
	 } else {
		 if(reply.Instruc == 3){
			 return 3
		 } else{
			 //告知空转
			 //Instruc == 0
			 return 0
		 }
	 }
	 return reply.Instruc
}
func(worker* workerInfo)tellMapDone(filename string){
	wkst := WorkerState{
		worker.machineIP,
		1,
		filename,
		2,
	}
	reply := StateReply{0}
	call("Coordinator.TellState",wkst,&reply)

	//check if recieve
	if reply.Ack != 1 {
		fmt.Println("coor do not ack")
	}
	//fmt.Println("map done"+filename)
}
func (worker* workerInfo)tellReduceDone(keypartition string){
	wkst := WorkerState{
		worker.machineIP,
		2,
		keypartition,
		2,
	}
	reply := StateReply{0}
	call("Coordinator.TellState",wkst,&reply)
	if reply.Ack != 1 {
		fmt.Println("coor do not ack")
	}
	//fmt.Println("reduce done"+keypartition)

}
/*
ret
0:has set ip  should ask for task (repeat first heart beat)
1:a map task
2:a reduce task
3:exit
*/
func (worker* workerInfo)askForTask() AskForTaskReply{
	reply := AskForTaskReply{-1,"non"}
	call("Coordinator.AskForTask",worker.machineIP,&reply)
	//translate
	//fmt.Println(reply)
	return reply
}
func (worker* workerInfo)firstHeartBeat() bool{
	ip := makeRandIp()
	conn_reply := FirstHeartBeatReply{}
	retry := 5
	for !call("Coordinator.FirstHeartBeat",ip,&conn_reply) && retry > 0 {
		ip = makeRandIp()
		conn_reply = FirstHeartBeatReply{}
		retry--
	}
	fmt.Println("ip:"+ ip +"and state is"+fmt.Sprint(conn_reply.State))
	if(conn_reply.State != 0) {
		return false}
	worker.machineIP = ip	
	return true
}
func makeRandIp() string{
			//TODO check ip's validity
			//创造不同ip
			rand.Seed(time.Now().UnixNano())
	ip := fmt.Sprintf("%d.%d.%d.%d",rand.Intn(255),
	rand.Intn(255),rand.Intn(255),rand.Intn(255))
	return ip
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
