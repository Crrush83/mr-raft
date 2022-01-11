package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "strconv"
import "time"

// a lock for it?
//the 
type Coordinator struct {
	// Your definitions here.
	// -1 died 0 idle 1 do_map 2 do_reduce 
	machines map[string]int
	machineLock sync.Mutex
	machineHeartBeat map[string] int64
	machineHeartBeatLock sync.Mutex
	//machine->input []string
	hold_map map[string] []string
	machineMapTaskLock sync.Mutex 
	//machine->key []string
	hold_reduce map[string] []string
	machineReduceTaskLock sync.Mutex
	map_tasks map[string] map_task
	mapTaskLock sync.Mutex
	reduce_tasks [] reduce_task
	reduceTaskLock sync.Mutex
	map_finished bool
	mapFinishNum int
	numMutex sync.Mutex
	reduce_finished bool
	reduceFinishNum int
	mapMutex sync.Mutex
	exit bool
	biglock sync.Mutex
}
type map_task struct{
	//who are doing it
	//state 0:undo 1:doing 2:finished
	inputfile string
	worker string
	state int
}
type reduce_task struct{
	keypartition string
	worker string
	state int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// rpc.go define struct but not function!! why?
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
//why rpc ??
//the worker what do sth with which !!only can do by the coordinator
//the func wrote by coordinator

//

func (c* Coordinator) FirstHeartBeat(ip string,reply *FirstHeartBeatReply)error{
	//if ip non-exist or state is -1
	reply.State = 0;
	c.machineLock.Lock()
	c.machines[ip] = 0;
	c.machineHeartBeat[ip] = time.Now().Unix()
	c.machineLock.Unlock()
	return nil
}
func (c* Coordinator) checkHeartBeat(){
		//跟踪一个客户端的心跳
		ticker := time.NewTicker(1*time.Second)
		for {
			<-ticker.C
			now := time.Now().Unix()
			c.machineLock.Lock()
			c.machineHeartBeatLock.Lock()
			for ip,last := range c.machineHeartBeat {
			//	fmt.Println(ip+"%v",now - last)
				if now - last > 2 {
					fmt.Println(ip + "died")
					delete(c.machineHeartBeat,ip)
					//避免并发访问
					//此时拥有machineLock
					delete(c.machines,ip)
					fmt.Println("delete died machine")
					 
					//c.machines[ip] = -1
					c.machineMapTaskLock.Lock()
					fmt.Println("did the died worker hold map？")
					for _,maptask := range c.hold_map[ip]{
						//核查
						fmt.Println("?check hold map")
						c.mapTaskLock.Lock()
						if c.map_tasks[maptask].state != 2 {
							//不影响finishnum
							c.map_tasks[maptask] = map_task{maptask,"",0}
						}
						c.mapTaskLock.Unlock()
					}
					c.machineMapTaskLock.Unlock()
					c.machineReduceTaskLock.Lock()
					for _,reducetask := range c.hold_reduce[ip]{
						reducetask_int,_ := strconv.Atoi(reducetask)
						c.reduceTaskLock.Lock()
						if c.reduce_tasks[reducetask_int].state != 2{
						c.reduce_tasks[reducetask_int] = reduce_task{reducetask,"",0}	
						c.reduceTaskLock.Unlock()
					}
					}
					c.machineReduceTaskLock.Unlock()
					//delete(c.machines,ip)
				}
			}
			c.machineHeartBeatLock.Unlock()
			c.machineLock.Unlock()
		}	
	}
func (c* Coordinator) HeartBeat(ip string,reply *HeartBeatReply) error{
	c.machineHeartBeatLock.Lock()
	c.machineHeartBeat[ip] = time.Now().Unix()
	c.machineHeartBeatLock.Unlock()
	return nil
}
/*
ret
0:wait and ask a reduce
0:has set ip  should ask for task (repeat first heart beat)
1:a map task
2:a reduce task
3:exit
*/
func (c* Coordinator) AskForTask(ip string,reply *AskForTaskReply)error{
	c.machineLock.Lock()
	_,hasMachine := c.machines[ip]
	c.machineLock.Unlock()
	if hasMachine{
		
		//if map not finished MAYBE distribute a map task
		if !c.map_finished {
			givemaptask := false
			c.mapTaskLock.Lock()
			for _,task := range c.map_tasks{
				if task.state == 0 {
						givemaptask = true
						c.map_tasks[task.inputfile] = map_task{task.inputfile,ip,1}

						reply.Instruc = 1;
						reply.InputFileName = task.inputfile

						c.machineMapTaskLock.Lock()
						//map string ->  list 
						c.hold_map[ip] = append(c.hold_map[ip],task.inputfile)
						c.machineMapTaskLock.Unlock()
						
						c.machineLock.Lock()
						c.machines[ip] = 1;
						c.machineLock.Unlock()
						//fmt.Println(c.map_tasks)
						break
					}
				}
			c.mapTaskLock.Unlock()	
				//break or not find new map
			if !givemaptask {
				//no 0 state map
				//wait for reduce
				//fmt.Println("not give task")
				c.machineLock.Lock()
				c.machines[ip] = 0
				c.machineLock.Unlock()
				reply.Instruc = 0
				}
		} else if !c.reduce_finished {
			//map finished && reduce not finish
			//exist reduce task to distribute
			givereducetask := false
			c.reduceTaskLock.Lock()
			for _,task := range c.reduce_tasks{
				if task.state == 0 {
					givereducetask = true
					patition,_ := strconv.Atoi(task.keypartition)
					c.reduce_tasks[patition] = reduce_task{task.keypartition,ip,1}
					
					c.machineReduceTaskLock.Lock()
					c.hold_reduce[ip] = append(c.hold_reduce[ip],task.keypartition)
					c.machineReduceTaskLock.Unlock()
					reply.Instruc = 2
					fmt.Println("distrobute a reduce")
					reply.InputFileName = task.keypartition
					c.machineLock.Lock()
					c.machines[ip] = 2
					c.machineLock.Unlock()
					break	
				}
			}
			c.reduceTaskLock.Unlock()
			if !givereducetask {
				//reduce not finished 
				//may failed cant exit
				c.machineLock.Lock()
				c.machines[ip] = 0;
				c.machineLock.Unlock()
				reply.Instruc = 0;
			}
		} else {
			//reduce and map ALL finished
			//merge !!
			reply.Instruc = 3
			c.machineLock.Lock()
			c.machines[ip] = 3
			c.machineLock.Unlock()
			exit := true
			c.machineLock.Lock()
			for _,state :=  range c.machines {
				if state != 3 {
					exit = false
					break
				}
			}
			c.machineLock.Unlock()
			c.exit = exit
		}
	} else{
		//重新注册了
		//超时的时候 嗯
		reply.Instruc = 0
		c.machineLock.Lock()
		c.machines[ip] = 1
		c.machineHeartBeat[ip] = time.Now().Unix()
		c.machineLock.Unlock()
	}
		return nil	
	}

func (c *Coordinator)TellState(wkst WorkerState,reply *StateReply)error{
	if wkst.CarryTask  == 0{
		//ack and keep alive
		reply.Ack = 1
	}else if wkst.CarryTask == 1{
		//some state about map task
		reply.Ack = 1
		ip := wkst.MachineIP
		filename := wkst.TaskName
		if wkst.TaskState == 0 {
			//remove task from machine hold map task
			//turn the task's state to 0:need to distribute
			//
		} else if wkst.TaskState == 1{
			//keep it
		} else if wkst.TaskState == 2{
			//change task task to 2
			//delete from machine hold list
			//defer delete:if the machine down and holds some un-finished task delete it and re-diliver

			//
			c.mapTaskLock.Lock()
			c.map_tasks[filename]  = map_task{filename,ip,2}
			c.mapTaskLock.Unlock()
			//if all done map finished!
			//原子++ finished 写时加锁
			c.numMutex.Lock()
			c.mapFinishNum++;
			if c.mapFinishNum == len(c.map_tasks) {
				c.map_finished = true }
			c.numMutex.Unlock()
			fmt.Println("maps finished!")
		}
	}else if wkst.CarryTask == 2{
		reply.Ack = 1
		//some state about reduce task
		keypartition := wkst.TaskName
		ip := wkst.MachineIP
		//atoi
		partition,_ := strconv.Atoi(keypartition)


		if wkst.TaskState == 0 {
			//remove task from machine hold map task
			//turn the task's state to 0:need to distribute
			//
		} else if wkst.TaskState == 1{
			//keep it
		} else if wkst.TaskState == 2{
			//change task task to 2
			//delete from machine hold list
			//defer delete:if the machine down and holds some un-finished task delete it and re-diliver
			c.reduceTaskLock.Lock()
			c.reduce_tasks[partition] = reduce_task{keypartition,ip,2}
			c.reduceTaskLock.Unlock()
			//if all done map finished!
			//原子++ finished 写时加锁
			c.numMutex.Lock()
			c.reduceFinishNum++;
			if c.reduceFinishNum == len(c.reduce_tasks) {
				c.reduce_finished = true }
			c.numMutex.Unlock()
			fmt.Println("reduces finished!")
		}

	}
	return nil
}	

/*
ret
-1:wait and ask a reduce
0:has set ip  should ask for task (repeat first heart beat)
1:a map task
2:a reduce task
3:exit

type AskForTaskReply struct{
	Instruc int
	//or key
	InputFileName string	
}
*/

//
// start a thread that listens for RPCs from worker.go
// bind server() to your coordinator 
// the goroutine which the coordinator.server() run in !!should know which rpc can be call!!
// let goroutine's info include some rpcinfo...
// maybe some 后台任务
//server是小写的 并不会暴露给RPC客户端
func (c *Coordinator) server(){
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
	// 在此之后返回&c 那么用go运行的携程什么时候会结束？
}


// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	exit := false
	c.biglock.Lock()
	exit = c.exit
	c.biglock.Unlock()
	return exit
}
func (c* Coordinator)mergeout(){
	//不需要merge 测试会手动排序
	//通知tuichu
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
	}
	c.machines = make(map[string]int)
	c.machineHeartBeat = make(map[string]int64)
	c.hold_map = make(map[string] []string)
	//machine->key []string
	c.hold_reduce  = make(map[string] []string)
	c.map_tasks = make(map[string] map_task)
	c.reduce_tasks = make([]reduce_task,10)
	c.map_finished = false
	c.reduce_finished = false
	c.mapFinishNum = 0
	c.reduceFinishNum = 0
	c.exit = false

	//input file
	for _,filename := range files{
		c.map_tasks[filename] = map_task{filename,"",0}
	}
	//reduce task
	i := 0
	for i < 10 {
		//i是数字 当然要用%d啦
		sname := fmt.Sprintf("%d",i)
		c.reduce_tasks[i] = reduce_task{sname,"",0}
		i++
	}
	//check input
	//fmt.Println(c.map_tasks)
	//初始化完成 才接受worker的调用



	// Your code here
	go c.checkHeartBeat()
	c.server()
	return &c
	//&是取地址 *是地址类型的说明 虽然结构体是通过地址传的 但使用还是通过.访问
}
