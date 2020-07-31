# Lab

MIT 6.824 (Distributed Systems) Spring 2020 labs in Golang

Course website: https://pdos.csail.mit.edu/6.824/

## Lab1 MapReduce

Requriment: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

### Desgin

Both map and reduce tasks are represented by Task struct. And each task belongs to one task manager.

#### Master
Master has two task managers for the Map and Reduce phase.

Two main RPC handlers for the worker are `WorkerConfig` and `NextTask`.

`WorkerConfig` :
* Worker first time connect to master, must call this method to get the necessary configuration.

`NextTask` :
* The worker calls this one to get the next available task, or flags indicate the whole job completed or need wait for other workers.
* In request, the worker need include its ID, and previously completed task info and the result file list.
* After the master receives a request, it first acknowledges the previously completed task return by the worker, if the worker ID of the task matches with records in the manager,  then checks whether the current phase is completed, if yes promote to the next phase. Then the master finds the next available task to return back to the worker. If all tasks are in process, or the job completed then send the correct flag.

#### Worker
Each worker initializes with configuration send by the master. After initialization, it periodically sends the request to the master for the next task, if the request fails and excess max request retries then assume master dies, terminate itself. After the worker gets the next task it calls the corresponding task handler. If any error happens in the middle of the processing the task would cause the task to fail, the worker does not generate the partial results and send back to the master. The worker may receive a request from the master to wait for other workers or to terminate since the job has completed. 


## Lab2 Raft

- [x] Part A: Implement Raft leader election and heartbeats
- [X] Part B: Implement the leader and follower code to append new log entries
- [X] Part C: Raft persistent state
