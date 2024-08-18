# Go Distributed File System

- The distributed file system is designed for basic file handling operations including write, read, delete and update. 
- Utilizes Master Server architecture to handle the operations and the master is selected through the leader election process. 
- Tolerate multiple node failures without impacting file availability and file handling functionalities. Every file is written onto 3 nodes asynchronously via an active replication process to maintain 3 replicas of the file in case of node failure. The put and read operation make use of Simple Write Quorum wherein R = W = 2. 
- Utilizes heartbeat monitoring for failure detection. Every node sends and receives heartbeat messages from 2 successor and 2 predecessor nodes in a ring topology.

## Roadmap

- [ ] **Membership (A group of nodes can form a membership.)**
  - [ ] Determine leader to be the node that starts the membership
  - [ ] Each membership is a replica for the file service
  - [ ] Able to join and leave a group.
  - [ ] Handling node failures and network partitions.
  - [ ] Heartbeat to detect failures as shown

- [ ] **File Client and Server**
  - [x] File operations for Client:
    - [x] `put` (upload)
    - [x] `get` (download)
    - [x] `remove` (delete)
    - [x] `locate` (find)
    - [x] `ls` (list all files)
    - [x] `lshere` (list files at current node)
  - [ ] Server manage file storage and respond to client requests.

- [ ] **File Operation and Membership Commands**
    - [ ] Initializing a new group
    - [ ] Joining an existing group
    - [ ] Leaving the group
    - [ ] File operations (`put`, `get`, `remove`, `locate`, `ls`, `lshere`)

- [ ] **Replication and Consistency**
  - [ ] Data replication across multiple nodes.
  - [ ] Consistency for replicated data.
  - [ ] Sync for file operations.

## Testing
Tested on AWS with 10 nodes. Below is the report showing the experiments of all operations against different file size using the system deployed on four AWS instances.
