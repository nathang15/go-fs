# Go Distributed File System

- The distributed file system is designed for basic file handling operations including write, read, delete and update. 
- Utilizes Leader-Follower architecture to handle the operations and the master(leader) is selected through the leader election process. 
- Tolerate multiple node failures without impacting file availability and file handling functionalities. Every file is written onto 3 nodes asynchronously via an active replication process to maintain 3 replicas of the file in case of node failure. The put and read operation make use of Simple Write Quorum wherein R = W = 2. 
- Utilizes heartbeat monitoring for failure detection. Every node sends and receives heartbeat messages from 2 successor and 2 predecessor nodes in a ring topology.

## Roadmap

- [x] **Membership (A group of nodes can form a membership.)**
  - [x] Determine leader to be the node that starts the membership
  - [x] Each membership is a replica for the file service
  - [x] Able to join and leave a group.
  - [x] Handling node failures and network partitions.
  - [x] Heartbeat to detect failures as shown

- [x] **File Client and Server**
  - [x] File operations for Client:
    - [x] `put` (upload)
    - [x] `get` (download)
    - [x] `remove` (delete)
    - [x] `locate` (find)
    - [x] `ls` (list all files)
    - [x] `lshere` (list files at current node)
  - [x] Server manage file storage and respond to client requests.

- [x] **File Operation and Membership Commands**
    - [x] Initializing a new group
    - [x] Joining an existing group
    - [x] Leaving the group
    - [x] File operations (`put`, `get`, `remove`, `locate`, `ls`, `lshere`)

- [x] **Replication and Consistency**
  - [x] Data replication across multiple nodes.
  - [x] Consistency for replicated data.
  - [x] Sync for file operations.

## Testing
Deployed on 3 AWS EC2 instances and achieved 35ms for write/update and 10ms for read for 100 Mb file size. Also working when not using AWS, just need to change up the config and main file.
