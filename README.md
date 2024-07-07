Go Distributed File System

## Roadmap

- [ ] **Membership**
  - [ ] Able to join and leave a group.
  - [ ] Handling node failures and network partitions.
  - [ ] Heartbeat

- [ ] **File Client and Server**
  - [x] File operations for Client:
    - [x] `put` (upload)
    - [x] `get` (download)
    - [x] `remove` (delete)
    - [x] `locate` (find)
    - [x] `ls` (list all files)
    - [x] `lshere` (list files at current node)
  - [ ] Server manage file storage and respond to client requests.

- [ ] **User Commands**
    - [ ] Initializing a new group
    - [ ] Joining an existing group
    - [ ] Leaving the group
    - [ ] File operations (`put`, `get`, `remove`, `locate`, `ls`, `lshere`)

- [ ] **Replication and Consistency**
  - [ ] Data replication across multiple nodes.
  - [ ] Consistency for replicated data.
  - [ ] Sync for file operations.

- [ ] **Testing**