Go Distributed File System

## Roadmap

- [ ] **Membership**
  - [ ] Able to join and leave a group.
  - [ ] Handling node failures and network partitions.
  - [ ] Heartbeat

- [ ] **File Client and Server**
  - [ ] File operations for Client:
    - [ ] `put` (upload)
    - [ ] `get` (download)
    - [ ] `remove` (delete)
    - [ ] `locate` (find)
    - [ ] `ls` (list all files)
    - [ ] `lshere` (list files at current node)
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