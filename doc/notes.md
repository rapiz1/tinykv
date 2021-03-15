# Notes

## Project 1: KV Server

这一部分主要是熟悉 go 语法和 badger 的用法，仔细阅读项目目录和操作定义，按照要求实现即可，比较简单。

## Project 2: Raft

仔细阅读和理解 Raft 论文后，基本没有问题。但是一些没有定义清楚的细节需要根据测试推测。

### 2A

实现 raft 算法。

- 论文中并不存在 Heartbeat。这会引起比较多的细节问题，需要面向测试编程。

- raftlog storage 的定义不清楚，需要根据测试反推它的行为。https://github.com/tidb-incubator/tinykv/pull/170 可以一定程度上解决这个问题。

- 领导人选举时，如果根据已知投票情况可以得出结果，需要提前结束选举。

- 由于 Heartbeat 和 AppendEntries 是分开的，所以 Leader 在收到 HeartbeatResponse 后需要考虑是否发送 AppendEntries。这比 raft 论文中要复杂。

### 2B

实际上是处理 raft 算法与 client 交互的部分。raft phd 论文中有关于 Client Interaction 的讨论，读了之后会理解更深。

- 2B 中传入的 raft command 虽然定义中可以有多个请求，但实际上只会有一个，因此只需处理一个的情况。

- 大部分 raft command 都会对应一个 callback 用来返回响应。但不是所有请求都有响应，有些请求对应的 raft log entry 可能未 commit 就被覆盖了，这种请求永远也不会得到响应。propose 时需要处理这些失效的请求的 callback。

### 2C

实现 snapshot。

- raft 论文有两版，其中一版比较详细，包含多个扩展。2C 中 snapshot 的细节可以搜索 raft phd 论文。**这里要注意，如果 /tmp 是用 tmpfs 挂载的，那么 applySnapshot 会永远阻塞。**可以让 badger 不使用 directio 来解决。

## Project 3: MultiRaft

这是最难的一部分，项目的结构很复杂，需要阅读的代码量很大。尤其是关于 Region 的创建、删除等管理的流程和源码，里面各种的对象，参数传递和 channel 沟通很复杂，我没有完全理解。最后实现的 Split 还是有可能出错。

### 3A

- 3A 实现 leader transfer 和 membership change。这部分需要阅读 raft phd 论文后结合自己的理解实现。

### 3B

- 3B 实现 LeaderTransfer, ConfChange 的 Client Interaction 部分。还需要支持 MultiRaft 的 Split 操作。关于 Region 的操作我不太懂，不知道要去哪里维护哪些信息。最后这部分有测试不能通过。

### 3C

完全根据文档描述和测试实现调度算法即可。

## Project 4: Transaction

实现事务接口。

根据文档描述迷迷糊糊做完了，编码难度比较小，感觉没有深刻理解设计背后的原因。这部分或许应该需要阅读更多的文献。

## 一些感想

### 收获

完成了 tinykv, 我感觉自己对 raft 算法的工作流程已经很熟悉。但是实际上 raft 算法的简单性也是 raft 算法的缺陷。因为我在 tinykv 中遇到的困难，难以调试的 bug，实际上都是 raft 算法管辖之外的部分，但这些部分对于一个完整的分布式系统来说实际上是不可缺少的。raft 算法只是一个共识算法。也可以说，raft 算法把复杂性放在了它自己之外。raft phd 论文中提到，作者发现 raft 实现最容易出 bug 的地方，不是 Log Replication, Leader Election 这些环节，而是 Client Interaction。

### 关于 TinyKV

总体来说，tinykv 的难度还是很大的，留给学习者的空白很多。我的 RaftLog 为了通过测试经过了无数次重构，因为很难知道究竟什么是预期行为。一些同学也有同感。不可否认的是，如果真的能把 TinyKV 完全实现和调优，估计上手实际分布式数据库的代码会非常快。但这一过程想必要付出非常多的时间。我做 TinyKV 的时间大概有三个星期，3B 中间一度想要放弃，最后想着善始善终，还是把 3B 的 Bug 跳过，继续往后实现。

总之，如果对于想要从事分布式数据库开发的同学，我认为 TinyKV 是非常好的练手。如果是一般的想要了解分布式系统的同学，TinyKV 的留白和需要的时间可能还是大了一点。