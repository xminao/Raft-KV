# Raft-KV
### RequestAppendEntries

发送AppendEntries RPC的函数，主要发送心跳，接收一个AppendEntrie。

收到的AppendEntries分几种情况，按照顺序优先处理。

1. 无论是什么角色，收到的Entry任期大于当前节点任期，当前节点转为Follower，更新当前的任期；
2. 如果是Candidate角色，收到Entry，和当前任期号相同，说明有一个节点已经赢得选举，当前节点角色转为Follower；
3. 如果是Follower角色，收到Entry，重置选举定时器；
4. 如果是Leaer角色，不作任何反应。  

  

  



### RequestVote

一个候选人发起选举收集选票，需要过半的服务器支持自己才能成为新的Leader。

相同的任期内（任期号相同）有多个Candidate，但是对于投票的Follower来说只有一票，投给最先来的请求，通过VoteFor字段保证。

处理流程：

1. 因为收到了RPC，重置选举定时器；
2. 判断发送请求的任期是否更新，更新就转换为Follower并重置投票和任期；
3. 如果角色是Follower，判断当前节点是否未投票，如果未投就投给收到的第一个节点，如果投了就是给反对票；
4. 如果是Candidate和Leader，直接给反对票。
