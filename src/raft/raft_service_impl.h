#pragma once
#include "raft_service.grpc.pb.h"
#include "raft_node.h"



class RaftServiceImpl : public raft::RaftService::Service {
private:
    RaftNode& raft_node;
public:
    RaftServiceImpl(RaftNode& raft_node);
    grpc::Status RequestVote(grpc::ServerContext*, const raft::VoteRequest* req, raft::VoteResponse* resp) override;
    grpc::Status AppendEntries(grpc::ServerContext*, const raft::AppendEntriesRequest* req, raft::AppendEntriesResponse* resp) override;
};