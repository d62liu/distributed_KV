#pragma once
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "raft_service.grpc.pb.h"


class RaftClient{
    std::unique_ptr<raft::RaftService::Stub> stub;
    public:
    RaftClient(std::string peer_address);
    raft::VoteResponse RequestVote(const raft::VoteRequest& req);
    raft::AppendEntriesResponse AppendEntries(const raft::AppendEntriesRequest& req);
};