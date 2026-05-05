#include "raft/raft_client.h"
#include <chrono>

RaftClient::RaftClient(std::string peer_address){
    auto channel = grpc::CreateChannel(peer_address, grpc::InsecureChannelCredentials());                                                              
    stub = raft::RaftService::NewStub(channel);
}

//term, candidate_id
raft::VoteResponse RaftClient::RequestVote(const raft::VoteRequest& req){
    grpc::ClientContext ctx;
    raft::VoteResponse resp;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    stub->RequestVote(&ctx, req, &resp);
    return resp;
}

raft::AppendEntriesResponse RaftClient::AppendEntries(const raft::AppendEntriesRequest& req){
    grpc::ClientContext ctx;
    raft::AppendEntriesResponse resp;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    stub->AppendEntries(&ctx, req, &resp);
    return resp;
}