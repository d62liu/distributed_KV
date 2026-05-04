#include "raft_service_impl.h"

RaftServiceImpl::RaftServiceImpl(RaftNode& raft_node) : raft_node{raft_node}{}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext*, const raft::VoteRequest* req, raft::VoteResponse* resp) {
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext*, const raft::AppendEntriesRequest* req, raft::AppendEntriesResponse* resp) {
    return grpc::Status::OK;
}