#include "raft_service_impl.h"

RaftServiceImpl::RaftServiceImpl(RaftNode& raft_node) : raft_node{raft_node}{}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext*, const raft::VoteRequest* req, raft::VoteResponse* resp) {
    bool granted = raft_node.request_vote(req->term(), req->candidate_id(),
                                          req->last_log_index(), req->last_log_term());
    resp->set_vote_granted(granted);
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext*, const raft::AppendEntriesRequest* req, raft::AppendEntriesResponse* resp) {
    bool success = raft_node.append_entries(*req);
    resp->set_success(success);
    return grpc::Status::OK;
}