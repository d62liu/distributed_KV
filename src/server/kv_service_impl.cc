#include "server/kv_service_impl.h"

KVServiceImpl::KVServiceImpl(Store& store, RaftNode& raft_node)
    : store(store), raft_node(raft_node) {}

grpc::Status KVServiceImpl::Put(grpc::ServerContext*, const PutRequest* req, PutResponse* resp) {
    Command cmd;
    cmd.set_type(Command::PUT);
    cmd.set_key(req->key());
    cmd.set_value(req->value());
    cmd.set_request_id(req->request_id());
    bool ok = raft_node.propose(cmd);
    if (!ok) return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "not leader: " + raft_node.get_leader_address());
    resp->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Get(grpc::ServerContext*, const GetRequest* req, GetResponse* resp) {
    if (!raft_node.is_leader())
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "not leader: " + raft_node.get_leader_address());
    std::string value = store.get(req->key());
    bool found = !value.empty();
    resp->set_found(found);
    if (found) resp->set_value(value);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Delete(grpc::ServerContext*, const DeleteRequest* req, DeleteResponse* resp) {
    Command cmd;
    cmd.set_type(Command::DELETE);
    cmd.set_key(req->key());
    cmd.set_request_id(req->request_id());
    bool ok = raft_node.propose(cmd);
    if (!ok) return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "not leader: " + raft_node.get_leader_address());
    resp->set_success(true);
    return grpc::Status::OK;
}
