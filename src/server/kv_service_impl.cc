#include "server/kv_service_impl.h"

KVServiceImpl::KVServiceImpl(Store& store, RaftNode& raft_node)
    : store(store), raft_node(raft_node) {}

grpc::Status KVServiceImpl::Put(grpc::ServerContext*, const PutRequest* req, PutResponse* resp) {
    if (store.is_applied(req->request_id())) {
        resp->set_success(true);
        return grpc::Status::OK;
    }
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
    auto ri = raft_node.read_index();
    if (!ri)
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "not leader: " + raft_node.get_leader_address());
    raft_node.wait_apply(*ri);
    auto value = store.get(req->key());
    resp->set_found(value.has_value());
    if (value) resp->set_value(*value);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Delete(grpc::ServerContext*, const DeleteRequest* req, DeleteResponse* resp) {
    if (store.is_applied(req->request_id())) {
        resp->set_success(true);
        return grpc::Status::OK;
    }
    Command cmd;
    cmd.set_type(Command::DELETE);
    cmd.set_key(req->key());
    cmd.set_request_id(req->request_id());
    bool ok = raft_node.propose(cmd);
    if (!ok) return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "not leader: " + raft_node.get_leader_address());
    resp->set_success(true);
    return grpc::Status::OK;
}
