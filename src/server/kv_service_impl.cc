#include "server/kv_service_impl.h"

KVServiceImpl::KVServiceImpl(ShardedTable& shard_map) : shard_map(shard_map) {}

grpc::Status KVServiceImpl::Put(grpc::ServerContext*, const PutRequest* req, PutResponse* resp) {
    shard_map.put(req->key(), req->value());
    resp->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Get(grpc::ServerContext*, const GetRequest* req, GetResponse* resp) {
    std::string value = shard_map.get(req->key());
    bool found = !value.empty();
    resp->set_found(found);
    if (found) resp->set_value(value);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Delete(grpc::ServerContext*, const DeleteRequest*, DeleteResponse* resp) {
    resp->set_success(true);
    return grpc::Status::OK;
}
