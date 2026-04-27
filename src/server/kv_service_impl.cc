#include "server/kv_service_impl.h"

KVServiceImpl::KVServiceImpl(Store& store) : store(store) {}

grpc::Status KVServiceImpl::Put(grpc::ServerContext*, const PutRequest* req, PutResponse* resp) {
    store.put(req->key(), req->value(), req->request_id());
    resp->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Get(grpc::ServerContext*, const GetRequest* req, GetResponse* resp) {
    std::string value = store.get(req->key());
    bool found = !value.empty();
    resp->set_found(found);
    if (found) resp->set_value(value);
    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Delete(grpc::ServerContext*, const DeleteRequest* req, DeleteResponse* resp) {
    store.remove(req->key(), req->request_id());
    resp->set_success(true);
    return grpc::Status::OK;
}
