#pragma once
#include "kv_service.grpc.pb.h"
#include "storage/store.h"

class KVServiceImpl : public KVService::Service {
public:
    KVServiceImpl(Store& store);
    grpc::Status Put(grpc::ServerContext*, const PutRequest*, PutResponse*) override;
    grpc::Status Get(grpc::ServerContext*, const GetRequest*, GetResponse*) override;
    grpc::Status Delete(grpc::ServerContext*, const DeleteRequest*, DeleteResponse*) override;
private:
    Store& store;
};
