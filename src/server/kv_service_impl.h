#pragma once
#include "kv_service.grpc.pb.h"
#include "storage/store.h"
#include "raft/raft_node.h"

class KVServiceImpl : public KVService::Service {
private:
    Store& store;
    RaftNode& raft_node;
public:
    KVServiceImpl(Store& store, RaftNode& raft_node);
    grpc::Status Put(grpc::ServerContext*, const PutRequest*, PutResponse*) override;
    grpc::Status Get(grpc::ServerContext*, const GetRequest*, GetResponse*) override;
    grpc::Status Delete(grpc::ServerContext*, const DeleteRequest*, DeleteResponse*) override;
};
