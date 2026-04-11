#pragma once
#include "kv_service.grpc.pb.h"
#include "storage/shard_map.h"

class KVServiceImpl : public KVService::Service {
public:
    KVServiceImpl(ShardedTable& shard_map);
    grpc::Status Put(grpc::ServerContext*, const PutRequest*, PutResponse*) override;
    grpc::Status Get(grpc::ServerContext*, const GetRequest*, GetResponse*) override;
    grpc::Status Delete(grpc::ServerContext*, const DeleteRequest*, DeleteResponse*) override;
private:
    ShardedTable& shard_map;
};
