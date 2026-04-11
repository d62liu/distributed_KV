#include "storage/shard_map.h"
#include "server/kv_service_impl.h"
#include <grpcpp/grpcpp.h>

int main(int argc, char* argv[]){
    ShardedTable shard_map(16);
    KVServiceImpl kv_service(shard_map);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + std::string(argv[1]), grpc::InsecureServerCredentials());
    builder.RegisterService(&kv_service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait(); 
}

