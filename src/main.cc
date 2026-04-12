#include "storage/store.h"
#include "server/kv_service_impl.h"
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

int main(int argc, char* argv[]){
    std::string port = argv[1];
    std::string wal_path = "dkv_" + port + ".wal";

    Store store(16, wal_path);
    store.recover(); 

    KVServiceImpl kv_service(store);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + port, grpc::InsecureServerCredentials());
    builder.RegisterService(&kv_service);

    spdlog::info("dkv-server starting on port {}", port);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait();
}
