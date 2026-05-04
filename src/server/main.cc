#include "storage/store.h"
#include "server/kv_service_impl.h"
#include "raft/raft_node.h"
#include "raft/raft_service_impl.h"
#include "config/cluster_config.h"
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

int main(int argc, char* argv[]){
    std::string port = argv[1];
    std::string node_id = argv[2];
    std::string wal_path = "dkv_" + port + ".wal";

    ClusterConfig config;
    std::vector<std::string> peer_addresses;
    for (const auto& peer : config.get_peers(node_id)) {
        peer_addresses.push_back(peer.address);
    }

    Store store(16, wal_path);
    store.recover();

    RaftNode raft_node(node_id, "p1", peer_addresses);
    RaftServiceImpl raft_service(raft_node);

    KVServiceImpl kv_service(store);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + port, grpc::InsecureServerCredentials());
    builder.RegisterService(&kv_service);
    builder.RegisterService(&raft_service);

    spdlog::info("dkv-server starting on port {}", port);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait();
}
