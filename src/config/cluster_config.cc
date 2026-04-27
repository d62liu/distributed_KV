#include "config/cluster_config.h"

ClusterConfig::ClusterConfig() {
    nodes["node1"] = {"node1", "localhost:50051"};
    nodes["node2"] = {"node2", "localhost:50052"};
    nodes["node3"] = {"node3", "localhost:50053"};

    partitions["p1"] = {"p1", "node1", {"node2", "node3"}};
    partitions["p2"] = {"p2", "node2", {"node1", "node3"}};
    partitions["p3"] = {"p3", "node3", {"node1", "node2"}};
    partitions["p4"] = {"p4", "node1", {"node2", "node3"}};
    partitions["p5"] = {"p5", "node2", {"node1", "node3"}};
    partitions["p6"] = {"p6", "node3", {"node1", "node2"}};
}

const NodeInfo& ClusterConfig::get_node(const std::string& node_id) const {
    return nodes.at(node_id);
}

const PartitionInfo& ClusterConfig::get_partition(const std::string& partition_id) const {
    return partitions.at(partition_id);
}

std::vector<NodeInfo> ClusterConfig::get_peers(const std::string& my_node_id) const {
    std::vector<NodeInfo> peers;
    for (const auto& [id, info] : nodes) {
        if (id != my_node_id) {
            peers.push_back(info);
        }
    }
    return peers;
}
