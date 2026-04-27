#pragma once
#include <string>
#include <vector>
#include <unordered_map>

struct NodeInfo{
    std::string id;
    std::string address;
};

struct PartitionInfo{
    std::string id;
    std::string primary;
    std::vector<std::string> replicas;
};

class ClusterConfig {
  public:                                                                                                                     
      ClusterConfig();

      const NodeInfo& get_node(const std::string& node_id) const;                                                             
      const PartitionInfo& get_partition(const std::string& partition_id) const;
      std::vector<NodeInfo> get_peers(const std::string& my_node_id) const;                                                   
                                                                                                                              
  private:
      std::unordered_map<std::string, NodeInfo> nodes;                                                                        
      std::unordered_map<std::string, PartitionInfo> partitions;
  };  