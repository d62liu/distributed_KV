#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <cstdint>

struct Shard {
    std::unordered_map<std::string, std::string> data;
    std::shared_mutex mu;
};

class ShardedTable {
    int numShards;
    std::vector<Shard> shards;
    uint64_t get_shard(const std::string& key);
public:
    ShardedTable(int num_shards);
    std::string get(const std::string& key);
    void put(const std::string& key, const std::string& value);
    void remove(const std::string& key);
};
