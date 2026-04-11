#pragma once
#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <cstdint>

struct Shard {
    std::string id;
    std::unordered_map<std::string, std::string> data;
    std::shared_mutex mu;
};

class ShardedTable {
    int numShards;
    std::unordered_map<uint64_t, Shard> shard_map;
    std::set<uint64_t> sorted_key;
    uint64_t get_hash(const std::string& key);
public:
    ShardedTable(int num_shards);
    ~ShardedTable() = default;
    void addShard();
    void removeShard(uint64_t index);
    int get_node(const std::string& id);
    std::string get(const std::string& key);
    void put(const std::string& key, const std::string& value);
    int getSize();
    int getShards();
};
