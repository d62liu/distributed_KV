#include "storage/shard_map.h"
#include <xxhash.h>
#include <iostream>

ShardedTable::ShardedTable(int num_shards) : numShards{num_shards} {
    for (int i = 0; i < num_shards; ++i) {
        std::string id = std::to_string(i);
        uint64_t id_hash = get_hash(id);
        shard_map.emplace(std::piecewise_construct,
            std::forward_as_tuple(id_hash),
            std::forward_as_tuple());
        shard_map[id_hash].id = id;
        sorted_key.insert(id_hash);
    }
}

uint64_t ShardedTable::get_hash(const std::string& key) {
    return XXH64(key.data(), key.size(), 0);
}

std::string ShardedTable::get(const std::string& key) {
    uint64_t hash_val = XXH64(key.data(), key.size(), 0);
    auto it = sorted_key.lower_bound(hash_val);
    if (it == sorted_key.end()) it = sorted_key.begin(); // wrap around the ring
    std::shared_lock<std::shared_mutex> lock(shard_map[*it].mu);
    try {
        return shard_map[*it].data.at(key);
    } catch (const std::out_of_range&) {
        std::cerr << "Key not found" << std::endl;
        return "";
    }
}

void ShardedTable::put(const std::string& key, const std::string& value) {
    uint64_t hash_val = XXH64(key.data(), key.size(), 0);
    auto it = sorted_key.lower_bound(hash_val);
    if (it == sorted_key.end()) it = sorted_key.begin();
    std::unique_lock<std::shared_mutex> lock(shard_map[*it].mu);
    shard_map[*it].data[key] = value;
}
