#include "storage/shard_map.h"
#include <xxhash.h>

ShardedTable::ShardedTable(int num_shards) : numShards(num_shards), shards(num_shards) {}

uint64_t ShardedTable::get_shard(const std::string& key) {
    return XXH64(key.data(), key.size(), 0) % numShards;
}

std::optional<std::string> ShardedTable::get(const std::string& key) {
    Shard& shard = shards[get_shard(key)];
    std::shared_lock<std::shared_mutex> lock(shard.mu);
    auto it = shard.data.find(key);
    if (it == shard.data.end()) return std::nullopt;
    return it->second;
}

void ShardedTable::put(const std::string& key, const std::string& value) {
    Shard& shard = shards[get_shard(key)];
    std::unique_lock<std::shared_mutex> lock(shard.mu);
    shard.data[key] = value;
}

void ShardedTable::remove(const std::string& key) {
    Shard& shard = shards[get_shard(key)];
    std::unique_lock<std::shared_mutex> lock(shard.mu);
    shard.data.erase(key);
}
