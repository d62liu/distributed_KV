#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include "util/timer.h"
#include "raft_service.pb.h"
#include "kv_service.pb.h"
#include "storage/store.h"

enum class State {
    Follower,
    Candidate,
    Leader
};

class RaftNode {
    std::string node_id;
    std::string partition_id;
    std::vector<std::string> peers;
    Store& store;
    State state;
    Timer timer;
    std::optional<Timer> heart_beat;
    std::string voted_for;
    unsigned term_number;
    std::vector<raft::LogEntry> log;
    uint64_t commit_index;
    uint64_t last_applied;

public:
    RaftNode(std::string node_id, std::string partition_id, std::vector<std::string> peers, Store& store);
    void start_election();
    bool request_vote(uint64_t term, const std::string& candidate_id);
    bool append_entries(const raft::AppendEntriesRequest& req);
    bool propose(const Command& cmd);
    void send_heartbeat();

private:
    void step_down(uint64_t term);
    void apply_committed();
};
