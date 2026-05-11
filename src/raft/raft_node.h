#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <mutex>
#include <condition_variable>
#include <optional>
#include "util/timer.h"
#include "raft_service.pb.h"
#include "kv_service.pb.h"
#include "storage/store.h"
#include "raft/raft_client.h"

enum class State {
    Follower,
    Candidate,
    Leader
};

class RaftNode {
    std::string node_id;
    std::vector<std::string> peers;
    std::unordered_map<std::string, std::string> peer_addresses;
    Store& store;
    State state;
    Timer timer;
    std::optional<Timer> heart_beat;
    std::string voted_for;
    std::string current_leader_id;
    unsigned term_number;
    std::vector<raft::LogEntry> log;
    uint64_t commit_index;
    uint64_t last_applied;
    std::unordered_map<std::string, RaftClient> clients;
    std::unordered_map<std::string, uint64_t> next_index;
    std::unordered_map<std::string, uint64_t> match_index;
    mutable std::mutex mu;
    std::mutex propose_mu;
    std::condition_variable apply_cv;
    std::string persist_path;

public:
    RaftNode(std::string node_id, std::vector<std::string> peers, std::unordered_map<std::string, std::string> peer_addresses, Store& store);
    void start_election();
    bool request_vote(uint64_t term, const std::string& candidate_id,
                      uint64_t candidate_last_log_index, uint64_t candidate_last_log_term);
    bool append_entries(const raft::AppendEntriesRequest& req);
    bool propose(const Command& cmd);
    std::optional<uint64_t> read_index();
    void wait_apply(uint64_t index);
    std::string get_leader_address() const;
    bool is_leader() const;
    void send_heartbeat();
    void recover_state();

private:
    void step_down(uint64_t term);
    void apply_committed();
    void persist_state();
};
