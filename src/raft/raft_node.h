#pragma once
#include <string>
#include <vector>
#include <functional>
#include <cstdint>
#include "util/timer.h"

enum class State {
    Follower,
    Candidate,
    Leader
};

class RaftNode {
    std::string node_id;
    std::string partition_id;
    std::vector<std::string> peers;
    State state;
    Timer timer;
    std::string voted_for;
    unsigned term_number;

public:
    RaftNode(std::string node_id, std::string partition_id, std::vector<std::string> peers)
        : node_id(std::move(node_id))
        ,partition_id(std::move(partition_id))
        ,peers(std::move(peers))
        ,state(State::Follower)
        ,timer([this]() { start_election(); })
        ,term_number(0)
        ,voted_for("")
    {}
    void start_election();
    bool request_vote(uint64_t term, const std::string& candidate_id);
    bool append_entries(uint64_t term, const std::string& leader_id);
    void send_heartbeat();

};
