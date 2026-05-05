#include "raft_node.h"
#include "raft/raft_client.h"
#include <algorithm>

RaftNode::RaftNode(std::string node_id, std::string partition_id, std::vector<std::string> peers, Store& store)
    : node_id(std::move(node_id))
    , partition_id(std::move(partition_id))
    , peers(std::move(peers))
    , store(store)
    , state(State::Follower)
    , timer([this]() { start_election(); })
    , term_number(0)
    , voted_for("")
    , commit_index(0)
    , last_applied(0)
{}

void RaftNode::start_election() {
    state = State::Candidate;
    ++term_number;
    voted_for = node_id;

    raft::VoteRequest req;
    req.set_term(term_number);
    req.set_candidate_id(node_id);

    int votes = 1;
    for (const auto& peer : peers) {
        RaftClient client(peer);
        raft::VoteResponse resp = client.RequestVote(req);
        if (resp.term() > term_number) { step_down(resp.term()); return; }
        if (resp.vote_granted()) ++votes;
    }

    if (votes > static_cast<int>(peers.size() + 1) / 2) {
        state = State::Leader;
        timer.stop();
        heart_beat.emplace(50, [this]() { send_heartbeat(); });
    }
}

bool RaftNode::request_vote(uint64_t term, const std::string& candidate_id) {
    if (term < term_number) return false;
    if (voted_for != "" && voted_for != candidate_id) return false;
    term_number = term;
    voted_for = candidate_id;
    timer.reset();
    return true;
}

bool RaftNode::append_entries(const raft::AppendEntriesRequest& req) {
    if (req.term() < term_number) return false;
    term_number = req.term();
    timer.reset();

    for (const auto& entry : req.entries()) {
        uint64_t idx = entry.index();
        if (idx < log.size()) {
            log[idx] = entry;
            log.resize(idx + 1);
        } else {
            log.push_back(entry);
        }
    }

    if (req.commit_index() > commit_index) {
        commit_index = std::min(req.commit_index(), (uint64_t)log.size());
        apply_committed();
    }

    return true;
}

bool RaftNode::propose(const Command& cmd) {
    if (state != State::Leader) return false;

    raft::LogEntry entry;
    entry.set_index(log.size());
    entry.set_term(term_number);
    *entry.mutable_command() = cmd;
    log.push_back(entry);

    raft::AppendEntriesRequest req;
    req.set_term(term_number);
    req.set_leader_id(node_id);
    req.set_commit_index(commit_index);
    *req.add_entries() = entry;

    int acks = 1;
    for (const auto& peer : peers) {
        RaftClient client(peer);
        raft::AppendEntriesResponse resp = client.AppendEntries(req);
        if (resp.term() > term_number) { step_down(resp.term()); return false; }
        if (resp.success()) ++acks;
    }

    if (acks > static_cast<int>(peers.size() + 1) / 2) {
        commit_index = log.size();
        apply_committed();
        return true;
    }
    return false;
}

void RaftNode::apply_committed() {
    while (last_applied < commit_index) {
        const auto& entry = log[last_applied];
        const auto& cmd = entry.command();
        if (cmd.type() == Command::PUT) {
            store.put(cmd.key(), cmd.value(), cmd.request_id());
        } else if (cmd.type() == Command::DELETE) {
            store.remove(cmd.key(), cmd.request_id());
        }
        ++last_applied;
    }
}

void RaftNode::step_down(uint64_t term) {
    state = State::Follower;
    term_number = term;
    voted_for = "";
    heart_beat.reset();
    timer.reset();
}

void RaftNode::send_heartbeat() {
    raft::AppendEntriesRequest req;
    req.set_term(term_number);
    req.set_leader_id(node_id);
    req.set_commit_index(commit_index);
    for (const auto& peer : peers) {
        RaftClient client(peer);
        raft::AppendEntriesResponse resp = client.AppendEntries(req);
        if (resp.term() > term_number) { step_down(resp.term()); return; }
    }
}
