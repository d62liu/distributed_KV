#include "raft_node.h"
#include "raft/raft_client.h"
#include <algorithm>
#include <fstream>
#include <sstream>

RaftNode::RaftNode(std::string node_id, std::vector<std::string> peers, std::unordered_map<std::string, std::string> peer_addresses, Store& store)
    : node_id(std::move(node_id))
    , peers(std::move(peers))
    , peer_addresses(std::move(peer_addresses))
    , store(store)
    , state(State::Follower)
    , timer([this]() { start_election(); })
    , term_number(0)
    , voted_for("")
    , commit_index(0)
    , last_applied(0)
{
    persist_path = "raft_" + this->node_id + ".state";
    for (const auto& peer : this->peers) {
        clients.try_emplace(peer, peer);
    }
    recover_state();
}

void RaftNode::start_election() {
    std::lock_guard<std::mutex> lock(mu);
    state = State::Candidate;
    ++term_number;
    voted_for = node_id;
    persist_state();

    raft::VoteRequest req;
    req.set_term(term_number);
    req.set_candidate_id(node_id);
    req.set_last_log_index(log.empty() ? 0 : log.back().index());
    req.set_last_log_term(log.empty() ? 0 : log.back().term());

    int votes = 1;
    for (const auto& peer : peers) {
        auto& client = clients.at(peer);
        raft::VoteResponse resp = client.RequestVote(req);
        if (resp.term() > term_number) { step_down(resp.term()); return; }
        if (resp.vote_granted()) ++votes;
    }

    if (votes > static_cast<int>(peers.size() + 1) / 2) {
        state = State::Leader;
        for (const auto& peer : peers) {
            next_index[peer] = log.size();
            match_index[peer] = 0;
        }
        timer.cancel();
        heart_beat.emplace(50, [this]() { send_heartbeat(); });
    }
}

bool RaftNode::request_vote(uint64_t term, const std::string& candidate_id,
                            uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) {
    std::lock_guard<std::mutex> lock(mu);
    if (term < term_number) return false;
    if (voted_for != "" && voted_for != candidate_id) return false;

    uint64_t my_last_term = log.empty() ? 0 : log.back().term();
    uint64_t my_last_index = log.empty() ? 0 : log.back().index();
    if (candidate_last_log_term < my_last_term) return false;
    if (candidate_last_log_term == my_last_term && candidate_last_log_index < my_last_index) return false;

    term_number = term;
    voted_for = candidate_id;
    persist_state();
    timer.reset();
    return true;
}

bool RaftNode::append_entries(const raft::AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mu);
    if (req.term() < term_number) return false;
    term_number = req.term();
    current_leader_id = req.leader_id();
    timer.reset();

    if (req.prev_log_term() != 0) {
        uint64_t prev = req.prev_log_index();
        if (prev >= log.size() || log[prev].term() != req.prev_log_term())
            return false;
    }

    for (const auto& entry : req.entries()) {
        uint64_t idx = entry.index();
        if (idx < log.size()) {
            log[idx] = entry;
            log.resize(idx + 1);
        } else {
            log.push_back(entry);
        }
    }

    persist_state();

    if (req.commit_index() > commit_index) {
        commit_index = std::min(req.commit_index(), (uint64_t)log.size());
        apply_committed();
    }

    return true;
}

bool RaftNode::propose(const Command& cmd) {
    std::lock_guard<std::mutex> propose_lock(propose_mu);

    uint64_t new_entry_index;
    uint64_t local_term;
    {
        std::lock_guard<std::mutex> lock(mu);
        if (state != State::Leader) return false;
        raft::LogEntry entry;
        entry.set_index(log.size());
        entry.set_term(term_number);
        *entry.mutable_command() = cmd;
        log.push_back(entry);
        persist_state();
        new_entry_index = log.size() - 1;
        local_term = term_number;
    }

    int acks = 1;
    for (const auto& peer : peers) {
        auto& client = clients.at(peer);
        bool peer_acked = false;

        while (true) {
            raft::AppendEntriesRequest req;
            {
                std::lock_guard<std::mutex> lock(mu);
                if (state != State::Leader || term_number != local_term) return false;
                uint64_t ni = next_index[peer];
                if (ni > new_entry_index) { peer_acked = true; break; }
                req.set_term(term_number);
                req.set_leader_id(node_id);
                req.set_commit_index(commit_index);
                if (ni > 0) {
                    req.set_prev_log_index(ni - 1);
                    req.set_prev_log_term(log[ni - 1].term());
                }
                for (uint64_t i = ni; i < log.size(); ++i)
                    *req.add_entries() = log[i];
            }

            raft::AppendEntriesResponse resp = client.AppendEntries(req);

            {
                std::lock_guard<std::mutex> lock(mu);
                if (resp.term() > term_number) { step_down(resp.term()); return false; }
                if (state != State::Leader || term_number != local_term) return false;
                if (resp.success()) {
                    match_index[peer] = new_entry_index;
                    next_index[peer] = new_entry_index + 1;
                    peer_acked = true;
                    break;
                }
                if (next_index[peer] == 0) break;
                next_index[peer]--;
            }
        }

        if (peer_acked) ++acks;
    }

    // Phase 3: commit if majority acked
    {
        std::lock_guard<std::mutex> lock(mu);
        if (state != State::Leader || term_number != local_term) return false;
        if (acks > static_cast<int>(peers.size() + 1) / 2) {
            if (new_entry_index < log.size() && log[new_entry_index].term() == local_term) {
                uint64_t new_commit = new_entry_index + 1;
                if (new_commit > commit_index) {
                    commit_index = new_commit;
                    apply_committed();
                }
                return true;
            }
        }
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
    persist_state();
    heart_beat.reset();
    timer.reset();
}

std::string RaftNode::get_leader_address() const {
    std::lock_guard<std::mutex> lock(mu);
    auto it = peer_addresses.find(current_leader_id);
    if (it != peer_addresses.end()) return it->second;
    return "";
}

bool RaftNode::is_leader() const {
    std::lock_guard<std::mutex> lock(mu);
    return state == State::Leader;
}

void RaftNode::persist_state() {
    raft::RaftState rs;
    rs.set_term(term_number);
    rs.set_voted_for(voted_for);
    for (const auto& entry : log) {
        *rs.add_entries() = entry;
    }
    std::string data;
    rs.SerializeToString(&data);
    std::ofstream out(persist_path, std::ios::binary | std::ios::trunc);
    out.write(data.data(), data.size());
}

void RaftNode::recover_state() {
    std::ifstream in(persist_path, std::ios::binary);
    if (!in.good()) return;
    std::ostringstream buf;
    buf << in.rdbuf();
    std::string data = buf.str();
    raft::RaftState rs;
    if (!rs.ParseFromString(data)) return;
    term_number = rs.term();
    voted_for = rs.voted_for();
    for (const auto& entry : rs.entries()) {
        log.push_back(entry);
    }
}

void RaftNode::send_heartbeat() {
    std::lock_guard<std::mutex> lock(mu);
    for (const auto& peer : peers) {
        auto& client = clients.at(peer);
        uint64_t ni = next_index[peer];

        raft::AppendEntriesRequest req;
        req.set_term(term_number);
        req.set_leader_id(node_id);
        req.set_commit_index(commit_index);
        if (ni > 0) {
            req.set_prev_log_index(ni - 1);
            req.set_prev_log_term(log[ni - 1].term());
        }

        raft::AppendEntriesResponse resp = client.AppendEntries(req);
        if (resp.term() > term_number) { step_down(resp.term()); return; }
        if (!resp.success() && ni > 0) next_index[peer]--;
    }
}
