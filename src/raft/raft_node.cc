#include "raft_node.h"
#include "raft/raft_client.h"
#include <algorithm>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

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
    meta_path = "raft_" + this->node_id + ".meta";
    log_path = "raft_" + this->node_id + ".log";
    log_fd = -1;
    stopping = false;
    for (const auto& peer : this->peers) {
        clients.try_emplace(peer, peer);
    }
    recover_state();
    log_fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0) spdlog::error("RaftNode: failed to open {}", log_path);
    apply_thread = std::thread(&RaftNode::apply_loop, this);
}

RaftNode::~RaftNode() {
    {
        std::lock_guard<std::mutex> lock(mu);
        stopping = true;
    }
    apply_cv.notify_all();
    if (apply_thread.joinable()) apply_thread.join();
    if (log_fd >= 0) close(log_fd);
}

void RaftNode::start_election() {
    std::lock_guard<std::mutex> lock(mu);
    state = State::Candidate;
    ++term_number;
    voted_for = node_id;
    persist_meta();

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
    persist_meta();
    timer.reset();
    return true;
}

bool RaftNode::append_entries(const raft::AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mu);
    if (req.term() < term_number) return false;
    bool term_changed = (req.term() != term_number);
    term_number = req.term();
    current_leader_id = req.leader_id();
    timer.reset();

    if (req.prev_log_term() != 0) {
        uint64_t prev = req.prev_log_index();
        if (prev >= log.size() || log[prev].term() != req.prev_log_term())
            return false;
    }

    uint64_t old_size = log.size();
    bool truncated = false;
    for (const auto& entry : req.entries()) {
        uint64_t idx = entry.index();
        if (idx < log.size()) {
            log[idx] = entry;
            log.resize(idx + 1);
            truncated = true;
        } else {
            log.push_back(entry);
        }
    }

    if (term_changed) persist_meta();
    if (truncated) {
        rewrite_log();
    } else {
        for (uint64_t i = old_size; i < log.size(); ++i) {
            persist_log_entry(log[i]);
        }
    }

    if (req.commit_index() > commit_index) {
        commit_index = std::min(req.commit_index(), (uint64_t)log.size());
        apply_cv.notify_all();
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
        persist_log_entry(entry);
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
                    apply_cv.notify_all();
                }
                return true;
            }
        }
    }
    return false;
}

void RaftNode::apply_loop() {
    while (true) {
        std::vector<raft::LogEntry> to_apply;
        uint64_t target;
        {
            std::unique_lock<std::mutex> lock(mu);
            apply_cv.wait(lock, [this]() { return stopping || last_applied < commit_index; });
            if (stopping) return;
            target = commit_index;
            to_apply.reserve(target - last_applied);
            for (uint64_t i = last_applied; i < target; ++i) {
                to_apply.push_back(log[i]);
            }
        }

        for (const auto& entry : to_apply) {
            const auto& cmd = entry.command();
            if (cmd.type() == Command::PUT) {
                store.put(cmd.key(), cmd.value(), cmd.request_id());
            } else if (cmd.type() == Command::DELETE) {
                store.remove(cmd.key(), cmd.request_id());
            }
        }

        {
            std::lock_guard<std::mutex> lock(mu);
            last_applied = target;
        }
        apply_cv.notify_all();
    }
}

std::optional<uint64_t> RaftNode::read_index() {
    uint64_t saved_term;
    uint64_t saved_commit;
    {
        std::lock_guard<std::mutex> lock(mu);
        if (state != State::Leader) return std::nullopt;
        saved_term = term_number;
        saved_commit = commit_index;
    }

    int acks = 1;
    for (const auto& peer : peers) {
        auto& client = clients.at(peer);
        raft::AppendEntriesRequest req;
        req.set_term(saved_term);
        req.set_leader_id(node_id);
        req.set_commit_index(saved_commit);

        raft::AppendEntriesResponse resp = client.AppendEntries(req);

        {
            std::lock_guard<std::mutex> lock(mu);
            if (resp.term() > term_number) { step_down(resp.term()); return std::nullopt; }
            if (state != State::Leader || term_number != saved_term) return std::nullopt;
        }
        if (resp.success()) ++acks;
    }

    if (acks <= static_cast<int>(peers.size() + 1) / 2) return std::nullopt;
    return saved_commit;
}

void RaftNode::wait_apply(uint64_t index) {
    std::unique_lock<std::mutex> lock(mu);
    apply_cv.wait(lock, [this, index]() { return last_applied >= index; });
}

void RaftNode::step_down(uint64_t term) {
    state = State::Follower;
    term_number = term;
    voted_for = "";
    persist_meta();
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

void RaftNode::persist_meta() {
    raft::RaftState rs;
    rs.set_term(term_number);
    rs.set_voted_for(voted_for);
    std::string data;
    rs.SerializeToString(&data);

    std::string tmp = meta_path + ".tmp";
    int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { spdlog::error("RaftNode: failed to open {}", tmp); return; }
    ssize_t n = ::write(fd, data.data(), data.size());
    if (n != static_cast<ssize_t>(data.size())) spdlog::error("RaftNode: short write to {}", tmp);
    fsync(fd);
    close(fd);
    if (rename(tmp.c_str(), meta_path.c_str()) != 0) {
        spdlog::error("RaftNode: rename failed for {}", meta_path);
    }
}

void RaftNode::persist_log_entry(const raft::LogEntry& entry) {
    if (log_fd < 0) return;
    std::string data;
    entry.SerializeToString(&data);
    uint32_t len = static_cast<uint32_t>(data.size());
    ::write(log_fd, &len, 4);
    ::write(log_fd, data.data(), len);
    fsync(log_fd);
}

void RaftNode::rewrite_log() {
    if (log_fd >= 0) { close(log_fd); log_fd = -1; }
    std::string tmp = log_path + ".tmp";
    int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { spdlog::error("RaftNode: failed to open {}", tmp); return; }
    for (const auto& entry : log) {
        std::string data;
        entry.SerializeToString(&data);
        uint32_t len = static_cast<uint32_t>(data.size());
        ::write(fd, &len, 4);
        ::write(fd, data.data(), len);
    }
    fsync(fd);
    close(fd);
    if (rename(tmp.c_str(), log_path.c_str()) != 0) {
        spdlog::error("RaftNode: rename failed for {}", log_path);
    }
    log_fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
}

void RaftNode::recover_state() {
    std::ifstream meta(meta_path, std::ios::binary);
    if (meta.good()) {
        std::ostringstream buf;
        buf << meta.rdbuf();
        raft::RaftState rs;
        if (rs.ParseFromString(buf.str())) {
            term_number = rs.term();
            voted_for = rs.voted_for();
        }
    }

    int fd = open(log_path.c_str(), O_RDONLY);
    if (fd < 0) return;
    while (true) {
        uint32_t len = 0;
        if (::read(fd, &len, 4) != 4) break;
        std::string data(len, '\0');
        if (::read(fd, data.data(), len) != static_cast<ssize_t>(len)) {
            spdlog::warn("RaftNode: truncated log entry, stopping replay");
            break;
        }
        raft::LogEntry entry;
        if (!entry.ParseFromString(data)) {
            spdlog::warn("RaftNode: failed to parse log entry, stopping replay");
            break;
        }
        log.push_back(entry);
    }
    close(fd);
    spdlog::info("RaftNode: recovered term={} voted_for={} log_entries={}",
                 term_number, voted_for, log.size());
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
