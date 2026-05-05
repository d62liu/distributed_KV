#include "raft_node.h"
#include "raft/raft_client.h"


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
        if (resp.vote_granted()) ++votes;
    }

    if (votes > static_cast<int>(peers.size() + 1) / 2) {
        state = State::Leader;
    }
    if (state == State::Leader){
        Timer heartbeat(50, [this]() { send_heartbeat(); });           
    }                                                                                                                            }

bool RaftNode::request_vote(uint64_t term, const std::string& candidate_id) {
    if (term < term_number) return false;
    if (voted_for != "" && voted_for != candidate_id) return false;
    term_number = term;
    voted_for = candidate_id;
    timer.reset();
    return true;
}

bool RaftNode::append_entries(uint64_t term, const std::string& leader_id){
    if (term < term_number){
        return false;
    }
    term_number = term;
    timer.reset();
    return true;
}

void RaftNode::send_heartbeat(){
    raft::AppendEntriesRequest req;
    req.set_term(term_number);
    req.set_leader_id(node_id);
    for (const auto& peer : peers) {
        RaftClient client(peer);
        client.AppendEntries(req);
    }
}

