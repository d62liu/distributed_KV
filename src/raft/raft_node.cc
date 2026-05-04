#include "raft_node.h"
#include "raft_service_impl.h"


void RaftNode::start_election() {
    state = State::Candidate;
    ++term_number;
    voted_for = node_id;

}
