#include "manual.hpp"

void InteractiveHandler::query13(Query13Response& _return, const Query13Request& request)
{
    _return.shortestPathLength = -1;
    uint64_t vid1 = personSchema.findId(request.person1Id);
    uint64_t vid2 = personSchema.findId(request.person2Id);
    if(vid1 == (uint64_t)-1) return;
    if(vid2 == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid1).data() == nullptr) return;
    if(engine.get_vertex(vid2).data() == nullptr) return;
    _return.shortestPathLength = pairwiseShortestPath(engine, vid1, vid2, (label_t)snb::EdgeSchema::Person2Person);
}
