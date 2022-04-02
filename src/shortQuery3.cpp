#include "manual.hpp"

void InteractiveHandler::shortQuery3(std::vector<ShortQuery3Response> & _return, const ShortQuery3Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
    if(!person) return;
    
    std::vector<std::tuple<int64_t, uint64_t, snb::PersonSchema::Person*>> idx;

    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Person);
    while (nbrs.valid())
    {
        int64_t date = *(uint64_t*)nbrs.edge_data().data();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
        idx.emplace_back(-date, person->id, person);
        nbrs.next();
    }
    std::sort(idx.begin(), idx.end());
    for(auto p : idx)
    {
        _return.emplace_back();
        auto person = std::get<2>(p);
        _return.back().personId = person->id;
        _return.back().firstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().lastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().friendshipCreationDate = -std::get<0>(p);
    }

}
