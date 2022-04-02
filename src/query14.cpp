#include "manual.hpp"

void InteractiveHandler::query14(std::vector<Query14Response> & _return, const Query14Request& request)
{
    _return.clear();
    uint64_t vid1 = personSchema.findId(request.person1Id);
    uint64_t vid2 = personSchema.findId(request.person2Id);
    if(vid1 == (uint64_t)-1) return;
    if(vid2 == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid1).data() == nullptr) return;
    if(engine.get_vertex(vid2).data() == nullptr) return;
    auto paths = pairwiseShortestPath_path(engine, vid1, vid2, (label_t)snb::EdgeSchema::Person2Person);
    std::sort(paths.begin(), paths.end(), [](const std::pair<double, std::vector<uint64_t>> &a, const std::pair<double, std::vector<uint64_t>> &b)
    {
        return a.first > b.first;
    });
    std::unordered_map<uint64_t, uint64_t> idx;
    for(auto p:paths)
    {
        _return.emplace_back();
        _return.back().pathWeight = p.first;
        for(auto vid:p.second)
        {
            auto iter = idx.find(vid);
            if(iter == idx.end()){
                auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
                iter = idx.emplace(vid, person->id).first;
            }
            _return.back().personIdsInPath.emplace_back(iter->second);
        }
    }

}
