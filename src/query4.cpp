#include "manual.hpp"

void InteractiveHandler::query4(std::vector<Query4Response> & _return, const Query4Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    uint64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    std::unordered_map<uint64_t, std::pair<uint64_t, int>> idx;


    for(size_t i=0;i<friends.size();i++)
    {
        uint64_t vid = friends[i];
        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
        while (nbrs.valid())
        {
            uint64_t date = *(uint64_t*)nbrs.edge_data().data();
            if(date < endDate)
            {
                uint64_t vid = nbrs.dst_id();
                auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                {
                    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                    while (nbrs.valid())
                    {
                        auto iter = idx.find(nbrs.dst_id());
                        if(iter == idx.end())
                        {
                            idx.emplace(nbrs.dst_id(), std::make_pair(message->creationDate, 1));
                        }
                        else
                        {
                            iter->second.first = std::min(message->creationDate, iter->second.first);
                            iter->second.second++;
                        }
                        nbrs.next();
                    }
                }
            }
            nbrs.next();
        }
    }
    std::set<std::pair<int, std::string>> idx_by_count;
    for(auto i=idx.begin();i!=idx.end();i++)
    {
        if(i->second.first >= (uint64_t)request.startDate && (idx_by_count.size() < (size_t)request.limit || idx_by_count.rbegin()->first >= -i->second.second))
        {
            auto tag = (snb::TagSchema::Tag*)engine.get_vertex(i->first).data();
            idx_by_count.emplace(-i->second.second, std::string(tag->name(), tag->nameLen()));
            while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(*idx_by_count.rbegin());
        }
    }
    for(auto p : idx_by_count)
    {
        _return.emplace_back();
        _return.back().postCount = -p.first;
        _return.back().tagName = p.second;
    }


}
