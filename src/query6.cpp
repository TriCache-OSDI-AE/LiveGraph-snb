#include "manual.hpp"

void InteractiveHandler::query6(std::vector<Query6Response> & _return, const Query6Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    uint64_t tagId = tagSchema.findName(request.tagName);
    if(tagId == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
    friends.push_back(std::numeric_limits<uint64_t>::max());
    std::unordered_map<uint64_t, int> idx;
    {
        uint64_t vid = tagId;
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Tag2Post);
            while (nbrs.valid())
            {
                auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                if(*std::lower_bound(friends.begin(), friends.end(), message->creator) == message->creator)
                {
                    uint64_t vid = nbrs.dst_id();
                    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                    while (nbrs.valid())
                    {
                        if(nbrs.dst_id() != tagId)
                        {
                            idx[nbrs.dst_id()]++;
                        }
                        nbrs.next();
                    }
                }
                nbrs.next();
            }
        }
    }
    std::set<std::pair<int, std::string>> idx_by_count;
    for(auto i=idx.begin();i!=idx.end();i++)
    {
        if(idx_by_count.size() < (size_t)request.limit || idx_by_count.rbegin()->first >= -i->second)
        {
            auto tag = (snb::TagSchema::Tag*)engine.get_vertex(i->first).data();
            idx_by_count.emplace(-i->second, std::string(tag->name(), tag->nameLen()));
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
