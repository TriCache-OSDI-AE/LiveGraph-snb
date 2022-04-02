#include "manual.hpp"

void InteractiveHandler::query12(std::vector<Query12Response> & _return, const Query12Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    uint64_t tagclassId = tagclassSchema.findName(request.tagClassName);
    if(tagclassId == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    auto tags = multihop_another_etype(engine, tagclassId, 65536, (label_t)snb::EdgeSchema::TagClass2TagClass_down, (label_t)snb::EdgeSchema::TagClass2Tag);
    tags.push_back(std::numeric_limits<uint64_t>::max());
    std::map<std::pair<int, uint64_t>, std::pair<uint64_t, std::vector<uint64_t>>> idx;


    for(size_t i=0;i<friends.size();i++)
    {
        uint64_t vid = friends[i];
        int count = 0;
        std::set<uint64_t> tagSet;
        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
        while (nbrs.valid())
        {
            bool flag = false;
            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
            uint64_t vid = message->replyOfPost;
            if(vid != (uint64_t)-1){
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                while (nbrs.valid())
                {
                    uint64_t tag = nbrs.dst_id();
                    if(*std::lower_bound(tags.begin(), tags.end(), tag) == tag)
                    {
                        flag = true;
                        tagSet.emplace(tag);
                    }
                    nbrs.next();
                }
            }
            if(flag) count++;
            nbrs.next();
        }
        if(count)
        {
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
            uint64_t person_id = person->id;
            auto key = std::make_pair(-count, person_id);
            std::vector<uint64_t> tagV;
            for(auto p: tagSet) tagV.push_back(p);
            auto value = std::make_pair(vid, tagV);

            if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
            {
                idx.emplace(key, value);
                while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
            }

        }
    }

    for(auto p : idx)
    {
        _return.emplace_back();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(p.second.first).data();
        _return.back().personId = person->id;
        _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
        for(auto vid: p.second.second)
        {
            auto tag = (snb::TagSchema::Tag*)engine.get_vertex(vid).data();
            _return.back().tagNames.emplace_back(tag->name(), tag->nameLen());
        }
        std::sort(_return.back().tagNames.begin(), _return.back().tagNames.end());
        _return.back().replyCount = -p.first.first;
    }

}
