#include "manual.hpp"

void InteractiveHandler::query7(std::vector<Query7Response> & _return, const Query7Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    friends.push_back(std::numeric_limits<uint64_t>::max());
    auto posts = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Post_creator});
    auto comments = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Comment_creator});
    std::map<std::pair<int64_t, uint64_t>, std::tuple<uint64_t, uint64_t>> idx;
    std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>> person_hash;

    for(size_t i=0;i<posts.size();i++)
    {
        uint64_t vid = posts[i];
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Person_like);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
                uint64_t person_id = person->id;
                auto key = std::make_pair(-date, person_id);
                auto value = std::make_tuple(nbrs.dst_id(), vid);
                std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>>::iterator iter;

                if((idx.size() < (size_t)request.limit || idx.rbegin()->first > key) && ((iter=person_hash.find(person_id)) == person_hash.end() || iter->second > key))
                {
                    idx.emplace(key, value);
                    if(iter == person_hash.end())
                    {
                        person_hash.emplace(person_id, key);
                        while(idx.size() > (size_t)request.limit)
                        {
                            person_hash.erase(idx.rbegin()->first.second);
                            idx.erase(idx.rbegin()->first);
                        }
                    }
                    else
                    {
                        idx.erase(iter->second);
                        iter->second = key;
                    }
                    
                }
                else
                {
                    flag = idx.size() < (size_t)request.limit || idx.rbegin()->first.first > key.first;
                }
                nbrs.next();
            }
        }
    }


    for(size_t i=0;i<comments.size();i++)
    {
        uint64_t vid = comments[i];
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Comment2Person_like);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
                uint64_t person_id = person->id;
                auto key = std::make_pair(-date, person_id);
                auto value = std::make_tuple(nbrs.dst_id(), vid);
                std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>>::iterator iter;

                if((idx.size() < (size_t)request.limit || idx.rbegin()->first > key) && ((iter=person_hash.find(person_id)) == person_hash.end() || iter->second > key))
                {
                    idx.emplace(key, value);
                    if(iter == person_hash.end())
                    {
                        person_hash.emplace(person_id, key);
                        while(idx.size() > (size_t)request.limit)
                        {
                            person_hash.erase(idx.rbegin()->first.second);
                            idx.erase(idx.rbegin()->first);
                        }
                    }
                    else
                    {
                        idx.erase(iter->second);
                        iter->second = key;
                    }
                    
                }
                else
                {
                    flag = idx.size() < (size_t)request.limit || idx.rbegin()->first.first > key.first;
                }
                nbrs.next();
            }
        }
    }

    for(auto p : idx)
    {
        _return.emplace_back();
        uint64_t person_vid = std::get<0>(p.second);
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(person_vid).data();
        uint64_t message_vid = std::get<1>(p.second);
        uint64_t like_time = -p.first.first;
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(message_vid).data();
        _return.back().personId = person->id;
        _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().likeCreationDate = like_time;
        _return.back().commentOrPostId = message->id;
        _return.back().commentOrPostContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
        _return.back().minutesLatency = (like_time-message->creationDate)/(60lu*1000lu);
        _return.back().isNew = (*std::lower_bound(friends.begin(), friends.end(), person_vid)) != person_vid;
    }

}
