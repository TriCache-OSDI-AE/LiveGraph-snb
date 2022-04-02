#include "manual.hpp"

void InteractiveHandler::query9(std::vector<Query9Response> & _return, const Query9Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});

    std::map<std::pair<int64_t, uint64_t>, snb::MessageSchema::Message*> idx;

    for(size_t i=0;i<friends.size();i++)
    {
        uint64_t vid = friends[i];
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                if(date < request.maxDate)
                {
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    auto key = std::make_pair(-date, message->id);
                    auto value = message;

                    if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                    {
                        idx.emplace(key, value);
                        while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                    }
                    else
                    {
                        flag = idx.rbegin()->first.first > key.first;
                    }

                }
                nbrs.next();
            }
        }
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                if(date < request.maxDate)
                {
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    auto key = std::make_pair(-date, message->id);
                    auto value = message;

                    if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                    {
                        idx.emplace(key, value);
                        while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                    }
                    else
                    {
                        flag = idx.rbegin()->first.first > key.first;
                    }

                }
                nbrs.next();
            }
        }
    }
    for(auto p : idx)
    {
        _return.emplace_back();
        auto message = p.second;
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
        _return.back().personId = person->id;
        _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().messageId = message->id;;
        _return.back().messageCreationDate = message->creationDate;
        _return.back().messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
    }


}
