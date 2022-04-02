#include "manual.hpp"

void InteractiveHandler::query2(std::vector<Query2Response> & _return, const Query2Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    std::map<std::pair<int64_t, uint64_t>, Query2Response> idx;


    for(size_t i=0;i<friends.size();i++)
    {
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(friends[i]).data();
        uint64_t vid = friends[i];
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                if(date <= request.maxDate)
                {
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    auto key = std::make_pair(-date, message->id);
                    auto value = Query2Response();
                    value.personId = person->id;
                    value.messageCreationDate = message->creationDate;
                    value.messageId = message->id;

                    {
                        if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                        {
                            value.personFirstName = std::string(person->firstName(), person->firstNameLen());
                            value.personLastName = std::string(person->lastName(), person->lastNameLen());
                            value.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
                            idx.emplace(key, value);
                            while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                        }
                        else
                        {
                            flag = false;
                        }
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
                if(date <= request.maxDate)
                {
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    auto key = std::make_pair(-date, message->id);
                    auto value = Query2Response();
                    value.personId = person->id;
                    value.messageCreationDate = message->creationDate;
                    value.messageId = message->id;

                    {
                        if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                        {
                            value.personFirstName = std::string(person->firstName(), person->firstNameLen());
                            value.personLastName = std::string(person->lastName(), person->lastNameLen());
                            value.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
                            idx.emplace(key, value);
                            while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                        }
                        else
                        {
                            flag = false;
                        }
                    }
                }
                nbrs.next();
            }
        }
    }
    for(auto p : idx) _return.emplace_back(p.second);


}
