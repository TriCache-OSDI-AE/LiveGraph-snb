#include "manual.hpp"

void InteractiveHandler::shortQuery2(std::vector<ShortQuery2Response> & _return, const ShortQuery2Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
    if(!person) return;
    
    std::map<std::pair<int64_t, int64_t>, snb::MessageSchema::Message*> idx;
    {
        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
        bool flag = true;
        while (nbrs.valid() && flag)
        {
            int64_t date = *(uint64_t*)nbrs.edge_data().data();
            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
            auto key = std::make_pair(-date, -(int64_t)message->id);
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
            nbrs.next();
        }
    }
    {
        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
        bool flag = true;
        while (nbrs.valid() && flag)
        {
            int64_t date = *(uint64_t*)nbrs.edge_data().data();
            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
            auto key = std::make_pair(-date, -(int64_t)message->id);
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
            nbrs.next();
        }
    }
    for(auto p : idx)
    {
        _return.emplace_back();
        auto message = p.second;
        _return.back().messageId = message->id;
        _return.back().messageCreationDate = message->creationDate;
        _return.back().messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
        for(;message->replyOfComment != (uint64_t)-1;message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfComment).data());
        if(message->replyOfPost != (uint64_t)-1) message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfPost).data();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
        _return.back().originalPostId = message->id;
        _return.back().originalPostAuthorId = person->id;
        _return.back().originalPostAuthorFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().originalPostAuthorLastName = std::string(person->lastName(), person->lastNameLen());
    }

}
