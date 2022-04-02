#include "manual.hpp"

void InteractiveHandler::update5(const Update5Request& request)
{
    uint64_t person_vid = personSchema.findId(request.personId);
    uint64_t forum_vid = forumSchema.findId(request.forumId);
    if(person_vid == (uint64_t)-1) return;
    if(forum_vid == (uint64_t)-1) return;
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(person_vid).data()) return;
        if(!txn.get_vertex(forum_vid).data()) return;
        try
        {
            auto nbrs = txn.get_edges(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator);
            uint64_t posts = 0;
            while (nbrs.valid())
            {
                auto message = (snb::MessageSchema::Message*)txn.get_vertex(nbrs.dst_id()).data();
                if(message->forumid == forum_vid) posts++;
                nbrs.next();
            }

            snb::Buffer buf(sizeof(uint64_t)+sizeof(uint64_t));
            *(uint64_t*)buf.data() = request.joinDate;
            *(uint64_t*)(buf.data()+sizeof(uint64_t)) = posts;

            txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Forum_member, forum_vid, std::string(buf.data(), buf.size()));
            txn.put_edge(forum_vid, (label_t)snb::EdgeSchema::Forum2Person_member, person_vid, std::string(buf.data(), buf.size()));

            auto epoch = txn.commit();
            break;
        } 
        catch (typename decltype(txn)::RollbackExcept &e) 
        {
            txn.abort();
        }
    }
}
