#include "manual.hpp"

void InteractiveHandler::update6(const Update6Request& request)
{
    uint64_t vid = postSchema.findId(request.postId);
    if(vid != (uint64_t)-1) return;
    uint64_t person_vid = personSchema.findId(request.authorPersonId);
    uint64_t forum_vid = forumSchema.findId(request.forumId);
    uint64_t place_vid = placeSchema.findId(request.countryId);
    if(person_vid == (uint64_t)-1) return;
    if(forum_vid == (uint64_t)-1) return;
    if(place_vid == (uint64_t)-1) return;
    std::vector<uint64_t> tag_vid;
    for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
    auto message_buf = snb::MessageSchema::createMessage(request.postId, request.imageFile, 
            request.creationDate, request.locationIp, request.browserUsed, 
            request.language, request.content, person_vid, forum_vid, place_vid, 
            (uint64_t)-1, (uint64_t)-1, snb::MessageSchema::Message::Type::Post);
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(person_vid).data()) return;
        if(!txn.get_vertex(forum_vid).data()) return;
        try
        {
            if(vid == (uint64_t)-1)
            {
                vid = txn.new_vertex();
                postSchema.insertId(request.postId, vid);
            }
            txn.put_vertex(vid, std::string(message_buf.data(), message_buf.size()));

            {
                std::vector<std::pair<uint64_t, uint64_t>> edges;
                auto nbrs = txn.get_edges(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                while (nbrs.valid())
                {
                    uint64_t date = *(uint64_t*)(nbrs.edge_data().data());
                    if(date <= (uint64_t)request.creationDate) break;
                    edges.emplace_back(nbrs.dst_id(), date);
                    nbrs.next();
                }
                for(auto p:edges) txn.del_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator, p.first);
                txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator, vid, std::string((char*)&request.creationDate, sizeof(request.creationDate)));
                std::reverse(edges.begin(), edges.end());
                for(auto p:edges) txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator, p.first, std::string((char*)&p.second, sizeof(p.second)));
            }

            {
                auto bytes = txn.get_edge(person_vid, (label_t)snb::EdgeSchema::Person2Forum_member, forum_vid);
                if(bytes.data())
                {
                    auto buf = std::string(bytes);
                    *(uint64_t*)(buf.data()+sizeof(uint64_t)) += 1;
                    txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Forum_member, forum_vid, std::string(buf.data(), buf.size()));
                    txn.put_edge(forum_vid, (label_t)snb::EdgeSchema::Forum2Person_member, person_vid, std::string(buf.data(), buf.size()));
                }
            }

            txn.put_edge(forum_vid, (label_t)snb::EdgeSchema::Forum2Post, vid, std::string());

            for(auto tag:tag_vid)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Post2Tag, tag, std::string());
                txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Post, vid, std::string());
            }

            auto epoch = txn.commit();
            break;
        } 
        catch (typename decltype(txn)::RollbackExcept &e) 
        {
            txn.abort();
        }
    }
}
