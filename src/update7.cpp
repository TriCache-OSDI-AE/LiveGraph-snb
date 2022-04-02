#include "manual.hpp"

void InteractiveHandler::update7(const Update7Request& request)
{
    uint64_t vid = commentSchema.findId(request.commentId);
    if(vid != (uint64_t)-1) return;
    uint64_t person_vid = personSchema.findId(request.authorPersonId);
    uint64_t place_vid = placeSchema.findId(request.countryId);
    uint64_t post_vid = postSchema.findId(request.replyToPostId);
    uint64_t comment_vid = commentSchema.findId(request.replyToCommentId);
    if(person_vid == (uint64_t)-1) return;
    if(place_vid == (uint64_t)-1) return;
    if(post_vid == (uint64_t)-1 && comment_vid == (uint64_t)-1) return;
    uint64_t message_vid = (post_vid != (uint64_t)-1) ? post_vid : comment_vid;
    std::vector<uint64_t> tag_vid;
    for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
    auto message_buf = snb::MessageSchema::createMessage(request.commentId, std::string(), 
            request.creationDate, request.locationIp, request.browserUsed, 
            std::string(), request.content, person_vid, (uint64_t)-1, place_vid, 
            post_vid, comment_vid, snb::MessageSchema::Message::Type::Comment);
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(person_vid).data()) return;
        if(!txn.get_vertex(message_vid).data()) return;
        try
        {
            if(vid == (uint64_t)-1)
            {
                vid = txn.new_vertex();
                commentSchema.insertId(request.commentId, vid);
            }
            txn.put_vertex(vid, std::string(message_buf.data(), message_buf.size()));

            {
                std::vector<std::pair<uint64_t, uint64_t>> edges;
                auto nbrs = txn.get_edges(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
                while (nbrs.valid())
                {
                    uint64_t date = *(uint64_t*)(nbrs.edge_data().data());
                    if(date <= (uint64_t)request.creationDate) break;
                    edges.emplace_back(nbrs.dst_id(), date);
                    nbrs.next();
                }
                for(auto p:edges) txn.del_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, p.first);
                txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, vid, std::string((char*)&request.creationDate, sizeof(request.creationDate)));
                std::reverse(edges.begin(), edges.end());
                for(auto p:edges) txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, p.first, std::string((char*)&p.second, sizeof(p.second)));
            }

            {
                std::vector<std::pair<uint64_t, uint64_t>> edges;
                auto nbrs = txn.get_edges(message_vid, (label_t)snb::EdgeSchema::Message2Message_down);
                while (nbrs.valid())
                {
                    uint64_t date = *(uint64_t*)(nbrs.edge_data().data());
                    if(date <= (uint64_t)request.creationDate) break;
                    edges.emplace_back(nbrs.dst_id(), date);
                    nbrs.next();
                }
                for(auto p:edges) txn.del_edge(message_vid, (label_t)snb::EdgeSchema::Message2Message_down, p.first);
                txn.put_edge(message_vid, (label_t)snb::EdgeSchema::Message2Message_down, vid, std::string((char*)&request.creationDate, sizeof(request.creationDate)));
                std::reverse(edges.begin(), edges.end());
                for(auto p:edges) txn.put_edge(message_vid, (label_t)snb::EdgeSchema::Message2Message_down, p.first, std::string((char*)&p.second, sizeof(p.second)));
            }

            {
                auto friend_vid = ((snb::MessageSchema::Message*)txn.get_vertex(message_vid).data())->creator;
                auto bytes = txn.get_edge(person_vid, (label_t)snb::EdgeSchema::Person2Person, friend_vid);
                if(bytes.data())
                {
                    auto buf = std::string(bytes);
                    *(double*)(buf.data()+sizeof(uint64_t)) += (post_vid == message_vid)?1.0:0.5;
                    txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Person, friend_vid, std::string(buf.data(), buf.size()));
                    txn.put_edge(friend_vid, (label_t)snb::EdgeSchema::Person2Person, person_vid, std::string(buf.data(), buf.size()));
                }
            }

            for(auto tag:tag_vid)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Comment2Tag, tag, std::string());
                txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Comment, vid, std::string());
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
