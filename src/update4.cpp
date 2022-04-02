#include "manual.hpp"

void InteractiveHandler::update4(const Update4Request& request)
{
    uint64_t vid = forumSchema.findId(request.forumId);
    if(vid != (uint64_t)-1) return;
    uint64_t moderator_vid = personSchema.findId(request.moderatorPersonId);
    if(moderator_vid == (uint64_t)-1) return;
    auto forum_buf = snb::ForumSchema::createForum(request.forumId, request.forumTitle, request.creationDate, moderator_vid);
    std::vector<uint64_t> tag_vid;
    for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(moderator_vid).data()) return;
        try
        {
            if(vid == (uint64_t)-1)
            {
                vid = txn.new_vertex();
                forumSchema.insertId(request.forumId, vid);
            }
            txn.put_vertex(vid, std::string(forum_buf.data(), forum_buf.size()));
            txn.put_edge(moderator_vid, (label_t)snb::EdgeSchema::Person2Forum_moderator, vid, std::string());
            for(auto tag:tag_vid)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Forum2Tag, tag, std::string());
                txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Forum, vid, std::string());
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
