#include "manual.hpp"

void InteractiveHandler::update3(const Update3Request& request)
{
    uint64_t person_vid = personSchema.findId(request.personId);
    uint64_t comment_vid = commentSchema.findId(request.commentId);
    if(person_vid == (uint64_t)-1) return;
    if(comment_vid == (uint64_t)-1) return;
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(person_vid).data()) return;
        if(!txn.get_vertex(comment_vid).data()) return;
        try
        {
            txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_like, comment_vid, std::string((char*)&request.creationDate, sizeof(request.creationDate)));
            txn.put_edge(comment_vid, (label_t)snb::EdgeSchema::Comment2Person_like, person_vid, std::string((char*)&request.creationDate, sizeof(request.creationDate)));

            auto epoch = txn.commit();
            break;
        } 
        catch (typename decltype(txn)::RollbackExcept &e) 
        {
            txn.abort();
        }
    }
}
