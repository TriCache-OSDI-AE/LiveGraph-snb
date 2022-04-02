#include "manual.hpp"

void InteractiveHandler::shortQuery6(ShortQuery6Response& _return, const ShortQuery6Request& request)
{
    _return = ShortQuery6Response();
    uint64_t vid = postSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
    if(!message) return;
    for(;message->replyOfComment != (uint64_t)-1;message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfComment).data());
    if(message->replyOfPost != (uint64_t)-1) message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfPost).data();
    auto forum = (snb::ForumSchema::Forum*)engine.get_vertex(message->forumid).data();
    if(!engine.get_vertex(vid).data()) return;
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(forum->moderator).data();
    _return.forumId = forum->id;
    _return.forumTitle = std::string(forum->title(), forum->titleLen());
    _return.moderatorId = person->id;
    _return.moderatorFirstName = std::string(person->firstName(), person->firstNameLen());
    _return.moderatorLastName = std::string(person->lastName(), person->lastNameLen());
}
