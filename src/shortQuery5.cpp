#include "manual.hpp"

void InteractiveHandler::shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request& request)
{
    _return = ShortQuery5Response();
    uint64_t vid = postSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
    if(!message) return;
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
    _return.personId = person->id;
    _return.firstName = std::string(person->firstName(), person->firstNameLen());
    _return.lastName = std::string(person->lastName(), person->lastNameLen());
}
