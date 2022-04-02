#include "manual.hpp"

void InteractiveHandler::shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request& request)
{
    _return = ShortQuery4Response();
    uint64_t vid = postSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
    if(!message) return;
    _return.messageCreationDate = message->creationDate;
    _return.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
}
