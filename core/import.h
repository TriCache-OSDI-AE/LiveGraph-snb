#pragma once
#include "core/schema.h"

namespace snb
{
    extern void importPerson(snb::PersonSchema &personSchema, std::string path);
    extern void importPlace(snb::PlaceSchema &placeSchema, std::string path);
    extern void importOrg(snb::OrgSchema &orgSchema, std::string path);
    extern void importPost(snb::MessageSchema &postSchema, std::string path);
    extern void importComment(snb::MessageSchema &commentSchema, std::string path);
    extern void importTag(snb::TagSchema &tagSchema, std::string path);
    extern void importTagClass(snb::TagClassSchema &tagclassSchema, std::string path);
    extern void importForum(snb::ForumSchema &forumSchema, std::string path);
}
