#pragma once
#include "core/schema.h"
#include <string>

namespace snb
{
    const std::string personPathSuffix="/person_0_0.csv";
    const std::string placePathSuffix="/place_0_0.csv";
    const std::string orgPathSuffix="/organisation_0_0.csv";
    const std::string postPathSuffix="/post_0_0.csv";
    const std::string commentPathSuffix="/comment_0_0.csv";
    const std::string tagPathSuffix="/tag_0_0.csv";
    const std::string tagclassPathSuffix="/tagclass_0_0.csv";
    const std::string forumPathSuffix="/forum_0_0.csv";

    extern void prepareVIndex(snb::Schema &schema, std::string path);
    extern void importPerson(snb::PersonSchema &personSchema, std::string path);
    extern void importPlace(snb::PlaceSchema &placeSchema, std::string path);
    extern void importOrg(snb::OrgSchema &orgSchema, std::string path);
    extern void importPost(snb::MessageSchema &postSchema, std::string path);
    extern void importComment(snb::MessageSchema &commentSchema, std::string path);
    extern void importTag(snb::TagSchema &tagSchema, std::string path);
    extern void importTagClass(snb::TagClassSchema &tagclassSchema, std::string path);
    extern void importForum(snb::ForumSchema &forumSchema, std::string path);
}
