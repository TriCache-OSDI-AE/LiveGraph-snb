#include <string>
#include <vector>
#include <thread>
#include "core/schema.h" 
#include "core/import.h"

int main(int argc, char** argv)
{
    std::string path = argv[1];
    snb::PersonSchema personSchema;
    snb::PlaceSchema placeSchema;
    snb::OrgSchema orgSchema;
    snb::MessageSchema postSchema;
    snb::MessageSchema commentSchema;
    snb::TagSchema tagSchema;
    snb::TagClassSchema tagclassSchema;
    snb::ForumSchema forumSchema;

    {
        std::vector<std::thread> pool;
        //pool.emplace_back(snb::importPerson, std::ref(personSchema), path);
        //pool.emplace_back(snb::importPlace, std::ref(placeSchema), path);
        //pool.emplace_back(snb::importOrg, std::ref(orgSchema), path);
        //pool.emplace_back(snb::importPost, std::ref(postSchema), path);
        //pool.emplace_back(snb::importComment, std::ref(commentSchema), path);
        //pool.emplace_back(snb::importTag, std::ref(tagSchema), path);
        //pool.emplace_back(snb::importTagClass, std::ref(tagclassSchema), path);
        pool.emplace_back(snb::importForum, std::ref(forumSchema), path);
        for(auto &t:pool) t.join();
    }

    return 0;
}
