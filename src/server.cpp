#include <string>
#include <vector>
#include <thread>
#include "core/schema.h" 
#include "core/import.h"
#include "core/graph.h"

using namespace snb;
livegraph::Graph *snb::graph = nullptr;

int main(int argc, char** argv)
{
    std::string graphPath = argv[1];
    std::string dataPath = argv[2];

    graph = new livegraph::Graph(graphPath);

    PersonSchema personSchema;
    PlaceSchema placeSchema;
    OrgSchema orgSchema;
    MessageSchema postSchema;
    MessageSchema commentSchema;
    TagSchema tagSchema;
    TagClassSchema tagclassSchema;
    ForumSchema forumSchema;

    {
        std::vector<std::thread> pool;
        pool.emplace_back(prepareVIndex, std::ref(personSchema),   dataPath+personPathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(placeSchema),    dataPath+placePathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(orgSchema),      dataPath+orgPathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(postSchema),     dataPath+postPathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(commentSchema),  dataPath+commentPathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(tagSchema),      dataPath+tagPathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(tagclassSchema), dataPath+tagclassPathSuffix);
        pool.emplace_back(prepareVIndex, std::ref(forumSchema),    dataPath+forumPathSuffix);
        for(auto &t:pool) t.join();
    }

    {
        std::vector<std::thread> pool;
        pool.emplace_back(importPerson,   std::ref(personSchema),   dataPath);
        pool.emplace_back(importPlace,    std::ref(placeSchema),    dataPath);
        pool.emplace_back(importOrg,      std::ref(orgSchema),      dataPath);
        pool.emplace_back(importPost,     std::ref(postSchema),     dataPath);
        pool.emplace_back(importComment,  std::ref(commentSchema),  dataPath);
        pool.emplace_back(importTag,      std::ref(tagSchema),      dataPath);
        pool.emplace_back(importTagClass, std::ref(tagclassSchema), dataPath);
        pool.emplace_back(importForum,    std::ref(forumSchema),    dataPath);
        for(auto &t:pool) t.join();
    }

    delete graph;
    return 0;
}
