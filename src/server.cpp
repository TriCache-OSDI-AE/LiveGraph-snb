#include <ctime>
#include <thread>
#include <chrono>
#include <vector>
#include <cassert>
#include <sstream>
#include <fstream>
#include <iostream>
#include <functional>
#include <string_view>
#include <unordered_set>

#include <tbb/parallel_sort.h>
#include <tbb/concurrent_vector.h>

#include <fcntl.h>
#include <sys/mman.h>

#include "core/lg.hpp"
#include "core/schema.hpp" 
#include "core/import.hpp"
#include "core/util.hpp"

#include "Interactive.h"
#include <thrift/transport/TSocket.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "neo4j.hpp"

using namespace ::apache::thrift;
using namespace ::apache::thrift::concurrency;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

Graph *graph = nullptr;
TServerFramework *server = nullptr;
snb::PersonSchema personSchema;
snb::PlaceSchema placeSchema;
snb::OrgSchema orgSchema;
snb::MessageSchema postSchema;
snb::MessageSchema commentSchema;
snb::TagSchema tagSchema;
snb::TagClassSchema tagclassSchema;
snb::ForumSchema forumSchema;

std::vector<std::string> static parallel_readlines(const std::string &path)
{
    int fd = open(path.c_str(), O_RDONLY, 0640);
    if(fd == -1) throw std::runtime_error(std::string("open path ") + path + " error.");
    uint64_t size = lseek(fd, 0, SEEK_END);
    char *data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(data == MAP_FAILED) throw std::runtime_error("mmap error.");
    size_t num = 0;
    std::atomic_size_t cur_num = 0;
    #pragma omp parallel for reduction(+: num)
    for(size_t i=0;i<size;i++)
    {
        if(data[i] == '\n') num++;
    }
    std::vector<size_t> positions(num);
    #pragma omp parallel for 
    for(size_t i=0;i<size;i++)
    {
        if(data[i] == '\n') positions[cur_num++] = i;
    }
    tbb::parallel_sort(positions.begin(), positions.end());
    std::vector<std::string> ret(num);
    #pragma omp parallel for 
    for(size_t i=0;i<num;i++)
    {
        if(i == 0)
        {
            ret[i] = std::string(data, positions[i]);
        }
        else if(positions[i-1]+1 <= positions[i]-1)
        {
            ret[i] = std::string(data+positions[i-1]+1, positions[i]-positions[i-1]-1);
        }
    }
    if(positions.back() != size-1) ret.emplace_back(std::string(data+positions.back()+1, size-positions.back()));

    munmap(data, size);
    close(fd);
    return ret;
    
    //std::ifstream fi(path);
    //std::vector<std::string> ret;
    //for(std::string line;std::getline(fi, line);) ret.emplace_back(line);
    //return ret;
}

void prepareVIndex(snb::Schema &schema, std::string path)
{
    auto loader = graph->begin_batch_loader();
    auto lines = parallel_readlines(path);
    //#pragma omp parallel for
    for(size_t i=1;i<lines.size();i++)
    {
        auto v = split(lines[i], csv_split);
        uint64_t id = std::stoul(v[0]);
        uint64_t vid = loader.new_vertex();
        schema.insertId(id, vid);
    } 
}

void importPerson(snb::PersonSchema &personSchema, std::string path, snb::PlaceSchema &placeSchema)
{
    auto all_persons = parallel_readlines(path+snb::personPathSuffix);
    assert(all_persons[0] == "id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|place|language|email");

    std::vector<uint64_t> person_vids(all_persons.size()), place_vids(all_persons.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_persons.size();i++)
    {
        std::vector<std::string> person_v = split(all_persons[i], csv_split);
        person_vids[i] = personSchema.findId(std::stoull(person_v[0]));
        place_vids[i] = placeSchema.findId(std::stoull(person_v[8]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_persons.size();i++)
    {
        std::vector<std::string> person_v = split(all_persons[i], csv_split);
        std::vector<std::string> speaks_v = split(person_v[9], list_split);
        std::vector<std::string> emails_v = split(person_v[10], list_split);
        std::sort(speaks_v.begin(), speaks_v.end());

        std::chrono::system_clock::time_point birthday = from_date(person_v[4]);
        std::chrono::system_clock::time_point creationDate = from_time(person_v[5]);
        uint64_t person_vid = person_vids[i];
        uint64_t place_vid = place_vids[i];

        auto person_buf = snb::PersonSchema::createPerson(std::stoull(person_v[0]), person_v[1], person_v[2], person_v[3],  
                std::chrono::duration_cast<std::chrono::hours>(birthday.time_since_epoch()).count(), emails_v, speaks_v, person_v[7], person_v[6],
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(), 
                place_vid);

        personSchema.insertName(person_v[1]+snb::key_spacing+person_v[0], person_vid);

        loader.put_vertex(person_vid, std::string_view(person_buf.data(), person_buf.size()));
        loader.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Person, person_vid, std::string_view(), true);

//        const snb::PersonSchema::Person *person = (const snb::PersonSchema::Person*)person_buf.data();
//        std::cout << person->id << "|" << person->place << "|" << std::string(person->firstName(), person->firstNameLen()) << "|" 
//            << std::string(person->lastName(), person->lastNameLen()) << "|" 
//            << std::string(person->gender(), person->genderLen()) << "|" 
//            << std::string(person->browserUsed(), person->browserUsedLen()) << "|" 
//            << std::string(person->locationIP(), person->locationIPLen()) << "|";
//        std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::hours(person->birthday)));
//        std::cout << std::ctime(&t) << "|";
//        t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(person->creationDate)));
//        std::cout << std::ctime(&t) << "|";
//        emails_v = split(std::string(person->emails(), person->emailsLen()), zero_split);
//        for(auto e:emails_v) std::cout << e << ",";
//        std::cout << "|";
//        speaks_v = split(std::string(person->speaks(), person->speaksLen()), zero_split);
//        for(auto e:speaks_v) std::cout << e << ",";
//        std::cout << std::endl;
    }
}


void importPlace(snb::PlaceSchema &placeSchema, std::string path)
{
    auto all_places = parallel_readlines(path+snb::placePathSuffix);
    assert(all_places[0] == "id|name|url|type|isPartOf");

    std::vector<uint64_t> place_vids(all_places.size()), isPartOf_vids(all_places.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_places.size();i++)
    {
        std::vector<std::string> place_v = split(all_places[i], csv_split);
        place_vids[i] = placeSchema.findId(std::stoull(place_v[0]));
        isPartOf_vids[i] = place_v.size()>4?placeSchema.findId(std::stoull(place_v[4])):(uint64_t)-1;
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_places.size();i++)
    {
        std::vector<std::string> place_v = split(all_places[i], csv_split);
        snb::PlaceSchema::Place::Type type;
        if(place_v[3] == "city")
        {
            type = snb::PlaceSchema::Place::Type::City;
        }
        else if(place_v[3] == "country")
        {
            type = snb::PlaceSchema::Place::Type::Country;
        }
        else if(place_v[3] == "continent")
        {
            type = snb::PlaceSchema::Place::Type::Continent;
        }
        else
        {
            type = snb::PlaceSchema::Place::Type::Error;
            assert(false);
        }

        uint64_t place_vid = place_vids[i];
        uint64_t isPartOf_vid = isPartOf_vids[i];
        placeSchema.insertName(place_v[1], place_vid);

        auto place_buf = snb::PlaceSchema::createPlace(std::stoull(place_v[0]), place_v[1], place_v[2], type, isPartOf_vid);

        loader.put_vertex(place_vid, std::string_view(place_buf.data(), place_buf.size()));
        if(isPartOf_vid != (uint64_t)-1)
        {
            loader.put_edge(isPartOf_vid, (label_t)snb::EdgeSchema::Place2Place_down, place_vid, std::string_view(), true);
        }

//        const snb::PlaceSchema::Place *place = (const snb::PlaceSchema::Place*)place_buf.data();
//        std::cout << place->id << "|" << std::string(place->name(), place->nameLen()) << "|" 
//            << std::string(place->url(), place->urlLen()) << "|"  << (int)place->type << "|" << place->isPartOf << std::endl;

    }
}

void importOrg(snb::OrgSchema &orgSchema, std::string path, snb::PlaceSchema &placeSchema)
{
    auto all_orgs = parallel_readlines(path+snb::orgPathSuffix);
    assert(all_orgs[0] == "id|type|name|url|place");

    std::vector<uint64_t> org_vids(all_orgs.size()), place_vids(all_orgs.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_orgs.size();i++)
    {
        std::vector<std::string> org_v = split(all_orgs[i], csv_split);
        org_vids[i] = orgSchema.findId(std::stoull(org_v[0]));
        place_vids[i] = placeSchema.findId(std::stoull(org_v[4]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_orgs.size();i++)
    {
        std::vector<std::string> org_v = split(all_orgs[i], csv_split);
        snb::OrgSchema::Org::Type type;
        if(org_v[1] == "company")
        {
            type = snb::OrgSchema::Org::Type::Company;
        }
        else if(org_v[1] == "university")
        {
            type = snb::OrgSchema::Org::Type::University;
        }
        else
        {
            type = snb::OrgSchema::Org::Type::Error;
            assert(false);
        }

        uint64_t org_vid = org_vids[i];
        uint64_t place_vid = place_vids[i];

        auto org_buf = snb::OrgSchema::createOrg(std::stoull(org_v[0]), type, org_v[2], org_v[3], place_vid);

        loader.put_vertex(org_vid, std::string_view(org_buf.data(), org_buf.size()));
        loader.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Org, org_vid, std::string_view(), true);

//        const snb::OrgSchema::Org *org = (const snb::OrgSchema::Org*)org_buf.data();
//        std::cout << org->id << "|" << std::string(org->name(), org->nameLen()) << "|" 
//            << std::string(org->url(), org->urlLen()) << "|"  << (int)org->type << "|" << org->place << std::endl;

    }
}

void importPost(snb::MessageSchema &postSchema, std::string path, snb::PersonSchema &personSchema, snb::ForumSchema &forumSchema, snb::PlaceSchema &placeSchema)
{
    auto all_messages = parallel_readlines(path+snb::postPathSuffix);
    assert(all_messages[0] == "id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place");
    std::vector<std::vector<std::string>> message_vs;

    for(size_t i=1;i<all_messages.size();i++)
    {
        message_vs.emplace_back(split(all_messages[i], csv_split));
    }
    all_messages.clear();
    tbb::parallel_sort(message_vs.begin(), message_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[2] < b[2];
    });

    std::vector<uint64_t> message_vids(message_vs.size()), creator_vids(message_vs.size()), forumid_vids(message_vs.size()), place_vids(message_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<message_vs.size();i++)
    {
        auto &message_v = message_vs[i];
        message_vids[i] = postSchema.findId(std::stoull(message_v[0]));
        creator_vids[i] = personSchema.findId(std::stoull(message_v[8]));
        forumid_vids[i] = forumSchema.findId(std::stoull(message_v[9]));
        place_vids[i] = placeSchema.findId(std::stoull(message_v[10]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=0; i<message_vs.size(); i++)
    {
        auto &message_v = message_vs[i];
        snb::MessageSchema::Message::Type type = snb::MessageSchema::Message::Type::Post;
        assert(std::stoull(message_v[7]) <= message_v[6].length());
        std::chrono::system_clock::time_point creationDate = from_time(message_v[2]);

        uint64_t message_vid = message_vids[i];
        uint64_t creator_vid = creator_vids[i];
        uint64_t forumid_vid = forumid_vids[i];
        uint64_t place_vid = place_vids[i];
        uint64_t creationDateInt = std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count();

        auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), message_v[1], 
                creationDateInt,
                message_v[3], message_v[4], message_v[5], message_v[6], 
                creator_vid, forumid_vid, place_vid, (uint64_t)-1, (uint64_t)-1, type);

        loader.put_vertex(message_vid, std::string_view(message_buf.data(), message_buf.size()));
        loader.put_edge(forumid_vid, (label_t)snb::EdgeSchema::Forum2Post, message_vid, std::string_view(), true);
        loader.put_edge(creator_vid, (label_t)snb::EdgeSchema::Person2Post_creator, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)), true);

//        const snb::MessageSchema::Message *message = (const snb::MessageSchema::Message*)message_buf.data();
//        std::cout << message->id << "|" << std::string(message->imageFile(), message->imageFileLen()) << "|" 
//            << std::string(message->locationIP(), message->locationIPLen()) << "|"  
//            << std::string(message->browserUsed(), message->browserUsedLen()) << "|"  
//            << std::string(message->language(), message->languageLen()) << "|"  
//            << std::string(message->content(), message->contentLen()) << "|"  
//            << message->creator << "|" << message->forumid << "|" << message->place << "|" 
//            << message->replyOfPost << "|" << message->replyOfComment << "|" << (int)message->type << "|";
//        std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(message->creationDate)));
//        std::cout << std::ctime(&t);

    }
}

void importComment(snb::MessageSchema &commentSchema, std::string path, snb::PersonSchema &personSchema, snb::PlaceSchema &placeSchema, snb::MessageSchema &postSchema)
{
    auto all_messages = parallel_readlines(path+snb::commentPathSuffix);
    assert(all_messages[0] == "id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment");
    std::vector<std::vector<std::string>> message_vs;

    for(size_t i=1;i<all_messages.size();i++)
    {
        message_vs.emplace_back(split(all_messages[i], csv_split));
    }
    all_messages.clear();
    tbb::parallel_sort(message_vs.begin(), message_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[1] < b[1];
    });

    std::vector<uint64_t> message_vids(message_vs.size()), creator_vids(message_vs.size()), place_vids(message_vs.size());
    std::vector<uint64_t> replyOfPost_vids(message_vs.size()), replyOfComment_vids(message_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<message_vs.size();i++)
    {
        auto &message_v = message_vs[i];
        message_vids[i] = commentSchema.findId(std::stoull(message_v[0]));
        creator_vids[i] = personSchema.findId(std::stoull(message_v[6]));
        place_vids[i] = placeSchema.findId(std::stoull(message_v[7]));
        replyOfPost_vids[i] = !message_v[8].empty()?postSchema.findId(std::stoull(message_v[8])):(uint64_t)-1;
        replyOfComment_vids[i] = message_v.size()>9?commentSchema.findId(std::stoull(message_v[9])):(uint64_t)-1;
    }
    auto loader = graph->begin_batch_loader();

    for(size_t i=0; i<message_vs.size(); i++)
    {
        auto &message_v = message_vs[i];
        snb::MessageSchema::Message::Type type = snb::MessageSchema::Message::Type::Comment;
        assert(std::stoull(message_v[5]) <= message_v[4].length());
        std::chrono::system_clock::time_point creationDate = from_time(message_v[1]);

        uint64_t message_vid = message_vids[i];
        uint64_t creator_vid = creator_vids[i];
        uint64_t place_vid = place_vids[i];
        uint64_t replyOfPost_vid = replyOfPost_vids[i];
        uint64_t replyOfComment_vid = replyOfComment_vids[i];
        uint64_t creationDateInt = std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count();

        auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), std::string(), 
                creationDateInt,
                message_v[2], message_v[3], std::string(), message_v[4], 
                creator_vid, (uint64_t)-1, place_vid, replyOfPost_vid, replyOfComment_vid, type);

        loader.put_vertex(message_vid, std::string_view(message_buf.data(), message_buf.size()));
        loader.put_edge(creator_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)), true);
        if(replyOfPost_vid != (uint64_t)-1)
        {
            loader.put_edge(replyOfPost_vid, (label_t)snb::EdgeSchema::Message2Message_down, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)), true);
        }
        if(replyOfComment_vid != (uint64_t)-1)
        {
            loader.put_edge(replyOfComment_vid, (label_t)snb::EdgeSchema::Message2Message_down, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)), true);
        }

//        const snb::MessageSchema::Message *message = (const snb::MessageSchema::Message*)message_buf.data();
//        std::cout << message->id << "|" << std::string(message->imageFile(), message->imageFileLen()) << "|" 
//            << std::string(message->locationIP(), message->locationIPLen()) << "|"  
//            << std::string(message->browserUsed(), message->browserUsedLen()) << "|"  
//            << std::string(message->language(), message->languageLen()) << "|"  
//            << std::string(message->content(), message->contentLen()) << "|"  
//            << message->creator << "|" << message->forumid << "|" << message->place << "|" 
//            << message->replyOfPost << "|" << message->replyOfComment << "|" << (int)message->type << "|";
//        std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(message->creationDate)));
//        std::cout << std::ctime(&t);

    }
}

void importTag(snb::TagSchema &tagSchema, std::string path, snb::TagClassSchema &tagclassSchema)
{
    auto all_tags = parallel_readlines(path+snb::tagPathSuffix);
    assert(all_tags[0] == "id|name|url|hasType");

    std::vector<uint64_t> tag_vids(all_tags.size()), hasType_vids(all_tags.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_tags.size();i++)
    {
        std::vector<std::string> tag_v = split(all_tags[i], csv_split);
        tag_vids[i] = tagSchema.findId(std::stoull(tag_v[0]));
        hasType_vids[i] = tagclassSchema.findId(std::stoull(tag_v[3]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_tags.size();i++)
    {
        std::vector<std::string> tag_v = split(all_tags[i], csv_split);

        uint64_t tag_vid = tag_vids[i];
        uint64_t hasType_vid = hasType_vids[i];
        tagSchema.insertName(tag_v[1], tag_vid);

        auto tag_buf = snb::TagSchema::createTag(std::stoull(tag_v[0]), tag_v[1], tag_v[2], hasType_vid);

        loader.put_vertex(tag_vid, std::string_view(tag_buf.data(), tag_buf.size()));
        loader.put_edge(hasType_vid, (label_t)snb::EdgeSchema::TagClass2Tag, tag_vid, std::string_view(), true);

//        const snb::TagSchema::Tag *tag = (const snb::TagSchema::Tag*)tag_buf.data();
//        std::cout << tag->id << "|" << std::string(tag->name(), tag->nameLen()) << "|" 
//            << std::string(tag->url(), tag->urlLen()) << "|"  << tag->hasType << std::endl;

    }
}

void importTagClass(snb::TagClassSchema &tagclassSchema, std::string path)
{
    auto all_tagclasss = parallel_readlines(path+snb::tagclassPathSuffix);
    assert(all_tagclasss[0] == "id|name|url|isSubclassOf");

    std::vector<uint64_t> tagclass_vids(all_tagclasss.size()), isSubclassOf_vids(all_tagclasss.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_tagclasss.size();i++)
    {
        std::vector<std::string> tagclass_v = split(all_tagclasss[i], csv_split);
        tagclass_vids[i] = tagclassSchema.findId(std::stoull(tagclass_v[0]));
        isSubclassOf_vids[i] = tagclass_v.size()>3?tagclassSchema.findId(std::stoull(tagclass_v[3])):(uint64_t)-1;
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_tagclasss.size();i++)
    {
        std::vector<std::string> tagclass_v = split(all_tagclasss[i], csv_split);

        uint64_t tagclass_vid = tagclass_vids[i];
        uint64_t isSubclassOf_vid = isSubclassOf_vids[i];
        tagclassSchema.insertName(tagclass_v[1], tagclass_vid);

        auto tagclass_buf = snb::TagClassSchema::createTagClass(std::stoull(tagclass_v[0]), tagclass_v[1], tagclass_v[2], isSubclassOf_vid);

        loader.put_vertex(tagclass_vid, std::string_view(tagclass_buf.data(), tagclass_buf.size()));
        if(isSubclassOf_vid != (uint64_t)-1)
        {
            loader.put_edge(isSubclassOf_vid, (label_t)snb::EdgeSchema::TagClass2TagClass_down, tagclass_vid, std::string_view(), true);
        }

//        const snb::TagClassSchema::TagClass *tagclass = (const snb::TagClassSchema::TagClass*)tagclass_buf.data();
//        std::cout << tagclass->id << "|" << std::string(tagclass->name(), tagclass->nameLen()) << "|" 
//            << std::string(tagclass->url(), tagclass->urlLen()) << "|"  << tagclass->isSubclassOf << std::endl;

    }
}

void importForum(snb::ForumSchema &forumSchema, std::string path, snb::PersonSchema &personSchema)
{
    auto all_forums = parallel_readlines(path+snb::forumPathSuffix);
    assert(all_forums[0] == "id|title|creationDate|moderator");

    std::vector<uint64_t> forum_vids(all_forums.size()), moderator_vids(all_forums.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_forums.size();i++)
    {
        std::vector<std::string> forum_v = split(all_forums[i], csv_split);
        forum_vids[i] = forumSchema.findId(std::stoull(forum_v[0]));
        moderator_vids[i] = personSchema.findId(std::stoull(forum_v[3]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_forums.size();i++)
    {
        std::vector<std::string> forum_v = split(all_forums[i], csv_split);
        std::chrono::system_clock::time_point creationDate = from_time(forum_v[2]);

        uint64_t forum_vid = forum_vids[i];
        uint64_t moderator_vid = moderator_vids[i]; 

        auto forum_buf = snb::ForumSchema::createForum(std::stoull(forum_v[0]), forum_v[1],
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                moderator_vid);

        loader.put_vertex(forum_vid, std::string_view(forum_buf.data(), forum_buf.size()));
        loader.put_edge(moderator_vid, (label_t)snb::EdgeSchema::Person2Forum_moderator, forum_vid, std::string_view(), true);

//        const snb::ForumSchema::Forum *forum = (const snb::ForumSchema::Forum*)forum_buf.data();
//        std::cout << forum->id << "|" << std::string(forum->title(), forum->titleLen()) << "|" 
//            << forum->moderator << "|";
//        std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(forum->creationDate)));
//        std::cout << std::ctime(&t);

    }
}

void importRawEdge(snb::Schema &xSchema, snb::Schema &ySchema, snb::EdgeSchema x2y, snb::EdgeSchema y2x, std::string path)
{
    auto all_rawEdges = parallel_readlines(path);

    std::vector<uint64_t> x_vids(all_rawEdges.size()), y_vids(all_rawEdges.size());
    #pragma omp parallel for
    for(size_t i=1;i<all_rawEdges.size();i++)
    {
        std::vector<std::string> rawEdge_v = split(all_rawEdges[i], csv_split);
        x_vids[i] = xSchema.findId(std::stoull(rawEdge_v[0]));
        y_vids[i] = ySchema.findId(std::stoull(rawEdge_v[1]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_rawEdges.size();i++)
    {
        std::vector<std::string> rawEdge_v = split(all_rawEdges[i], csv_split);

        uint64_t x_vid = x_vids[i];
        uint64_t y_vid = y_vids[i];

        loader.put_edge(x_vid, (label_t)x2y, y_vid, std::string_view(), true);
        loader.put_edge(y_vid, (label_t)y2x, x_vid, std::string_view(), true);
    }
}

void importDataEdge(snb::Schema &xSchema, snb::Schema &ySchema, snb::EdgeSchema x2y, snb::EdgeSchema y2x, std::string path, std::function<snb::Buffer(std::string)> dataParser, bool sort)
{
    auto all_dataEdges = parallel_readlines(path);

    std::vector<std::vector<std::string>> dataEdge_vs;
    for(size_t i=1;i<all_dataEdges.size();i++)
    {
        dataEdge_vs.emplace_back(split(all_dataEdges[i], csv_split));
    }
    all_dataEdges.clear();
    if(sort) tbb::parallel_sort(dataEdge_vs.begin(), dataEdge_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[2] < b[2];
    });

    std::vector<uint64_t> x_vids(dataEdge_vs.size()), y_vids(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto &dataEdge_v = dataEdge_vs[i];
        x_vids[i] = xSchema.findId(std::stoull(dataEdge_v[0]));
        y_vids[i] = ySchema.findId(std::stoull(dataEdge_v[1]));
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto &dataEdge_v = dataEdge_vs[i];
        uint64_t x_vid = x_vids[i];
        uint64_t y_vid = y_vids[i];
        auto dataEdge_buf = dataParser(dataEdge_v[2]);

        loader.put_edge(x_vid, (label_t)x2y, y_vid, std::string_view(dataEdge_buf.data(), dataEdge_buf.size()), true);
        loader.put_edge(y_vid, (label_t)y2x, x_vid, std::string_view(dataEdge_buf.data(), dataEdge_buf.size()), true);
    }
}

void importMember(std::string path)
{
    auto all_dataEdges = parallel_readlines(path);

    std::vector<std::vector<std::string>> dataEdge_vs;
    for(size_t i=1;i<all_dataEdges.size();i++)
    {
        dataEdge_vs.emplace_back(split(all_dataEdges[i], csv_split));
    }
    all_dataEdges.clear();
    tbb::parallel_sort(dataEdge_vs.begin(), dataEdge_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[2] < b[2];
    });

    std::vector<uint64_t> forum_ids(dataEdge_vs.size()), person_ids(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto &dataEdge_v = dataEdge_vs[i];
        forum_ids[i] = forumSchema.findId(std::stoull(dataEdge_v[0]));
        person_ids[i] = personSchema.findId(std::stoull(dataEdge_v[1]));
    }

    std::vector<uint64_t> dataEdge_posts(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto engine = graph->begin_read_only_transaction();
        uint64_t forum_id = forum_ids[i];
        uint64_t person_id = person_ids[i];
        auto nbrs = engine.get_edges(person_id, (label_t)snb::EdgeSchema::Person2Post_creator);
        uint64_t posts = 0;
        while (nbrs.valid())
        {
            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
            if(message->forumid == forum_id) posts++;
            nbrs.next();
        }
        dataEdge_posts[i] = posts;
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto &dataEdge_v = dataEdge_vs[i];
        uint64_t forum_id = forum_ids[i];
        uint64_t person_id = person_ids[i];
        auto dateTime = from_time(dataEdge_v[2]);
        snb::Buffer buf(sizeof(uint64_t)+sizeof(double));
        *(uint64_t*)buf.data() = std::chrono::duration_cast<std::chrono::milliseconds>(dateTime.time_since_epoch()).count();
        *(uint64_t*)(buf.data()+sizeof(uint64_t)) = dataEdge_posts[i];

        loader.put_edge(forum_id, (label_t)snb::EdgeSchema::Forum2Person_member, person_id, std::string_view(buf.data(), buf.size()), true);
        loader.put_edge(person_id, (label_t)snb::EdgeSchema::Person2Forum_member, forum_id, std::string_view(buf.data(), buf.size()), true);
    }
}

void importKnows(std::string path)
{
    auto all_dataEdges = parallel_readlines(path);

    std::vector<std::vector<std::string>> dataEdge_vs;
    for(size_t i=1;i<all_dataEdges.size();i++)
    {
        dataEdge_vs.emplace_back(split(all_dataEdges[i], csv_split));
    }
    all_dataEdges.clear();
    tbb::parallel_sort(dataEdge_vs.begin(), dataEdge_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[2] < b[2];
    });

    std::vector<uint64_t> x_vids(dataEdge_vs.size()), y_vids(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto &dataEdge_v = dataEdge_vs[i];
        x_vids[i] = personSchema.findId(std::stoull(dataEdge_v[0]));
        y_vids[i] = personSchema.findId(std::stoull(dataEdge_v[1]));
    }

    std::vector<double> dataEdge_weight(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto engine = graph->begin_read_only_transaction();
        auto &dataEdge_v = dataEdge_vs[i];
        uint64_t left = x_vids[i];
        uint64_t right = y_vids[i];
        double weight = 0;
        {
            auto nbrs = engine.get_edges(left, (label_t)snb::EdgeSchema::Person2Comment_creator);
            while (nbrs.valid())
            {
                auto comment = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                auto rvid = comment->replyOfPost==(uint64_t)-1?comment->replyOfComment:comment->replyOfPost;
                auto reply = (snb::MessageSchema::Message*)engine.get_vertex(rvid).data();
                if(reply->creator == right) weight += (rvid == comment->replyOfPost)?1.0:0.5;
                nbrs.next();
            }

        }
        {
            auto nbrs = engine.get_edges(right, (label_t)snb::EdgeSchema::Person2Comment_creator);
            while (nbrs.valid())
            {
                auto comment = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                auto rvid = comment->replyOfPost==(uint64_t)-1?comment->replyOfComment:comment->replyOfPost;
                auto reply = (snb::MessageSchema::Message*)engine.get_vertex(rvid).data();
                if(reply->creator == left) weight += (rvid == comment->replyOfPost)?1.0:0.5;
                nbrs.next();
            }
        }
        dataEdge_weight[i] = weight;
    }

    auto loader = graph->begin_batch_loader();

    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto &dataEdge_v = dataEdge_vs[i];
        uint64_t x_vid = x_vids[i];
        uint64_t y_vid = y_vids[i];
        auto dateTime = from_time(dataEdge_v[2]);
        snb::Buffer buf(sizeof(uint64_t)+sizeof(double));
        *(uint64_t*)buf.data() = std::chrono::duration_cast<std::chrono::milliseconds>(dateTime.time_since_epoch()).count();
        *(double*)(buf.data()+sizeof(uint64_t)) = dataEdge_weight[i];

        loader.put_edge(x_vid, (label_t)snb::EdgeSchema::Person2Person, y_vid, std::string_view(buf.data(), buf.size()), true);
        loader.put_edge(y_vid, (label_t)snb::EdgeSchema::Person2Person, x_vid, std::string_view(buf.data(), buf.size()), true);
    }
}

class InteractiveCloneFactory : virtual public InteractiveIfFactory {
public:
    virtual ~InteractiveCloneFactory() {}
    virtual InteractiveIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo)
    {
        stdcxx::shared_ptr<TSocket> sock = stdcxx::dynamic_pointer_cast<TSocket>(connInfo.transport);
//        std::cout << "new connection" << std::endl;
//        std::cout << "Incoming connection\n";
//        std::cout << "\tSocketInfo: "  << sock->getSocketInfo()  << "\n";
//        std::cout << "\tPeerHost: "    << sock->getPeerHost()    << "\n";
//        std::cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
//        std::cout << "\tPeerPort: "    << sock->getPeerPort()    << "\n";
        return new InteractiveHandler(graph, personSchema, placeSchema, orgSchema, postSchema, commentSchema, tagSchema, tagclassSchema, forumSchema);
    }
    virtual void releaseHandler( InteractiveIf* handler) {
//        std::cout << "delete connection" << std::endl;
        delete handler;
    }
};

void signalHandler(int signum)
{
    if(::server) ::server->stop(); 
}

int main(int argc, char** argv)
{
    signal(SIGINT, signalHandler);
    std::string graphPath = argv[1];
    std::string dataPath = argv[2];
    int port = std::stoi(argv[3]);

    graph = new Graph(graphPath+"/graph.mmap", graphPath+"/graph.wal", graphPath+"/graph.save");

    rocksdb::DB *rocksdb_db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.table_cache_numshardbits = 10;
    options.compression = rocksdb::kNoCompression;
    options.bottommost_compression = rocksdb::kNoCompression;

    rocksdb::BlockBasedTableOptions id_table_options;
    id_table_options.index_type = rocksdb::BlockBasedTableOptions::kHashSearch;
    id_table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    id_table_options.block_cache = rocksdb::NewLRUCache(4096ul * 1024 * 1024);

    rocksdb::ColumnFamilyOptions id_options;
    id_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(uint64_t)));
    id_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(id_table_options));
    id_options.compression = rocksdb::kNoCompression;
    id_options.bottommost_compression = rocksdb::kNoCompression;

    rocksdb::BlockBasedTableOptions name_table_options;
    name_table_options.index_type = rocksdb::BlockBasedTableOptions::kBinarySearch;
    name_table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    name_table_options.block_cache = rocksdb::NewLRUCache(4096ul * 1024 * 1024);

    rocksdb::ColumnFamilyOptions name_options;
    name_options.prefix_extractor.reset(rocksdb::NewNoopTransform());
    name_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(name_table_options));
    name_options.compression = rocksdb::kNoCompression;
    name_options.bottommost_compression = rocksdb::kNoCompression;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    column_families.emplace_back("personSchema_vidx", id_options);
    column_families.emplace_back("personSchema_rvidx", id_options);
    column_families.emplace_back("personSchema_nameidx", name_options);
    column_families.emplace_back("placeSchema_vidx", id_options);
    column_families.emplace_back("placeSchema_rvidx", id_options);
    column_families.emplace_back("placeSchema_nameidx", name_options);
    column_families.emplace_back("orgSchema_vidx", id_options);
    column_families.emplace_back("orgSchema_rvidx", id_options);
    column_families.emplace_back("orgSchema_nameidx", name_options);
    column_families.emplace_back("postSchema_vidx", id_options);
    column_families.emplace_back("postSchema_rvidx", id_options);
    column_families.emplace_back("postSchema_nameidx", name_options);
    column_families.emplace_back("commentSchema_vidx", id_options);
    column_families.emplace_back("commentSchema_rvidx", id_options);
    column_families.emplace_back("commentSchema_nameidx", name_options);
    column_families.emplace_back("tagSchema_vidx", id_options);
    column_families.emplace_back("tagSchema_rvidx", id_options);
    column_families.emplace_back("tagSchema_nameidx", name_options);
    column_families.emplace_back("tagclassSchema_vidx", id_options);
    column_families.emplace_back("tagclassSchema_rvidx", id_options);
    column_families.emplace_back("tagclassSchema_nameidx", name_options);
    column_families.emplace_back("forumSchema_vidx", id_options);
    column_families.emplace_back("forumSchema_rvidx", id_options);
    column_families.emplace_back("forumSchema_nameidx", name_options);

    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    rocksdb::DB::Open(options, graphPath, column_families, &handles, &rocksdb_db);

    personSchema   = snb::PersonSchema  (rocksdb_db, handles[ 0], handles[ 1], handles[ 2]);
    placeSchema    = snb::PlaceSchema   (rocksdb_db, handles[ 3], handles[ 4], handles[ 5]);
    orgSchema      = snb::OrgSchema     (rocksdb_db, handles[ 6], handles[ 7], handles[ 8]);
    postSchema     = snb::MessageSchema (rocksdb_db, handles[ 9], handles[10], handles[11]);
    commentSchema  = snb::MessageSchema (rocksdb_db, handles[12], handles[13], handles[14]);
    tagSchema      = snb::TagSchema     (rocksdb_db, handles[15], handles[16], handles[17]);
    tagclassSchema = snb::TagClassSchema(rocksdb_db, handles[18], handles[19], handles[20]);
    forumSchema    = snb::ForumSchema   (rocksdb_db, handles[21], handles[22], handles[23]);

    if(graph->get_max_vertex_id() == 0)
    {
        {
            std::vector<std::thread> pool;
            pool.emplace_back(prepareVIndex, std::ref(personSchema),   dataPath+snb::personPathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(placeSchema),    dataPath+snb::placePathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(orgSchema),      dataPath+snb::orgPathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(postSchema),     dataPath+snb::postPathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(commentSchema),  dataPath+snb::commentPathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(tagSchema),      dataPath+snb::tagPathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(tagclassSchema), dataPath+snb::tagclassPathSuffix);
            pool.emplace_back(prepareVIndex, std::ref(forumSchema),    dataPath+snb::forumPathSuffix);
            for(auto &t:pool) t.join();
        }

        std::cout << "Finish prepareVIndex" << std::endl;

        {
            std::vector<std::thread> pool;
            pool.emplace_back(importPerson,   std::ref(personSchema),   dataPath, std::ref(placeSchema));
            pool.emplace_back(importPlace,    std::ref(placeSchema),    dataPath);
            pool.emplace_back(importOrg,      std::ref(orgSchema),      dataPath, std::ref(placeSchema));
            pool.emplace_back(importPost,     std::ref(postSchema),     dataPath, std::ref(personSchema), std::ref(forumSchema), std::ref(placeSchema));
            pool.emplace_back(importComment,  std::ref(commentSchema),  dataPath, std::ref(personSchema), std::ref(placeSchema), std::ref(postSchema));
            pool.emplace_back(importTag,      std::ref(tagSchema),      dataPath, std::ref(tagclassSchema));
            pool.emplace_back(importTagClass, std::ref(tagclassSchema), dataPath);
            pool.emplace_back(importForum,    std::ref(forumSchema),    dataPath, std::ref(personSchema));

            pool.emplace_back(importRawEdge, std::ref(personSchema),  std::ref(tagSchema), snb::EdgeSchema::Person2Tag,  snb::EdgeSchema::Tag2Person,  dataPath+snb::personHasInterestPathSuffix);
            pool.emplace_back(importRawEdge, std::ref(postSchema),    std::ref(tagSchema), snb::EdgeSchema::Post2Tag,    snb::EdgeSchema::Tag2Post,    dataPath+snb::postHasTagPathSuffix);
            pool.emplace_back(importRawEdge, std::ref(commentSchema), std::ref(tagSchema), snb::EdgeSchema::Comment2Tag, snb::EdgeSchema::Tag2Comment, dataPath+snb::commentHasTagPathSuffix);
            pool.emplace_back(importRawEdge, std::ref(forumSchema),   std::ref(tagSchema), snb::EdgeSchema::Forum2Tag,   snb::EdgeSchema::Tag2Forum,   dataPath+snb::forumHasTagPathSuffix);
            for(auto &t:pool) t.join();
        }
        {
            std::vector<std::thread> pool;

            auto DateTimeParser = [](std::string str)
            {
                auto dateTime = from_time(str);
                snb::Buffer buf(sizeof(uint64_t));
                *(uint64_t*)buf.data() = std::chrono::duration_cast<std::chrono::milliseconds>(dateTime.time_since_epoch()).count();
                return buf;
            };

            auto YearParser = [](std::string str)
            {
                snb::Buffer buf(sizeof(int32_t));
                *(int32_t*)buf.data() = std::stoi(str);
                return buf;
            };

//            pool.emplace_back(importDataEdge, std::ref(forumSchema),  std::ref(personSchema),  snb::EdgeSchema::Forum2Person_member, snb::EdgeSchema::Person2Forum_member, 
//                    dataPath+snb::forumHasMemberPathSuffix, DateTimeParser, true);
//            pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(personSchema),  snb::EdgeSchema::Person2Person,       snb::EdgeSchema::Person2Person, 
//                    dataPath+snb::personKnowsPersonPathSuffix, DateTimeParser, true);
            pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(postSchema),    snb::EdgeSchema::Person2Post_like,    snb::EdgeSchema::Post2Person_like, 
                    dataPath+snb::personLikePostPathSuffix, DateTimeParser, true);
            pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(commentSchema), snb::EdgeSchema::Person2Comment_like, snb::EdgeSchema::Comment2Person_like, 
                    dataPath+snb::personLikeCommentPathSuffix, DateTimeParser, true);
            pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(orgSchema),     snb::EdgeSchema::Person2Org_study,    snb::EdgeSchema::Org2Person_study, 
                    dataPath+snb::personStudyAtOrgPathSuffix, YearParser, true);
            pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(orgSchema),     snb::EdgeSchema::Person2Org_work,     snb::EdgeSchema::Org2Person_work, 
                    dataPath+snb::personWorkAtOrgPathSuffix, YearParser, true);
            pool.emplace_back(importMember, dataPath+snb::forumHasMemberPathSuffix);
            pool.emplace_back(importKnows, dataPath+snb::personKnowsPersonPathSuffix);
            for(auto &t:pool) t.join();
        }

        std::cout << "Finish importing" << std::endl;
    }
    else
    {
        std::cout << "Finish loading" << std::endl;
    }

    //const int workerCount = std::thread::hardware_concurrency();
    //stdcxx::shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(workerCount);
    //threadManager->threadFactory(stdcxx::make_shared<PlatformThreadFactory>());
    //threadManager->start();
    //::server = new TThreadPoolServer(
    //    stdcxx::make_shared<InteractiveProcessorFactory>(stdcxx::make_shared<InteractiveCloneFactory>()),
    //    stdcxx::make_shared<TServerSocket>(port),
    //    stdcxx::make_shared<TBufferedTransportFactory>(),
    //    stdcxx::make_shared<TBinaryProtocolFactory>(),
    //    threadManager);

    ::server = new TThreadedServer(
        stdcxx::make_shared<InteractiveProcessorFactory>(stdcxx::make_shared<InteractiveCloneFactory>()),
        stdcxx::make_shared<TServerSocket>(port),
        stdcxx::make_shared<TBufferedTransportFactory>(),
        stdcxx::make_shared<TBinaryProtocolFactory>());

    std::cout << "Starting the server..." << std::endl;
    ::server->serve();
    delete ::server;
    ::server = nullptr;

    delete graph;
    for(auto p:handles)
    {
        rocksdb_db->DestroyColumnFamilyHandle(p);
    }
    delete rocksdb_db;

    return 0;
}
