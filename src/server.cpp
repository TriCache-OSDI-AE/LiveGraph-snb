#include <ctime>
#include <thread>
#include <chrono>
#include <vector>
#include <cassert>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <functional>
#include <tbb/parallel_sort.h>
#include <tbb/concurrent_vector.h>
#include "core/schema.h" 
#include "core/import.h"
#include "LiveGraph/core/graph.hpp"

#include "Interactive.h"
#include <thrift/transport/TSocket.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::concurrency;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::interactive;

livegraph::Graph *graph = nullptr;
snb::PersonSchema personSchema;
snb::PlaceSchema placeSchema;
snb::OrgSchema orgSchema;
snb::MessageSchema postSchema;
snb::MessageSchema commentSchema;
snb::TagSchema tagSchema;
snb::TagClassSchema tagclassSchema;
snb::ForumSchema forumSchema;

std::vector<std::string> static inline split(const std::string &s, char delim) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, delim)) {
        elems.push_back(std::move(item));
    }
    return elems;
}
std::chrono::system_clock::time_point static inline from_date(const std::string &str)
{
    const std::string date_parse("%Y-%m-%d");
    std::tm tm{};
    std::stringstream ss(str);
    ss >> std::get_time(&tm, date_parse.c_str());
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}
std::chrono::system_clock::time_point static inline from_time(const std::string &str)
{
    const std::string time_parse("%Y-%m-%dT%H:%M:%S.");
    std::tm tm{};
    std::stringstream ss(str);
    int ms, zone;
    char c;
    ss >> std::get_time(&tm, time_parse.c_str()) >> ms >> c >> zone;
    if(c == '+') tm.tm_hour += zone;
    if(c == '-') tm.tm_hour -= zone;
    return std::chrono::system_clock::from_time_t(std::mktime(&tm)) + std::chrono::milliseconds(ms);
}
uint64_t static inline to_time(uint32_t date)
{
    auto tp = std::chrono::system_clock::time_point(std::chrono::hours(date));
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

const static char csv_split('|');
const static char zero_split('\000');

void prepareVIndex(snb::Schema &schema, std::string path)
{
    std::ifstream ifs(path);
    bool first = true;

    auto loader = graph->BeginBatchLoad();
    for(std::string line;std::getline(ifs, line);first = false)
    {
        if(first)
        {
            assert(line.substr(0, 3) == "id|");
            continue;
        }
        auto v = split(line, csv_split);
        size_t id = std::stoul(v[0]);
        size_t vid = loader.AddVertex();
        schema.insertId(id, vid);
    } 
}

void importPerson(snb::PersonSchema &personSchema, std::string path, snb::PlaceSchema &placeSchema)
{
    std::ifstream ifs_person(path+snb::personPathSuffix);
    std::ifstream ifs_emails(path+"/person_email_emailaddress_0_0.csv");
    std::ifstream ifs_speaks(path+"/person_speaks_language_0_0.csv");

    std::vector<std::string> all_persons, all_emails, all_speaks;
    for(std::string line;std::getline(ifs_person, line);) all_persons.push_back(line);
    for(std::string line;std::getline(ifs_emails, line);) all_emails.push_back(line);
    for(std::string line;std::getline(ifs_speaks, line);) all_speaks.push_back(line);
    assert(all_persons[0] == "id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|place");
    assert(all_emails[0] == "Person.id|email");
    assert(all_speaks[0] == "Person.id|language");

    auto loader = graph->BeginBatchLoad();

    for(size_t i=1,j=1,k=1;i<all_persons.size();i++)
    {
        std::vector<std::string> person_v = split(all_persons[i], csv_split);
        std::vector<std::string> emails_v, speaks_v;
        for(;j<all_emails.size();j++)
        {
            auto v = split(all_emails[j], csv_split);
            if(v[0] != person_v[0]) break;
            emails_v.push_back(v[1]);
        }
        for(;k<all_speaks.size();k++)
        {
            auto v = split(all_speaks[k], csv_split);
            if(v[0] != person_v[0]) break;
            speaks_v.push_back(v[1]);
        }

        std::chrono::system_clock::time_point birthday = from_date(person_v[4]);
        std::chrono::system_clock::time_point creationDate = from_time(person_v[5]);
        uint64_t person_vid = personSchema.findId(std::stoull(person_v[0])); 
        uint64_t place_vid = placeSchema.findId(std::stoull(person_v[8]));

        auto person_buf = snb::PersonSchema::createPerson(std::stoull(person_v[0]), person_v[1], person_v[2], person_v[3],  
                std::chrono::duration_cast<std::chrono::hours>(birthday.time_since_epoch()).count(), emails_v, speaks_v, person_v[7], person_v[6],
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(), 
                place_vid);

        loader.PutVertex(person_vid, livegraph::Bytes(person_buf.data(), person_buf.size()));
        loader.PutEdge(place_vid, (EdgeType)snb::EdgeSchema::Place2Person, person_vid, livegraph::Bytes());

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
    std::ifstream ifs_place(path+snb::placePathSuffix);

    std::vector<std::string> all_places;
    for(std::string line;std::getline(ifs_place, line);) all_places.push_back(line);
    assert(all_places[0] == "id|name|url|type|isPartOf");

    auto loader = graph->BeginBatchLoad();

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
            assert(false);
        }

        uint64_t place_vid = placeSchema.findId(std::stoull(place_v[0]));
        uint64_t isPartOf_vid = place_v.size()>4?placeSchema.findId(std::stoull(place_v[4])):(uint64_t)-1;
        placeSchema.insertName(place_v[1], place_vid);

        auto place_buf = snb::PlaceSchema::createPlace(std::stoull(place_v[0]), place_v[1], place_v[2], type, isPartOf_vid);

        loader.PutVertex(place_vid, livegraph::Bytes(place_buf.data(), place_buf.size()));
        if(isPartOf_vid != (uint64_t)-1)
        {
            loader.PutEdge(isPartOf_vid, (EdgeType)snb::EdgeSchema::Place2Place_down, place_vid, livegraph::Bytes());
        }

//        const snb::PlaceSchema::Place *place = (const snb::PlaceSchema::Place*)place_buf.data();
//        std::cout << place->id << "|" << std::string(place->name(), place->nameLen()) << "|" 
//            << std::string(place->url(), place->urlLen()) << "|"  << (int)place->type << "|" << place->isPartOf << std::endl;

    }
}

void importOrg(snb::OrgSchema &orgSchema, std::string path, snb::PlaceSchema &placeSchema)
{
    std::ifstream ifs_org(path+snb::orgPathSuffix);

    std::vector<std::string> all_orgs;
    for(std::string line;std::getline(ifs_org, line);) all_orgs.push_back(line);
    assert(all_orgs[0] == "id|type|name|url|place");

    auto loader = graph->BeginBatchLoad();

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
            assert(false);
        }

        uint64_t org_vid = orgSchema.findId(std::stoull(org_v[0]));
        uint64_t place_vid = placeSchema.findId(std::stoull(org_v[4]));

        auto org_buf = snb::OrgSchema::createOrg(std::stoull(org_v[0]), type, org_v[2], org_v[3], place_vid);

        loader.PutVertex(org_vid, livegraph::Bytes(org_buf.data(), org_buf.size()));
        loader.PutEdge(place_vid, (EdgeType)snb::EdgeSchema::Place2Org, org_vid, livegraph::Bytes());

//        const snb::OrgSchema::Org *org = (const snb::OrgSchema::Org*)org_buf.data();
//        std::cout << org->id << "|" << std::string(org->name(), org->nameLen()) << "|" 
//            << std::string(org->url(), org->urlLen()) << "|"  << (int)org->type << "|" << org->place << std::endl;

    }
}

void importPost(snb::MessageSchema &postSchema, std::string path, snb::PersonSchema &personSchema, snb::ForumSchema &forumSchema, snb::PlaceSchema &placeSchema)
{
    std::ifstream ifs_message(path+snb::postPathSuffix);

    std::vector<std::string> all_messages;
    for(std::string line;std::getline(ifs_message, line);) all_messages.push_back(line);
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

    auto loader = graph->BeginBatchLoad();

    for(auto &message_v : message_vs)
    {
        snb::MessageSchema::Message::Type type = snb::MessageSchema::Message::Type::Post;
        assert(std::stoull(message_v[7]) <= message_v[6].length());
        std::chrono::system_clock::time_point creationDate = from_time(message_v[2]);

        uint64_t message_vid = postSchema.findId(std::stoull(message_v[0]));
        uint64_t creator_vid = personSchema.findId(std::stoull(message_v[8]));
        uint64_t forumid_vid = forumSchema.findId(std::stoull(message_v[9]));
        uint64_t place_vid = placeSchema.findId(std::stoull(message_v[10]));
        uint64_t creationDateInt = std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count();

        auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), message_v[1], 
                creationDateInt,
                message_v[3], message_v[4], message_v[5], message_v[6], 
                creator_vid, forumid_vid, place_vid, (uint64_t)-1, (uint64_t)-1, type);

        loader.PutVertex(message_vid, livegraph::Bytes(message_buf.data(), message_buf.size()));
        loader.PutEdge(forumid_vid, (EdgeType)snb::EdgeSchema::Forum2Post, message_vid, livegraph::Bytes());
        loader.PutEdge(creator_vid, (EdgeType)snb::EdgeSchema::Person2Post_creator, message_vid, livegraph::Bytes((char*)&creationDateInt, sizeof(creationDateInt)));

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
    std::ifstream ifs_message(path+snb::commentPathSuffix);

    std::vector<std::string> all_messages;
    for(std::string line;std::getline(ifs_message, line);) all_messages.push_back(line);
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

    auto loader = graph->BeginBatchLoad();

    for(auto &message_v : message_vs)
    {
        snb::MessageSchema::Message::Type type = snb::MessageSchema::Message::Type::Comment;
        assert(std::stoull(message_v[5]) <= message_v[4].length());
        std::chrono::system_clock::time_point creationDate = from_time(message_v[1]);

        uint64_t message_vid = commentSchema.findId(std::stoull(message_v[0]));
        uint64_t creator_vid = personSchema.findId(std::stoull(message_v[6]));
        uint64_t place_vid = placeSchema.findId(std::stoull(message_v[7]));
        uint64_t replyOfPost_vid = !message_v[8].empty()?postSchema.findId(std::stoull(message_v[8])):(uint64_t)-1;
        uint64_t replyOfComment_vid = message_v.size()>9?commentSchema.findId(std::stoull(message_v[9])):(uint64_t)-1;
        uint64_t creationDateInt = std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count();

        auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), std::string(), 
                creationDateInt,
                message_v[2], message_v[3], std::string(), message_v[4], 
                creator_vid, (uint64_t)-1, place_vid, replyOfPost_vid, replyOfComment_vid, type);

        loader.PutVertex(message_vid, livegraph::Bytes(message_buf.data(), message_buf.size()));
        loader.PutEdge(creator_vid, (EdgeType)snb::EdgeSchema::Person2Comment_creator, message_vid, livegraph::Bytes((char*)&creationDateInt, sizeof(creationDateInt)));
        if(replyOfPost_vid != (uint64_t)-1)
        {
            loader.PutEdge(replyOfPost_vid, (EdgeType)snb::EdgeSchema::Message2Message_down, message_vid, livegraph::Bytes((char*)&creationDateInt, sizeof(creationDateInt)));
        }
        if(replyOfComment_vid != (uint64_t)-1)
        {
            loader.PutEdge(replyOfComment_vid, (EdgeType)snb::EdgeSchema::Message2Message_down, message_vid, livegraph::Bytes((char*)&creationDateInt, sizeof(creationDateInt)));
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
    std::ifstream ifs_tag(path+snb::tagPathSuffix);

    std::vector<std::string> all_tags;
    for(std::string line;std::getline(ifs_tag, line);) all_tags.push_back(line);
    assert(all_tags[0] == "id|name|url|hasType");

    auto loader = graph->BeginBatchLoad();

    for(size_t i=1;i<all_tags.size();i++)
    {
        std::vector<std::string> tag_v = split(all_tags[i], csv_split);

        uint64_t tag_vid = tagSchema.findId(std::stoull(tag_v[0]));
        uint64_t hasType_vid = tagclassSchema.findId(std::stoull(tag_v[3]));
        tagSchema.insertName(tag_v[1], tag_vid);

        auto tag_buf = snb::TagSchema::createTag(std::stoull(tag_v[0]), tag_v[1], tag_v[2], hasType_vid);

        loader.PutVertex(tag_vid, livegraph::Bytes(tag_buf.data(), tag_buf.size()));
        loader.PutEdge(hasType_vid, (EdgeType)snb::EdgeSchema::TagClass2Tag, tag_vid, livegraph::Bytes());

//        const snb::TagSchema::Tag *tag = (const snb::TagSchema::Tag*)tag_buf.data();
//        std::cout << tag->id << "|" << std::string(tag->name(), tag->nameLen()) << "|" 
//            << std::string(tag->url(), tag->urlLen()) << "|"  << tag->hasType << std::endl;

    }
}

void importTagClass(snb::TagClassSchema &tagclassSchema, std::string path)
{
    std::ifstream ifs_tagclass(path+snb::tagclassPathSuffix);

    std::vector<std::string> all_tagclasss;
    for(std::string line;std::getline(ifs_tagclass, line);) all_tagclasss.push_back(line);
    assert(all_tagclasss[0] == "id|name|url|isSubclassOf");

    auto loader = graph->BeginBatchLoad();

    for(size_t i=1;i<all_tagclasss.size();i++)
    {
        std::vector<std::string> tagclass_v = split(all_tagclasss[i], csv_split);

        uint64_t tagclass_vid = tagclassSchema.findId(std::stoull(tagclass_v[0]));
        uint64_t isSubclassOf_vid = tagclass_v.size()>3?tagclassSchema.findId(std::stoull(tagclass_v[3])):(uint64_t)-1;
        tagclassSchema.insertName(tagclass_v[1], tagclass_vid);

        auto tagclass_buf = snb::TagClassSchema::createTagClass(std::stoull(tagclass_v[0]), tagclass_v[1], tagclass_v[2], isSubclassOf_vid);

        loader.PutVertex(tagclass_vid, livegraph::Bytes(tagclass_buf.data(), tagclass_buf.size()));
        if(isSubclassOf_vid != (uint64_t)-1)
        {
            loader.PutEdge(isSubclassOf_vid, (EdgeType)snb::EdgeSchema::TagClass2TagClass_down, tagclass_vid, livegraph::Bytes());
        }

//        const snb::TagClassSchema::TagClass *tagclass = (const snb::TagClassSchema::TagClass*)tagclass_buf.data();
//        std::cout << tagclass->id << "|" << std::string(tagclass->name(), tagclass->nameLen()) << "|" 
//            << std::string(tagclass->url(), tagclass->urlLen()) << "|"  << tagclass->isSubclassOf << std::endl;

    }
}

void importForum(snb::ForumSchema &forumSchema, std::string path, snb::PersonSchema &personSchema)
{
    std::ifstream ifs_forum(path+snb::forumPathSuffix);

    std::vector<std::string> all_forums;
    for(std::string line;std::getline(ifs_forum, line);) all_forums.push_back(line);
    assert(all_forums[0] == "id|title|creationDate|moderator");

    auto loader = graph->BeginBatchLoad();

    for(size_t i=1;i<all_forums.size();i++)
    {
        std::vector<std::string> forum_v = split(all_forums[i], csv_split);
        std::chrono::system_clock::time_point creationDate = from_time(forum_v[2]);

        uint64_t forum_vid = forumSchema.findId(std::stoull(forum_v[0]));
        uint64_t moderator_vid = personSchema.findId(std::stoull(forum_v[3]));

        auto forum_buf = snb::ForumSchema::createForum(std::stoull(forum_v[0]), forum_v[1],
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                moderator_vid);

        loader.PutVertex(forum_vid, livegraph::Bytes(forum_buf.data(), forum_buf.size()));
        loader.PutEdge(moderator_vid, (EdgeType)snb::EdgeSchema::Person2Forum_moderator, forum_vid, livegraph::Bytes());

//        const snb::ForumSchema::Forum *forum = (const snb::ForumSchema::Forum*)forum_buf.data();
//        std::cout << forum->id << "|" << std::string(forum->title(), forum->titleLen()) << "|" 
//            << forum->moderator << "|";
//        std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(forum->creationDate)));
//        std::cout << std::ctime(&t);

    }
}

void importRawEdge(snb::Schema &xSchema, snb::Schema &ySchema, snb::EdgeSchema x2y, snb::EdgeSchema y2x, std::string path)
{
    std::ifstream ifs_rawEdge(path);

    std::vector<std::string> all_rawEdges;
    for(std::string line;std::getline(ifs_rawEdge, line);) all_rawEdges.push_back(line);

    auto loader = graph->BeginBatchLoad();

    for(size_t i=1;i<all_rawEdges.size();i++)
    {
        std::vector<std::string> rawEdge_v = split(all_rawEdges[i], csv_split);

        uint64_t x_vid = xSchema.findId(std::stoull(rawEdge_v[0]));
        uint64_t y_vid = ySchema.findId(std::stoull(rawEdge_v[1]));

        loader.PutEdge(x_vid, (EdgeType)x2y, y_vid, livegraph::Bytes());
        loader.PutEdge(y_vid, (EdgeType)y2x, x_vid, livegraph::Bytes());
    }
}

void importDataEdge(snb::Schema &xSchema, snb::Schema &ySchema, snb::EdgeSchema x2y, snb::EdgeSchema y2x, std::string path, std::function<snb::Buffer(std::string)> dataParser)
{
    std::ifstream ifs_dataEdge(path);

    std::vector<std::string> all_dataEdges;
    for(std::string line;std::getline(ifs_dataEdge, line);) all_dataEdges.push_back(line);

    std::vector<std::vector<std::string>> dataEdge_vs;
    for(size_t i=1;i<all_dataEdges.size();i++)
    {
        dataEdge_vs.emplace_back(split(all_dataEdges[i], csv_split));
    }
    all_dataEdges.clear();
    tbb::parallel_sort(dataEdge_vs.begin(), dataEdge_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[2] < b[2];
    });

    auto loader = graph->BeginBatchLoad();

    for(auto &dataEdge_v : dataEdge_vs)
    {
        uint64_t x_vid = xSchema.findId(std::stoull(dataEdge_v[0]));
        uint64_t y_vid = ySchema.findId(std::stoull(dataEdge_v[1]));
        auto dataEdge_buf = dataParser(dataEdge_v[2]);

        loader.PutEdge(x_vid, (EdgeType)x2y, y_vid, livegraph::Bytes(dataEdge_buf.data(), dataEdge_buf.size()));
        loader.PutEdge(y_vid, (EdgeType)y2x, x_vid, livegraph::Bytes(dataEdge_buf.data(), dataEdge_buf.size()));
    }
}

bool static operator == (const livegraph::Bytes &a, const std::string &b)
{
    if(a.Size() != b.size()) return false;
    for(size_t i=0;i<a.Size();i++) if(a.Data()[i] != b[i]) return false;
    return true;
}

class InteractiveHandler : virtual public InteractiveIf
{
private:
    std::vector<std::pair<int, uint64_t>> multihop_dis(livegraph::Engine &txn, uint64_t root, int num_hops, EdgeType etype)
    {
        std::vector<size_t> frontier = {root};
        std::vector<size_t> next_frontier;
        std::vector<std::pair<int, uint64_t>> collect;

        //TODO: parallel
        for (int k=0;k<num_hops;k++)
        {
            next_frontier.clear();
            for (auto vid : frontier)
            {
                auto nbrs = txn.GetNeighborhood(vid, etype);
                while (nbrs.Valid())
                {
                    if(nbrs.NeighborId() != root)
                    {
                        next_frontier.push_back(nbrs.NeighborId());
                        collect.emplace_back(k, nbrs.NeighborId());
                    }
                    nbrs.Next();
                }
            }
            frontier.swap(next_frontier);
        }

        tbb::parallel_sort(collect.begin(), collect.end());
        auto last = std::unique(collect.begin(), collect.end());
        collect.erase(last, collect.end());
        return collect;
    }

    std::vector<uint64_t> multihop(livegraph::Engine &txn, uint64_t root, int num_hops, EdgeType etype)
    {
        std::vector<size_t> frontier = {root};
        std::vector<size_t> next_frontier;
        std::vector<uint64_t> collect;

        //TODO: parallel
        for (int k=0;k<num_hops;k++)
        {
            next_frontier.clear();
            for (auto vid : frontier)
            {
                auto nbrs = txn.GetNeighborhood(vid, etype);
                while (nbrs.Valid())
                {
                    if(nbrs.NeighborId() != root)
                    {
                        next_frontier.push_back(nbrs.NeighborId());
                        collect.emplace_back(nbrs.NeighborId());
                    }
                    nbrs.Next();
                }
            }
            frontier.swap(next_frontier);
        }

        tbb::parallel_sort(collect.begin(), collect.end());
        auto last = std::unique(collect.begin(), collect.end());
        collect.erase(last, collect.end());
        return collect;
    }

public:
    InteractiveHandler()
    {
    }

    void query1(std::vector<Query1Response> & _return, const Query1Request& request)
    {
        graph->AssignWorkerId();
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->BeginAnalytics();
        auto friends = multihop_dis(engine, vid, 3, (EdgeType)snb::EdgeSchema::Person2Person);
        tbb::concurrent_vector<std::tuple<int, livegraph::Bytes, uint64_t, uint64_t, snb::PersonSchema::Person*>> idx;

        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            auto person = (snb::PersonSchema::Person*)engine.GetVertex(friends[i].second).Data();
            auto firstName = livegraph::Bytes(person->firstName(), person->firstNameLen());
            if(firstName == request.firstName)
            {
                auto lastName = livegraph::Bytes(person->lastName(), person->lastNameLen());
                idx.push_back(std::make_tuple(friends[i].first, lastName, person->id, friends[i].second, person));
            }
        }
        tbb::parallel_sort(idx.begin(), idx.end());
//        std::cout << request.personId << " " << request.firstName << " " << friends.size() << " " << idx.size() << std::endl;

        for(size_t i=0;i<std::min((size_t)request.limit, idx.size());i++)
        {
            Query1Response response;
            auto vid = std::get<3>(idx[i]);
            auto person = std::get<4>(idx[i]);
            response.friendId = person->id;
            response.friendLastName = std::string(person->lastName(), person->lastNameLen());
            response.distanceFromPerson = std::get<0>(idx[i]);
            response.friendGender = std::string(person->gender(), person->genderLen());
            response.friendBrowserUsed = std::string(person->browserUsed(), person->browserUsedLen());
            response.friendEmails = split(std::string(person->emails(), person->emailsLen()), zero_split);
            response.friendLanguages = split(std::string(person->speaks(), person->speaksLen()), zero_split);
            auto place = (snb::PlaceSchema::Place*)engine.GetVertex(person->place).Data();
            response.friendCityName = std::string(place->name(), place->nameLen());
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Org_study);
                while (nbrs.Valid())
                {
                    int year = *(int*)nbrs.EdgeData().Data();
                    uint64_t vid = nbrs.NeighborId();
                    auto org = (snb::OrgSchema::Org*)engine.GetVertex(vid).Data();
                    auto place = (snb::PlaceSchema::Place*)engine.GetVertex(org->place).Data();
                    response.friendUniversities_year.emplace_back(year);
                    response.friendUniversities_name.emplace_back(org->name(), org->nameLen());
                    response.friendUniversities_city.emplace_back(place->name(), place->nameLen());
                    nbrs.Next();
                }
            }
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Org_work);
                while (nbrs.Valid())
                {
                    int year = *(int*)nbrs.EdgeData().Data();
                    uint64_t vid = nbrs.NeighborId();
                    auto org = (snb::OrgSchema::Org*)engine.GetVertex(vid).Data();
                    auto place = (snb::PlaceSchema::Place*)engine.GetVertex(org->place).Data();
                    response.friendUniversities_year.emplace_back(year);
                    response.friendUniversities_name.emplace_back(org->name(), org->nameLen());
                    response.friendUniversities_city.emplace_back(place->name(), place->nameLen());
                    nbrs.Next();
                }
            }
            _return.emplace_back(response);
        }

    }

    void query2(std::vector<Query2Response> & _return, const Query2Request& request)
    {
        graph->AssignWorkerId();
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->BeginAnalytics();
        auto friends = multihop(engine, vid, 1, (EdgeType)snb::EdgeSchema::Person2Person);
        std::map<std::pair<int64_t, uint64_t>, Query2Response> idx;

        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            auto person = (snb::PersonSchema::Person*)engine.GetVertex(friends[i]).Data();
            uint64_t vid = friends[i];
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Post_creator);
                bool flag = true;
                while (nbrs.Valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.EdgeData().Data();
                    if(date <= request.maxDate)
                    {
                        auto key = std::make_pair(-date, postSchema.rfindId(nbrs.NeighborId()));
                        auto message = (snb::MessageSchema::Message*)engine.GetVertex(nbrs.NeighborId()).Data();
                        auto value = Query2Response();
                        value.personId = person->id;
                        value.messageCreationDate = message->creationDate;
                        value.messageId = message->id;
                        #pragma omp critical
                        {
                            if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                            {
                                value.personFirstName = std::string(person->firstName(), person->firstNameLen());
                                value.personLastName = std::string(person->lastName(), person->lastNameLen());
                                value.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
                                idx.emplace(key, value);
                                while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                            }
                            else
                            {
                                flag = false;
                            }
                        }
                    }
                    nbrs.Next();
                }
            }
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Comment_creator);
                bool flag = true;
                while (nbrs.Valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.EdgeData().Data();
                    if(date <= request.maxDate)
                    {
                        auto key = std::make_pair(-date, commentSchema.rfindId(nbrs.NeighborId()));
                        auto message = (snb::MessageSchema::Message*)engine.GetVertex(nbrs.NeighborId()).Data();
                        auto value = Query2Response();
                        value.personId = person->id;
                        value.messageCreationDate = message->creationDate;
                        value.messageId = message->id;
                        #pragma omp critical
                        {
                            if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                            {
                                value.personFirstName = std::string(person->firstName(), person->firstNameLen());
                                value.personLastName = std::string(person->lastName(), person->lastNameLen());
                                value.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
                                idx.emplace(key, value);
                                while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                            }
                            else
                            {
                                flag = false;
                            }
                        }
                    }
                    nbrs.Next();
                }
            }
        }
        for(auto p : idx) _return.push_back(p.second);
//        std::cout << request.personId << " " << idx.size() << std::endl;

    }

    void query3(std::vector<Query3Response> & _return, const Query3Request& request)
    {
        graph->AssignWorkerId();
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        uint64_t countryX = placeSchema.findName(request.countryXName);
        uint64_t countryY = placeSchema.findName(request.countryYName);
        if(vid == (uint64_t)-1) return;
        if(countryX == (uint64_t)-1) return;
        if(countryY == (uint64_t)-1) return;
        uint64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;
        auto engine = graph->BeginAnalytics();
        auto friends = multihop(engine, vid, 2, (EdgeType)snb::EdgeSchema::Person2Person);
        tbb::concurrent_vector<std::tuple<int, uint64_t, int, snb::PersonSchema::Person*>> idx;

        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            auto person = (snb::PersonSchema::Person*)engine.GetVertex(friends[i]).Data();
            auto place = (snb::PlaceSchema::Place*)engine.GetVertex(person->place).Data();
            if(place->isPartOf != countryX && place->isPartOf != countryY)
            {
                uint64_t vid = friends[i];
                int xCount = 0, yCount = 0;
                {
                    auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Comment_creator);
                    while (nbrs.Valid() && *(uint64_t*)nbrs.EdgeData().Data() >= (uint64_t)request.startDate)
                    {
                        uint64_t date = *(uint64_t*)nbrs.EdgeData().Data();
                        if(date < endDate)
                        {
                            auto message = (snb::MessageSchema::Message*)engine.GetVertex(nbrs.NeighborId()).Data();
                            if(message->place == countryX) xCount++;
                            if(message->place == countryY) yCount++;
                        }
                        nbrs.Next();
                    }

                }
                {
                    auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Post_creator);
                    while (nbrs.Valid() && *(uint64_t*)nbrs.EdgeData().Data() >= (uint64_t)request.startDate)
                    {
                        uint64_t date = *(uint64_t*)nbrs.EdgeData().Data();
                        if(date < endDate)
                        {
                            auto message = (snb::MessageSchema::Message*)engine.GetVertex(nbrs.NeighborId()).Data();
                            if(message->place == countryX) xCount++;
                            if(message->place == countryY) yCount++;
                        }
                        nbrs.Next();
                    }

                }
                if(xCount>0 && yCount>0) idx.push_back(std::make_tuple(-xCount, person->id, -yCount, person));
            }
        }
        tbb::parallel_sort(idx.begin(), idx.end());
//        std::cout << request.personId << " " << idx.size() << std::endl;

        for(size_t i=0;i<std::min((size_t)request.limit, idx.size());i++)
        {
            Query3Response response;
            auto person = std::get<3>(idx[i]);
            response.personId = std::get<1>(idx[i]);
            response.personFirstName = std::string(person->firstName(), person->firstNameLen());
            response.personLastName = std::string(person->lastName(), person->lastNameLen());
            response.xCount = -std::get<0>(idx[i]);
            response.yCount = -std::get<2>(idx[i]);
            response.count = response.xCount + response.yCount;
            _return.emplace_back(response);
        }
    }

    void query4(std::vector<Query4Response> & _return, const Query4Request& request)
    {
        graph->AssignWorkerId();
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        uint64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;
        auto engine = graph->BeginAnalytics();
        auto friends = multihop(engine, vid, 1, (EdgeType)snb::EdgeSchema::Person2Person);
        tbb::concurrent_hash_map<uint64_t, std::pair<uint64_t, int>> idx;

        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Post_creator);
            while (nbrs.Valid())
            {
                uint64_t date = *(uint64_t*)nbrs.EdgeData().Data();
                if(date < endDate)
                {
                    uint64_t vid = nbrs.NeighborId();
                    auto message = (snb::MessageSchema::Message*)engine.GetVertex(nbrs.NeighborId()).Data();
                    {
                        auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Post2Tag);
                        while (nbrs.Valid())
                        {
                            tbb::concurrent_hash_map<uint64_t, std::pair<uint64_t, int>>::accessor a;
                            if(date >= (uint64_t)request.startDate)
                            {
                                bool isnew = idx.insert(a, nbrs.NeighborId());
                                if(isnew)
                                {
                                    a->second.first = message->creationDate;
                                    a->second.second = 0;
                                }
                            }
                            else
                            {
                                idx.find(a, nbrs.NeighborId());
                            } 
                            if(!a.empty())
                            {
                                a->second.first = std::min(message->creationDate, a->second.first);
                                a->second.second++;
                            }
                            nbrs.Next();
                        }
                    }
                }
                nbrs.Next();
            }
        }
        std::set<std::pair<int, std::string>> idx_by_count;
        for(auto i=idx.begin();i!=idx.end();i++)
        {
            const char mc = 127;
            auto mkey = std::make_pair(-i->second.second, std::string(&mc, 1));
            if(i->second.first >= (uint64_t)request.startDate && (idx_by_count.size() < (size_t)request.limit || *idx_by_count.rbegin() > mkey))
            {
                auto tag = (snb::TagSchema::Tag*)engine.GetVertex(i->first).Data();
                idx_by_count.emplace(-i->second.second, std::string(tag->name(), tag->nameLen()));
                while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(*idx_by_count.rbegin());
            }
        }
        for(auto p : idx_by_count)
        {
            Query4Response resp;
            resp.postCount = -p.first;
            resp.tagName = p.second;
            _return.push_back(resp);
        }
//        std::cout << request.personId << " " << idx_by_count.size() << std::endl;

    }

    void query5(std::vector<Query5Response> & _return, const Query5Request& request)
    {
        graph->AssignWorkerId();
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->BeginAnalytics();
        auto friends = multihop(engine, vid, 2, (EdgeType)snb::EdgeSchema::Person2Person);
        tbb::concurrent_hash_map<uint64_t, int> idx;
        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            std::unordered_map<uint64_t, int> local_idx;
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Forum_member);
                while (nbrs.Valid() && *(uint64_t*)nbrs.EdgeData().Data()>(uint64_t)request.minDate)
                {
                    local_idx.emplace(nbrs.NeighborId(), 0);
                    nbrs.Next();
                }
            }
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Post_creator);
                while (nbrs.Valid() && *(uint64_t*)nbrs.EdgeData().Data()>(uint64_t)request.minDate)
                {
                    auto message = (snb::MessageSchema::Message*)engine.GetVertex(nbrs.NeighborId()).Data();
                    auto iter = local_idx.find(message->forumid);
                    if(iter != local_idx.end()) iter->second++;
                    nbrs.Next();
                }
            }
            for(auto p:local_idx)
            {
                tbb::concurrent_hash_map<uint64_t, int>::accessor a;
                idx.insert(a, p.first);
                a->second += p.second;
            }
        }
        std::set<std::pair<int, std::string>> idx_by_count;
        for(auto i=idx.begin();i!=idx.end();i++)
        {
            const char mc = 127;
            auto mkey = std::make_pair(-i->second, std::string(&mc, 1));
            if(idx_by_count.size() < (size_t)request.limit || *idx_by_count.rbegin() > mkey)
            {
                auto forum = (snb::ForumSchema::Forum*)engine.GetVertex(i->first).Data();
                idx_by_count.emplace(-i->second, std::string(forum->title(), forum->titleLen()));
                while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(*idx_by_count.rbegin());
            }
        }
        for(auto p : idx_by_count)
        {
            Query5Response resp;
            resp.postCount = -p.first;
            resp.forumTitle = p.second;
            _return.push_back(resp);
        }
//        std::cout << request.personId << " " << idx_by_count.size() << std::endl;
    }

    void query6(std::vector<Query6Response> & _return, const Query6Request& request)
    {
        graph->AssignWorkerId();
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        uint64_t tagId = tagSchema.findName(request.tagName);
        if(tagId == (uint64_t)-1) return;
        auto engine = graph->BeginAnalytics();
        auto friends = multihop(engine, vid, 2, (EdgeType)snb::EdgeSchema::Person2Person);
        tbb::concurrent_hash_map<uint64_t, int> idx;
        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            {
                auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Person2Post_creator);
                while (nbrs.Valid())
                {
                    uint64_t vid = nbrs.NeighborId();
                    bool hit = false;
                    {
                        auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Post2Tag);
                        while (nbrs.Valid() && !hit)
                        {
                            if(nbrs.NeighborId() == tagId) hit = true;
                            nbrs.Next();
                        }
                    }
                    if(hit)
                    {
                        auto nbrs = engine.GetNeighborhood(vid, (EdgeType)snb::EdgeSchema::Post2Tag);
                        while (nbrs.Valid())
                        {
                            if(nbrs.NeighborId() != tagId)
                            {
                                tbb::concurrent_hash_map<uint64_t, int>::accessor a;
                                idx.insert(a, nbrs.NeighborId());
                                a->second ++;
                            }
                            nbrs.Next();
                        }
                    }
                    nbrs.Next();
                }
            }
        }
        std::set<std::pair<int, std::string>> idx_by_count;
        for(auto i=idx.begin();i!=idx.end();i++)
        {
            const char mc = 127;
            auto mkey = std::make_pair(-i->second, std::string(&mc, 1));
            if(idx_by_count.size() < (size_t)request.limit || *idx_by_count.rbegin() > mkey)
            {
                auto tag = (snb::TagSchema::Tag*)engine.GetVertex(i->first).Data();
                idx_by_count.emplace(-i->second, std::string(tag->name(), tag->nameLen()));
                while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(*idx_by_count.rbegin());
            }
        }
        for(auto p : idx_by_count)
        {
            Query6Response resp;
            resp.postCount = -p.first;
            resp.tagName = p.second;
            _return.push_back(resp);
        }
//        std::cout << request.personId << " " << idx_by_count.size() << std::endl;

    }

    void shortQuery1(ShortQuery1Response& _return, const ShortQuery1Request& request)
    {
        // Your implementation goes here
        _return = ShortQuery1Response();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto txn = graph->BeginTransaction();
        auto bytes = txn.GetVertex(personSchema.findId(request.personId));
        auto person = (snb::PersonSchema::Person*)bytes.Data();
        _return.firstName = std::string(person->firstName(), person->firstNameLen());
        _return.lastName = std::string(person->lastName(), person->lastNameLen());
        _return.birthday = to_time(person->birthday);
        _return.locationIp = std::string(person->locationIP(), person->locationIPLen());
        _return.browserUsed = std::string(person->browserUsed(), person->browserUsedLen());
        _return.cityId = placeSchema.rfindId(person->place);
        _return.gender = std::string(person->gender(), person->genderLen());
        _return.creationDate = person->creationDate;
    }

};

class InteractiveCloneFactory : virtual public InteractiveIfFactory {
public:
    virtual ~InteractiveCloneFactory() {}
    virtual InteractiveIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo)
    {
        stdcxx::shared_ptr<TSocket> sock = stdcxx::dynamic_pointer_cast<TSocket>(connInfo.transport);
//        std::cout << "Incoming connection\n";
//        std::cout << "\tSocketInfo: "  << sock->getSocketInfo()  << "\n";
//        std::cout << "\tPeerHost: "    << sock->getPeerHost()    << "\n";
//        std::cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
//        std::cout << "\tPeerPort: "    << sock->getPeerPort()    << "\n";
        return new InteractiveHandler;
    }
    virtual void releaseHandler( InteractiveIf* handler) {
        delete handler;
    }
};


int main(int argc, char** argv)
{
    std::string graphPath = argv[1];
    std::string dataPath = argv[2];
    int port = std::stoi(argv[3]);

    graph = new livegraph::Graph(graphPath);

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

        pool.emplace_back(importDataEdge, std::ref(forumSchema), std::ref(personSchema),   snb::EdgeSchema::Forum2Person_member, snb::EdgeSchema::Person2Forum_member, 
                dataPath+snb::forumHasMemberPathSuffix, DateTimeParser);
        pool.emplace_back(importDataEdge, std::ref(forumSchema), std::ref(personSchema),   snb::EdgeSchema::Forum2Person_member, snb::EdgeSchema::Person2Forum_member, 
                dataPath+snb::forumHasMemberPathSuffix, DateTimeParser);
        pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(personSchema),  snb::EdgeSchema::Person2Person,       snb::EdgeSchema::Person2Person, 
                dataPath+snb::personKnowsPersonPathSuffix, DateTimeParser);
        pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(commentSchema), snb::EdgeSchema::Person2Comment_like, snb::EdgeSchema::Comment2Person_like, 
                dataPath+snb::personLikeCommentPathSuffix, DateTimeParser);
        pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(orgSchema),     snb::EdgeSchema::Person2Org_study,    snb::EdgeSchema::Org2Person_study, 
                dataPath+snb::personStudyAtOrgPathSuffix, YearParser);
        pool.emplace_back(importDataEdge, std::ref(personSchema), std::ref(orgSchema),     snb::EdgeSchema::Person2Org_work,     snb::EdgeSchema::Org2Person_work, 
                dataPath+snb::personWorkAtOrgPathSuffix, YearParser);
        for(auto &t:pool) t.join();
    }

    std::cout << "Finish import" << std::endl;

    const int workerCount = std::thread::hardware_concurrency();
    stdcxx::shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(workerCount);
    threadManager->threadFactory(stdcxx::make_shared<PlatformThreadFactory>());
    threadManager->start();

//    TThreadedServer server(
//        stdcxx::make_shared<InteractiveProcessorFactory>(stdcxx::make_shared<InteractiveCloneFactory>()),
//        stdcxx::make_shared<TServerSocket>(port),
//        stdcxx::make_shared<TBufferedTransportFactory>(),
//        stdcxx::make_shared<TBinaryProtocolFactory>());

    TThreadPoolServer server(
        stdcxx::make_shared<InteractiveProcessorFactory>(stdcxx::make_shared<InteractiveCloneFactory>()),
        stdcxx::make_shared<TServerSocket>(port),
        stdcxx::make_shared<TBufferedTransportFactory>(),
        stdcxx::make_shared<TBinaryProtocolFactory>(),
        threadManager);

    std::cout << "Starting the server..." << std::endl;
    server.serve();

    delete graph;
    return 0;
}
