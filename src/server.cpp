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
#include <string_view>
#include <unordered_set>
#include <tbb/parallel_sort.h>
#include <tbb/concurrent_vector.h>
#include "core/schema.h" 
#include "core/import.h"
#include "LiveGraph/bind/livegraph.hpp"
#include "date/include/date/tz.h"

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
using namespace lg;

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
std::pair<int, int> static inline to_monday(uint64_t date)
{
    auto tp = std::chrono::system_clock::time_point(std::chrono::hours(date));
    auto dp = date::floor<date::days>(tp);
    auto ymd = date::year_month_day(dp);
    int month = (unsigned)ymd.month();
    int day = (unsigned)ymd.day();
    return std::make_pair(month, day);
}

const static char csv_split('|');
const static char zero_split('\000');

void prepareVIndex(snb::Schema &schema, std::string path)
{
    std::ifstream ifs(path);
    bool first = true;

    auto loader = graph->begin_batch_loader();
    for(std::string line;std::getline(ifs, line);first = false)
    {
        if(first)
        {
            assert(line.substr(0, 3) == "id|");
            continue;
        }
        auto v = split(line, csv_split);
        uint64_t id = std::stoul(v[0]);
        uint64_t vid = loader.new_vertex();
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

    auto loader = graph->begin_batch_loader();

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
        std::sort(speaks_v.begin(), speaks_v.end());

        std::chrono::system_clock::time_point birthday = from_date(person_v[4]);
        std::chrono::system_clock::time_point creationDate = from_time(person_v[5]);
        uint64_t person_vid = personSchema.findId(std::stoull(person_v[0])); 
        uint64_t place_vid = placeSchema.findId(std::stoull(person_v[8]));

        auto person_buf = snb::PersonSchema::createPerson(std::stoull(person_v[0]), person_v[1], person_v[2], person_v[3],  
                std::chrono::duration_cast<std::chrono::hours>(birthday.time_since_epoch()).count(), emails_v, speaks_v, person_v[7], person_v[6],
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(), 
                place_vid);

        loader.put_vertex(person_vid, std::string_view(person_buf.data(), person_buf.size()));
        loader.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Person, person_vid, std::string_view());

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
            assert(false);
        }

        uint64_t place_vid = placeSchema.findId(std::stoull(place_v[0]));
        uint64_t isPartOf_vid = place_v.size()>4?placeSchema.findId(std::stoull(place_v[4])):(uint64_t)-1;
        placeSchema.insertName(place_v[1], place_vid);

        auto place_buf = snb::PlaceSchema::createPlace(std::stoull(place_v[0]), place_v[1], place_v[2], type, isPartOf_vid);

        loader.put_vertex(place_vid, std::string_view(place_buf.data(), place_buf.size()));
        if(isPartOf_vid != (uint64_t)-1)
        {
            loader.put_edge(isPartOf_vid, (label_t)snb::EdgeSchema::Place2Place_down, place_vid, std::string_view());
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
            assert(false);
        }

        uint64_t org_vid = orgSchema.findId(std::stoull(org_v[0]));
        uint64_t place_vid = placeSchema.findId(std::stoull(org_v[4]));

        auto org_buf = snb::OrgSchema::createOrg(std::stoull(org_v[0]), type, org_v[2], org_v[3], place_vid);

        loader.put_vertex(org_vid, std::string_view(org_buf.data(), org_buf.size()));
        loader.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Org, org_vid, std::string_view());

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

    auto loader = graph->begin_batch_loader();

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

        loader.put_vertex(message_vid, std::string_view(message_buf.data(), message_buf.size()));
        loader.put_edge(forumid_vid, (label_t)snb::EdgeSchema::Forum2Post, message_vid, std::string_view());
        loader.put_edge(creator_vid, (label_t)snb::EdgeSchema::Person2Post_creator, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)));

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

    auto loader = graph->begin_batch_loader();

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

        loader.put_vertex(message_vid, std::string_view(message_buf.data(), message_buf.size()));
        loader.put_edge(creator_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)));
        if(replyOfPost_vid != (uint64_t)-1)
        {
            loader.put_edge(replyOfPost_vid, (label_t)snb::EdgeSchema::Message2Message_down, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)));
        }
        if(replyOfComment_vid != (uint64_t)-1)
        {
            loader.put_edge(replyOfComment_vid, (label_t)snb::EdgeSchema::Message2Message_down, message_vid, std::string_view((char*)&creationDateInt, sizeof(creationDateInt)));
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

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_tags.size();i++)
    {
        std::vector<std::string> tag_v = split(all_tags[i], csv_split);

        uint64_t tag_vid = tagSchema.findId(std::stoull(tag_v[0]));
        uint64_t hasType_vid = tagclassSchema.findId(std::stoull(tag_v[3]));
        tagSchema.insertName(tag_v[1], tag_vid);

        auto tag_buf = snb::TagSchema::createTag(std::stoull(tag_v[0]), tag_v[1], tag_v[2], hasType_vid);

        loader.put_vertex(tag_vid, std::string_view(tag_buf.data(), tag_buf.size()));
        loader.put_edge(hasType_vid, (label_t)snb::EdgeSchema::TagClass2Tag, tag_vid, std::string_view());

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

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_tagclasss.size();i++)
    {
        std::vector<std::string> tagclass_v = split(all_tagclasss[i], csv_split);

        uint64_t tagclass_vid = tagclassSchema.findId(std::stoull(tagclass_v[0]));
        uint64_t isSubclassOf_vid = tagclass_v.size()>3?tagclassSchema.findId(std::stoull(tagclass_v[3])):(uint64_t)-1;
        tagclassSchema.insertName(tagclass_v[1], tagclass_vid);

        auto tagclass_buf = snb::TagClassSchema::createTagClass(std::stoull(tagclass_v[0]), tagclass_v[1], tagclass_v[2], isSubclassOf_vid);

        loader.put_vertex(tagclass_vid, std::string_view(tagclass_buf.data(), tagclass_buf.size()));
        if(isSubclassOf_vid != (uint64_t)-1)
        {
            loader.put_edge(isSubclassOf_vid, (label_t)snb::EdgeSchema::TagClass2TagClass_down, tagclass_vid, std::string_view());
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

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_forums.size();i++)
    {
        std::vector<std::string> forum_v = split(all_forums[i], csv_split);
        std::chrono::system_clock::time_point creationDate = from_time(forum_v[2]);

        uint64_t forum_vid = forumSchema.findId(std::stoull(forum_v[0]));
        uint64_t moderator_vid = personSchema.findId(std::stoull(forum_v[3]));

        auto forum_buf = snb::ForumSchema::createForum(std::stoull(forum_v[0]), forum_v[1],
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                moderator_vid);

        loader.put_vertex(forum_vid, std::string_view(forum_buf.data(), forum_buf.size()));
        loader.put_edge(moderator_vid, (label_t)snb::EdgeSchema::Person2Forum_moderator, forum_vid, std::string_view());

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

    auto loader = graph->begin_batch_loader();

    for(size_t i=1;i<all_rawEdges.size();i++)
    {
        std::vector<std::string> rawEdge_v = split(all_rawEdges[i], csv_split);

        uint64_t x_vid = xSchema.findId(std::stoull(rawEdge_v[0]));
        uint64_t y_vid = ySchema.findId(std::stoull(rawEdge_v[1]));

        loader.put_edge(x_vid, (label_t)x2y, y_vid, std::string_view());
        loader.put_edge(y_vid, (label_t)y2x, x_vid, std::string_view());
    }
}

void importDataEdge(snb::Schema &xSchema, snb::Schema &ySchema, snb::EdgeSchema x2y, snb::EdgeSchema y2x, std::string path, std::function<snb::Buffer(std::string)> dataParser, bool sort)
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
    if(sort) tbb::parallel_sort(dataEdge_vs.begin(), dataEdge_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
        return a[2] < b[2];
    });

    auto loader = graph->begin_batch_loader();

    for(auto &dataEdge_v : dataEdge_vs)
    {
        uint64_t x_vid = xSchema.findId(std::stoull(dataEdge_v[0]));
        uint64_t y_vid = ySchema.findId(std::stoull(dataEdge_v[1]));
        auto dataEdge_buf = dataParser(dataEdge_v[2]);

        loader.put_edge(x_vid, (label_t)x2y, y_vid, std::string_view(dataEdge_buf.data(), dataEdge_buf.size()));
        loader.put_edge(y_vid, (label_t)y2x, x_vid, std::string_view(dataEdge_buf.data(), dataEdge_buf.size()));
    }
}

void importMember(std::string path)
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

    std::vector<uint64_t> dataEdge_posts(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto engine = graph->begin_read_only_transaction();
        auto &dataEdge_v = dataEdge_vs[i];
        uint64_t forum_id = forumSchema.findId(std::stoull(dataEdge_v[0]));
        uint64_t person_id = personSchema.findId(std::stoull(dataEdge_v[1]));
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
        uint64_t forum_id = forumSchema.findId(std::stoull(dataEdge_v[0]));
        uint64_t person_id = personSchema.findId(std::stoull(dataEdge_v[1]));
        auto dateTime = from_time(dataEdge_v[2]);
        snb::Buffer buf(sizeof(uint64_t)+sizeof(double));
        *(uint64_t*)buf.data() = std::chrono::duration_cast<std::chrono::milliseconds>(dateTime.time_since_epoch()).count();
        *(uint64_t*)(buf.data()+sizeof(uint64_t)) = dataEdge_posts[i];

        loader.put_edge(forum_id, (label_t)snb::EdgeSchema::Forum2Person_member, person_id, std::string_view(buf.data(), buf.size()));
        loader.put_edge(person_id, (label_t)snb::EdgeSchema::Person2Forum_member, forum_id, std::string_view(buf.data(), buf.size()));
    }
}

void importKnows(std::string path)
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

    std::vector<double> dataEdge_weight(dataEdge_vs.size());
    #pragma omp parallel for
    for(size_t i=0;i<dataEdge_vs.size();i++)
    {
        auto engine = graph->begin_read_only_transaction();
        auto &dataEdge_v = dataEdge_vs[i];
        uint64_t left = personSchema.findId(std::stoull(dataEdge_v[0]));
        uint64_t right = personSchema.findId(std::stoull(dataEdge_v[1]));
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
        uint64_t x_vid = personSchema.findId(std::stoull(dataEdge_v[0]));
        uint64_t y_vid = personSchema.findId(std::stoull(dataEdge_v[1]));
        auto dateTime = from_time(dataEdge_v[2]);
        snb::Buffer buf(sizeof(uint64_t)+sizeof(double));
        *(uint64_t*)buf.data() = std::chrono::duration_cast<std::chrono::milliseconds>(dateTime.time_since_epoch()).count();
        *(double*)(buf.data()+sizeof(uint64_t)) = dataEdge_weight[i];

        loader.put_edge(x_vid, (label_t)snb::EdgeSchema::Person2Person, y_vid, std::string_view(buf.data(), buf.size()));
        loader.put_edge(y_vid, (label_t)snb::EdgeSchema::Person2Person, x_vid, std::string_view(buf.data(), buf.size()));
    }
}

class InteractiveHandler : virtual public InteractiveIf
{
private:
    std::vector<uint64_t> multihop(Transaction &txn, uint64_t root, int num_hops, std::vector<label_t> etypes)
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
                auto nbrs = txn.get_edges(vid, etypes[k]);
                while (nbrs.valid())
                {
                    if(nbrs.dst_id() != root)
                    {
                        next_frontier.push_back(nbrs.dst_id());
                        collect.emplace_back(nbrs.dst_id());
                    }
                    nbrs.next();
                }
            }
            frontier.swap(next_frontier);
        }

        std::sort(collect.begin(), collect.end());
        auto last = std::unique(collect.begin(), collect.end());
        collect.erase(last, collect.end());
        return collect;
    }

    std::vector<uint64_t> multihop_another_etype(Transaction &txn, uint64_t root, int num_hops, label_t etype, label_t another)
    {
        std::vector<size_t> frontier = {root};
        std::vector<size_t> next_frontier;
        std::vector<uint64_t> collect;

        //TODO: parallel
        for (int k=0;k<num_hops && !frontier.empty();k++)
        {
            next_frontier.clear();
            for (auto vid : frontier)
            {
                auto nbrs = txn.get_edges(vid, etype);
                while (nbrs.valid())
                {
                    next_frontier.push_back(nbrs.dst_id());
                    nbrs.next();
                }
            }
            for (auto vid : frontier)
            {
                auto nbrs = txn.get_edges(vid, another);
                while (nbrs.valid())
                {
                    collect.push_back(nbrs.dst_id());
                    nbrs.next();
                }
            }
            frontier.swap(next_frontier);
        }

        std::sort(collect.begin(), collect.end());
        auto last = std::unique(collect.begin(), collect.end());
        collect.erase(last, collect.end());
        return collect;
    }

    int pairwiseShortestPath(Transaction & txn, uint64_t vid_from, uint64_t vid_to, label_t etype, int max_hops = std::numeric_limits<int>::max()) 
    {
        std::unordered_map<uint64_t, uint64_t> parent, child;
        std::vector<uint64_t> forward_q, backward_q;
        parent[vid_from] = vid_from;
        child[vid_to] = vid_to;
        forward_q.push_back(vid_from);
        backward_q.push_back(vid_to);
        int hops = 0;
        while (hops++ < max_hops) 
        {
            std::vector<uint64_t> new_front;
            // decide which way to search first
            if (forward_q.size() <= backward_q.size()) 
            {
                // search forward
                for (uint64_t vid : forward_q) 
                {
                    auto out_edges = txn.get_edges(vid, etype);
                    while (out_edges.valid()) 
                    {
                        uint64_t dst = out_edges.dst_id();
                        if (child.find(dst) != child.end()) 
                        {
                            // found the path
                            return hops;
                        }
                        auto it = parent.find(dst);
                        if (it == parent.end()) 
                        {
                            parent.emplace_hint(it, dst, vid);
                            new_front.push_back(dst);
                        }
                        out_edges.next();
                    }
                }
                if (new_front.empty()) break;
                forward_q.swap(new_front);
            }
            else 
            {
                for (uint64_t vid : backward_q) 
                {
                    auto in_edges = txn.get_edges(vid, etype);
                    while (in_edges.valid()) 
                    {
                        uint64_t src = in_edges.dst_id();
                        if (parent.find(src) != parent.end()) 
                        {
                            // found the path
                            return hops;
                        }
                        auto it = child.find(src);
                        if (it == child.end()) 
                        {
                            child.emplace_hint(it, src, vid);
                            new_front.push_back(src);
                        }
                        in_edges.next();
                    }
                }
                if (new_front.empty()) break;
                backward_q.swap(new_front);
            }
        }
        return -1;
    }

    std::vector<std::pair<double, std::vector<uint64_t>>> pairwiseShortestPath_path(Transaction & txn, uint64_t vid_from, uint64_t vid_to, label_t etype, int max_hops = std::numeric_limits<int>::max()) 
    {
        std::unordered_map<uint64_t, int> parent, child;
        std::vector<uint64_t> forward_q, backward_q;
        parent[vid_from] = 0;
        child[vid_to] = 0;
        forward_q.push_back(vid_from);
        backward_q.push_back(vid_to);
        int hops = 0, psp = max_hops, fhops = 0, bhops = 0;
        std::map<std::pair<uint64_t, uint64_t>, double> hits;
        while (hops++ < std::min(psp, max_hops)) 
        {
            std::vector<uint64_t> new_front;
            if (forward_q.size() <= backward_q.size()) 
            {
                fhops++;
                // search forward
                for (uint64_t vid : forward_q) 
                {
                    auto out_edges = txn.get_edges(vid, etype);
                    while (out_edges.valid()) 
                    {
                        uint64_t dst = out_edges.dst_id();
                        double weight = *(double*)(out_edges.edge_data().data()+sizeof(uint64_t));
                        if (child.find(dst) != child.end()) 
                        {
                            hits.emplace(std::make_pair(vid, dst), weight);
                            psp = hops;
                        }
                        auto it = parent.find(dst);
                        if (it == parent.end()) 
                        {
                            parent.emplace_hint(it, dst, fhops);
                            new_front.push_back(dst);
                        }
                        out_edges.next();
                    }
                }
                if (new_front.empty()) break;
                forward_q.swap(new_front);
            }
            else 
            {
                bhops++;
                for (uint64_t vid : backward_q) 
                {
                    auto in_edges = txn.get_edges(vid, etype);
                    while (in_edges.valid()) 
                    {
                        uint64_t src = in_edges.dst_id();
                        double weight = *(double*)(in_edges.edge_data().data()+sizeof(uint64_t));
                        if (parent.find(src) != parent.end()) 
                        {
                            hits.emplace(std::make_pair(src, vid), weight);
                            psp = hops;
                        }
                        auto it = child.find(src);
                        if (it == child.end()) 
                        {
                            child.emplace_hint(it, src, bhops);
                            new_front.push_back(src);
                        }
                        in_edges.next();
                    }
                }
                if (new_front.empty()) break;
                backward_q.swap(new_front);
            }
        }

        std::vector<std::pair<double, std::vector<uint64_t>>> paths;
        for(auto p : hits)
        {
            std::vector<size_t> path;
            std::vector<std::pair<double, std::vector<uint64_t>>> fpaths, bpaths;
            std::function<void(uint64_t, int, double)> fdfs = [&](uint64_t vid, int deep, double weight)
            {
                path.push_back(vid);
                if(deep > 0)
                {
                    auto out_edges = txn.get_edges(vid, etype);
                    while (out_edges.valid()) 
                    {
                        uint64_t dst = out_edges.dst_id();
                        auto iter = parent.find(dst);
                        if(iter != parent.end() && iter->second == deep-1)
                        {
                            double w = *(double*)(out_edges.edge_data().data()+sizeof(uint64_t));
                            fdfs(dst, deep-1, weight+w);
                        }
                        out_edges.next();
                    }
                }
                else
                {
                    fpaths.emplace_back();
                    fpaths.back().first = weight;
                    std::reverse_copy(path.begin(), path.end(), std::back_inserter(fpaths.back().second));
                }
                path.pop_back();
            };

            std::function<void(uint64_t, int, double)> bdfs = [&](uint64_t vid, int deep, double weight)
            {
                path.push_back(vid);
                if(deep > 0)
                {
                    auto out_edges = txn.get_edges(vid, etype);
                    while (out_edges.valid()) 
                    {
                        uint64_t dst = out_edges.dst_id();
                        auto iter = child.find(dst);
                        if(iter != child.end() && iter->second == deep-1)
                        {
                            double w = *(double*)(out_edges.edge_data().data()+sizeof(uint64_t));
                            bdfs(dst, deep-1, weight+w);
                        }
                        out_edges.next();
                    }
                }
                else
                {
                    bpaths.emplace_back(weight, path);
                }
                path.pop_back();
            };

            fdfs(p.first.first, parent[p.first.first], 0.0);
            bdfs(p.first.second, child[p.first.second], 0.0);
            for(auto &f: fpaths)
            {
                for(auto &b: bpaths)
                {
                    paths.emplace_back(f.first+b.first+p.second, f.second);
                    std::copy(b.second.begin(), b.second.end(), std::back_inserter(paths.back().second));
                }
            }
        }
        return paths;
    }

public:
    InteractiveHandler()
    {
    }

    void query1(std::vector<Query1Response> & _return, const Query1Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        std::vector<std::tuple<int, std::string_view, uint64_t, uint64_t, snb::PersonSchema::Person*>> idx;

        std::vector<size_t> frontier = {vid};
        std::vector<size_t> next_frontier;
        std::unordered_set<uint64_t> person_hash{vid};
        uint64_t root = vid;

        for (int k=0;k<3 && idx.size()<(size_t)request.limit;k++)
        {
            next_frontier.clear();
            for (auto vid : frontier)
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Person);
                while (nbrs.valid())
                {
                    if(nbrs.dst_id() != root && person_hash.find(nbrs.dst_id()) == person_hash.end())
                    {
                        next_frontier.push_back(nbrs.dst_id());
                        person_hash.emplace(nbrs.dst_id());
                        auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
                        auto firstName = std::string_view(person->firstName(), person->firstNameLen());
                        if(firstName == request.firstName)
                        {
                            auto lastName = std::string_view(person->lastName(), person->lastNameLen());
                            idx.push_back(std::make_tuple(k, lastName, person->id, nbrs.dst_id(), person));
                        }
                    }
                    nbrs.next();
                }
            }
            frontier.swap(next_frontier);
        }

        std::sort(idx.begin(), idx.end());
//        tbb::parallel_sort(idx.begin(), idx.end());
//        std::cout << request.personId << " " << request.firstName << " " << friends.size() << " " << idx.size() << std::endl;

        for(size_t i=0;i<std::min((size_t)request.limit, idx.size());i++)
        {
            _return.emplace_back();
            auto vid = std::get<3>(idx[i]);
            auto person = std::get<4>(idx[i]);
            _return.back().friendId = person->id;
            _return.back().friendLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().distanceFromPerson = std::get<0>(idx[i])+1;
            _return.back().friendBirthday = to_time(person->birthday);
            _return.back().friendCreationDate = person->creationDate;
            _return.back().friendGender = std::string_view(person->gender(), person->genderLen());
            _return.back().friendBrowserUsed = std::string_view(person->browserUsed(), person->browserUsedLen());
            _return.back().friendLocationIp = std::string_view(person->locationIP(), person->locationIPLen());
            _return.back().friendEmails = split(std::string(person->emails(), person->emailsLen()), zero_split);
            _return.back().friendLanguages = split(std::string(person->speaks(), person->speaksLen()), zero_split);
            auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
            _return.back().friendCityName = std::string_view(place->name(), place->nameLen());
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Org_study);
                std::vector<std::tuple<uint64_t, int, snb::OrgSchema::Org*, snb::PlaceSchema::Place*>> orgs;
                while (nbrs.valid())
                {
                    int year = *(int*)nbrs.edge_data().data();
                    uint64_t vid = nbrs.dst_id();
                    auto org = (snb::OrgSchema::Org*)engine.get_vertex(vid).data();
                    auto place = (snb::PlaceSchema::Place*)engine.get_vertex(org->place).data();
                    orgs.emplace_back(org->id, year, org, place);
                    nbrs.next();
                }
                std::sort(orgs.begin(), orgs.end());
                for(auto t:orgs)
                {
                    int year = std::get<1>(t);
                    auto org = std::get<2>(t);
                    auto place = std::get<3>(t);
                    _return.back().friendUniversities_year.emplace_back(year);
                    _return.back().friendUniversities_name.emplace_back(org->name(), org->nameLen());
                    _return.back().friendUniversities_city.emplace_back(place->name(), place->nameLen());
                }
            }
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Org_work);
                std::vector<std::tuple<uint64_t, int, snb::OrgSchema::Org*, snb::PlaceSchema::Place*>> orgs;
                while (nbrs.valid())
                {
                    int year = *(int*)nbrs.edge_data().data();
                    uint64_t vid = nbrs.dst_id();
                    auto org = (snb::OrgSchema::Org*)engine.get_vertex(vid).data();
                    auto place = (snb::PlaceSchema::Place*)engine.get_vertex(org->place).data();
                    orgs.emplace_back(org->id, year, org, place);
                    nbrs.next();
                }
                std::sort(orgs.begin(), orgs.end());
                for(auto t:orgs)
                {
                    int year = std::get<1>(t);
                    auto org = std::get<2>(t);
                    auto place = std::get<3>(t);
                    _return.back().friendCompanies_year.emplace_back(year);
                    _return.back().friendCompanies_name.emplace_back(org->name(), org->nameLen());
                    _return.back().friendCompanies_city.emplace_back(place->name(), place->nameLen());

                }
            }
        }

    }

    void query2(std::vector<Query2Response> & _return, const Query2Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
        std::map<std::pair<int64_t, uint64_t>, Query2Response> idx;

//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(friends[i]).data();
            uint64_t vid = friends[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    if(date <= request.maxDate)
                    {
                        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                        auto key = std::make_pair(-date, message->id);
                        auto value = Query2Response();
                        value.personId = person->id;
                        value.messageCreationDate = message->creationDate;
                        value.messageId = message->id;
//                        #pragma omp critical
                        {
                            if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                            {
                                value.personFirstName = std::string_view(person->firstName(), person->firstNameLen());
                                value.personLastName = std::string_view(person->lastName(), person->lastNameLen());
                                value.messageContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
                                idx.emplace(key, value);
                                while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                            }
                            else
                            {
                                flag = false;
                            }
                        }
                    }
                    nbrs.next();
                }
            }
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    if(date <= request.maxDate)
                    {
                        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                        auto key = std::make_pair(-date, message->id);
                        auto value = Query2Response();
                        value.personId = person->id;
                        value.messageCreationDate = message->creationDate;
                        value.messageId = message->id;
//                        #pragma omp critical
                        {
                            if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                            {
                                value.personFirstName = std::string_view(person->firstName(), person->firstNameLen());
                                value.personLastName = std::string_view(person->lastName(), person->lastNameLen());
                                value.messageContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
                                idx.emplace(key, value);
                                while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                            }
                            else
                            {
                                flag = false;
                            }
                        }
                    }
                    nbrs.next();
                }
            }
        }
        for(auto p : idx) _return.push_back(p.second);
//        std::cout << request.personId << " " << idx.size() << std::endl;

    }

    void query3(std::vector<Query3Response> & _return, const Query3Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        uint64_t countryX = placeSchema.findName(request.countryXName);
        uint64_t countryY = placeSchema.findName(request.countryYName);
        if(vid == (uint64_t)-1) return;
        if(countryX == (uint64_t)-1) return;
        if(countryY == (uint64_t)-1) return;
        uint64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
        std::vector<std::tuple<int, uint64_t, int, snb::PersonSchema::Person*>> idx;
//        std::map<std::pair<int, uint64_t>, std::pair<int, snb::PersonSchema::Person*>> idx;

//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(friends[i]).data();
            auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
            if(place->isPartOf != countryX && place->isPartOf != countryY)
            {
                uint64_t vid = friends[i];
                int xCount = 0, yCount = 0;
                {
                    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
                    while (nbrs.valid() && *(uint64_t*)nbrs.edge_data().data() >= (uint64_t)request.startDate)
                    {
                        uint64_t date = *(uint64_t*)nbrs.edge_data().data();
                        if(date < endDate)
                        {
                            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                            if(message->place == countryX) xCount++;
                            if(message->place == countryY) yCount++;
                        }
                        nbrs.next();
                    }

                }
                {
                    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                    while (nbrs.valid() && *(uint64_t*)nbrs.edge_data().data() >= (uint64_t)request.startDate)
                    {
                        uint64_t date = *(uint64_t*)nbrs.edge_data().data();
                        if(date < endDate)
                        {
                            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                            if(message->place == countryX) xCount++;
                            if(message->place == countryY) yCount++;
                        }
                        nbrs.next();
                    }

                }
//                auto key = std::make_pair(-xCount, person->id);
//                auto value = std::make_pair(-yCount, person);
//                #pragma omp critical
//                if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
//                {
//                    idx.emplace(key, value);
//                    while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
//                }
                if(xCount>0 && yCount>0) idx.push_back(std::make_tuple(-xCount, person->id, -yCount, person));
            }
        }
        std::sort(idx.begin(), idx.end());
//        tbb::parallel_sort(idx.begin(), idx.end());
//        std::cout << request.personId << " " << idx.size() << std::endl;

//        for(auto p : idx)
//        {
//            _return.emplace_back();
//            auto person = p.second.second;
//            _return.back().personId = p.first.second;
//            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
//            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
//            _return.back().xCount = -p.first.first;
//            _return.back().yCount = -p.second.first;
//            _return.back().count = _return.back().xCount + _return.back().yCount;
//            _return.emplace_back(_return.back());
//        }

        for(size_t i=0;i<std::min((size_t)request.limit, idx.size());i++)
        {
            _return.emplace_back();
            auto person = std::get<3>(idx[i]);
            _return.back().personId = std::get<1>(idx[i]);
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().xCount = -std::get<0>(idx[i]);
            _return.back().yCount = -std::get<2>(idx[i]);
            _return.back().count = _return.back().xCount + _return.back().yCount;
        }
    }

    void query4(std::vector<Query4Response> & _return, const Query4Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        uint64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
        std::unordered_map<uint64_t, std::pair<uint64_t, int>> idx;

//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
            while (nbrs.valid())
            {
                uint64_t date = *(uint64_t*)nbrs.edge_data().data();
                if(date < endDate)
                {
                    uint64_t vid = nbrs.dst_id();
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    {
                        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                        while (nbrs.valid())
                        {
                            auto iter = idx.find(nbrs.dst_id());
                            if(iter == idx.end())
                            {
                                idx.emplace(nbrs.dst_id(), std::make_pair(message->creationDate, 1));
                            }
                            else
                            {
                                iter->second.first = std::min(message->creationDate, iter->second.first);
                                iter->second.second++;
                            }
                            nbrs.next();
                        }
                    }
                }
                nbrs.next();
            }
        }
        std::set<std::pair<int, std::string_view>> idx_by_count;
        for(auto i=idx.begin();i!=idx.end();i++)
        {
            if(i->second.first >= (uint64_t)request.startDate && (idx_by_count.size() < (size_t)request.limit || idx_by_count.rbegin()->first >= -i->second.second))
            {
                auto tag = (snb::TagSchema::Tag*)engine.get_vertex(i->first).data();
                idx_by_count.emplace(-i->second.second, std::string_view(tag->name(), tag->nameLen()));
                while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(*idx_by_count.rbegin());
            }
        }
        for(auto p : idx_by_count)
        {
            _return.emplace_back();
            _return.back().postCount = -p.first;
            _return.back().tagName = p.second;
        }
//        std::cout << request.personId << " " << idx_by_count.size() << std::endl;

    }

    void query5(std::vector<Query5Response> & _return, const Query5Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
        std::unordered_map<uint64_t, int> idx;
//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Forum_member);
                while (nbrs.valid() && *(uint64_t*)nbrs.edge_data().data()>(uint64_t)request.minDate)
                {
                    uint64_t posts = *(uint64_t*)(nbrs.edge_data().data()+sizeof(uint64_t));
                    idx[nbrs.dst_id()] += posts;
                    nbrs.next();
                }
            }
        }
        std::map<std::pair<int, size_t>, std::string_view> idx_by_count;
        for(auto i=idx.begin();i!=idx.end();i++)
        {
            if(idx_by_count.size() < (size_t)request.limit || idx_by_count.rbegin()->first.first >= -i->second)
            {
                auto forum = (snb::ForumSchema::Forum*)engine.get_vertex(i->first).data();
                idx_by_count.emplace(std::make_pair(-i->second, forum->id), std::string_view(forum->title(), forum->titleLen()));
                while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(idx_by_count.rbegin()->first);
            }
        }
        for(auto p : idx_by_count)
        {
            _return.emplace_back();
            _return.back().postCount = -p.first.first;
            _return.back().forumTitle = p.second;
        }
//        std::cout << request.personId << " " << idx_by_count.size() << std::endl;
    }

    void query6(std::vector<Query6Response> & _return, const Query6Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        uint64_t tagId = tagSchema.findName(request.tagName);
        if(tagId == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
        friends.push_back(std::numeric_limits<uint64_t>::max());
        std::unordered_map<uint64_t, int> idx;
        {
            uint64_t vid = tagId;
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Tag2Post);
                while (nbrs.valid())
                {
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    if(*std::lower_bound(friends.begin(), friends.end(), message->creator) == message->creator)
                    {
                        uint64_t vid = nbrs.dst_id();
                        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                        while (nbrs.valid())
                        {
                            if(nbrs.dst_id() != tagId)
                            {
                                idx[nbrs.dst_id()]++;
                            }
                            nbrs.next();
                        }
                    }
                    nbrs.next();
                }
            }
        }
        std::set<std::pair<int, std::string_view>> idx_by_count;
        for(auto i=idx.begin();i!=idx.end();i++)
        {
            if(idx_by_count.size() < (size_t)request.limit || idx_by_count.rbegin()->first >= -i->second)
            {
                auto tag = (snb::TagSchema::Tag*)engine.get_vertex(i->first).data();
                idx_by_count.emplace(-i->second, std::string_view(tag->name(), tag->nameLen()));
                while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(*idx_by_count.rbegin());
            }
        }
        for(auto p : idx_by_count)
        {
            _return.emplace_back();
            _return.back().postCount = -p.first;
            _return.back().tagName = p.second;
        }
//        std::cout << request.personId << " " << idx_by_count.size() << std::endl;

    }

    void query7(std::vector<Query7Response> & _return, const Query7Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
        friends.push_back(std::numeric_limits<uint64_t>::max());
        auto posts = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Post_creator});
        auto comments = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Comment_creator});
        std::map<std::pair<int64_t, uint64_t>, std::tuple<uint64_t, uint64_t>> idx;
        std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>> person_hash;
//        #pragma omp parallel for
        for(size_t i=0;i<posts.size();i++)
        {
            uint64_t vid = posts[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Person_like);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
                    uint64_t person_id = person->id;
                    auto key = std::make_pair(-date, person_id);
                    auto value = std::make_tuple(nbrs.dst_id(), vid);
                    std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>>::iterator iter;
//                    #pragma omp critical
                    if((idx.size() < (size_t)request.limit || idx.rbegin()->first > key) && ((iter=person_hash.find(person_id)) == person_hash.end() || iter->second > key))
                    {
                        idx.emplace(key, value);
                        if(iter == person_hash.end())
                        {
                            person_hash.emplace(person_id, key);
                            while(idx.size() > (size_t)request.limit)
                            {
                                person_hash.erase(idx.rbegin()->first.second);
                                idx.erase(idx.rbegin()->first);
                            }
                        }
                        else
                        {
                            idx.erase(iter->second);
                            iter->second = key;
                        }
                        flag = true;
                    }
                    else
                    {
                        flag = idx.rbegin()->first.first > key.first;
                    }
                    nbrs.next();
                }
            }
        }

//        #pragma omp parallel for
        for(size_t i=0;i<comments.size();i++)
        {
            uint64_t vid = comments[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Comment2Person_like);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
                    uint64_t person_id = person->id;
                    auto key = std::make_pair(-date, person_id);
                    auto value = std::make_tuple(nbrs.dst_id(), vid);
                    std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>>::iterator iter;
//                    #pragma omp critical
                    if((idx.size() < (size_t)request.limit || idx.rbegin()->first > key) && ((iter=person_hash.find(person_id)) == person_hash.end() || iter->second > key))
                    {
                        idx.emplace(key, value);
                        if(iter == person_hash.end())
                        {
                            person_hash.emplace(person_id, key);
                            while(idx.size() > (size_t)request.limit)
                            {
                                person_hash.erase(idx.rbegin()->first.second);
                                idx.erase(idx.rbegin()->first);
                            }
                        }
                        else
                        {
                            idx.erase(iter->second);
                            iter->second = key;
                        }
                        flag = true;
                    }
                    else
                    {
                        flag = idx.rbegin()->first.first > key.first;
                    }
                    nbrs.next();
                }
            }
        }

        for(auto p : idx)
        {
            _return.emplace_back();
            uint64_t person_vid = std::get<0>(p.second);
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(person_vid).data();
            uint64_t message_vid = std::get<1>(p.second);
            uint64_t like_time = -p.first.first;
            auto message = (snb::MessageSchema::Message*)engine.get_vertex(message_vid).data();
            _return.back().personId = person->id;
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().likeCreationDate = like_time;
            _return.back().commentOrPostId = message->id;
            _return.back().commentOrPostContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
            _return.back().minutesLatency = (like_time-message->creationDate)/(60lu*1000lu);
            _return.back().isNew = (*std::lower_bound(friends.begin(), friends.end(), person_vid)) != person_vid;
        }
//        std::cout << request.personId << " " << idx.size() << std::endl;
    }

    void query8(std::vector<Query8Response> & _return, const Query8Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto posts = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Post_creator});
        auto comments = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Comment_creator});
        std::map<std::pair<int64_t, uint64_t>, snb::MessageSchema::Message*> idx;
//        #pragma omp parallel for
        for(size_t i=0;i<posts.size();i++)
        {
            uint64_t vid = posts[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Message2Message_down);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    auto key = std::make_pair(-date, message->id);
                    auto value = message;
//                    #pragma omp critical
                    if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                    {
                        idx.emplace(key, value);
                        while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                    }
                    else
                    {
                        flag = idx.rbegin()->first.first > key.first;
                    }
                    nbrs.next();
                }
            }
        }

//        #pragma omp parallel for
        for(size_t i=0;i<comments.size();i++)
        {
            uint64_t vid = comments[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Message2Message_down);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                    auto key = std::make_pair(-date, message->id);
                    auto value = message;
                    std::unordered_map<uint64_t, std::pair<int64_t, uint64_t>>::iterator iter;
//                    #pragma omp critical
                    if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                    {
                        idx.emplace(key, value);
                        while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                    }
                    else
                    {
                        flag = idx.rbegin()->first.first > key.first;
                    }
                    nbrs.next();
                }
            }
        }

        for(auto p : idx)
        {
            _return.emplace_back();
            auto message = p.second;
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
            _return.back().personId = person->id;
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().commentId = message->id;;
            _return.back().commentCreationDate = message->creationDate;
            _return.back().commentContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
        }
//        std::cout << request.personId << " " << idx.size() << std::endl;

    }

    void query9(std::vector<Query9Response> & _return, const Query9Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});

        std::map<std::pair<int64_t, uint64_t>, snb::MessageSchema::Message*> idx;
//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    if(date < request.maxDate)
                    {
                        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                        auto key = std::make_pair(-date, message->id);
                        auto value = message;
//                        #pragma omp critical
                        if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                        {
                            idx.emplace(key, value);
                            while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                        }
                        else
                        {
                            flag = idx.rbegin()->first.first > key.first;
                        }

                    }
                    nbrs.next();
                }
            }
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
                bool flag = true;
                while (nbrs.valid() && flag)
                {
                    int64_t date = *(uint64_t*)nbrs.edge_data().data();
                    if(date < request.maxDate)
                    {
                        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                        auto key = std::make_pair(-date, message->id);
                        auto value = message;
//                        #pragma omp critical
                        if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                        {
                            idx.emplace(key, value);
                            while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                        }
                        else
                        {
                            flag = idx.rbegin()->first.first > key.first;
                        }

                    }
                    nbrs.next();
                }
            }
        }
        for(auto p : idx)
        {
            _return.emplace_back();
            auto message = p.second;
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
            _return.back().personId = person->id;
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().messageId = message->id;;
            _return.back().messageCreationDate = message->creationDate;
            _return.back().messageContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
        }
//        std::cout << request.personId << " " << idx.size() << std::endl;

    }

    void query10(std::vector<Query10Response> & _return, const Query10Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;

        std::vector<size_t> frontier = {vid};
        std::vector<size_t> next_frontier;
        std::unordered_set<uint64_t> person_hash{vid};
        uint64_t root = vid;
        for (int k=0;k<2;k++)
        {
            next_frontier.clear();
            for (auto vid : frontier)
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Person);
                while (nbrs.valid())
                {
                    if(nbrs.dst_id() != root && person_hash.find(nbrs.dst_id()) == person_hash.end())
                    {
                        next_frontier.push_back(nbrs.dst_id());
                        person_hash.emplace(nbrs.dst_id());
                    }
                    nbrs.next();
                }
            }
            frontier.swap(next_frontier);
        }
        auto friends = frontier;
        std::sort(friends.begin(), friends.end());

        auto tags = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Tag});
        auto nextMonth = request.month%12 + 1;
        tags.push_back(std::numeric_limits<uint64_t>::max());
        std::map<std::pair<int, uint64_t>, snb::PersonSchema::Person*> idx;
//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            auto person = (snb::PersonSchema::Person*)(engine.get_vertex(vid)).data();
            std::pair<int, int> monday;
//            #pragma omp critical
            {
                monday = to_monday(person->birthday);
            }
            if((monday.first==request.month && monday.second>=21) || (monday.first==nextMonth && monday.second<22))
            {
                int commonInterestScore = 0;
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                while (nbrs.valid())
                {
                    bool flag = false;
                    uint64_t vid = nbrs.dst_id();
                    {
                        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                        while (nbrs.valid() && !flag)
                        {
                            uint64_t tag = nbrs.dst_id();
                            if(*std::lower_bound(tags.begin(), tags.end(), tag) == tag)
                            {
                                flag = true;
                            }
                            nbrs.next();
                        }
                    }
                    if(flag) commonInterestScore ++; else commonInterestScore --;
                    nbrs.next();
                }
                auto key = std::make_pair(-commonInterestScore, person->id);
                auto value = person;
//                #pragma omp critical
                if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                {
                    idx.emplace(key, value);
                    while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                }
            }
        }
        for(auto p : idx)
        {
            _return.emplace_back();
            auto person = p.second;
            auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
            _return.back().personId = person->id;
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().commonInterestSore = -p.first.first;
            _return.back().personGender = std::string_view(person->gender(), person->genderLen());
            _return.back().personCityName = std::string_view(place->name(), place->nameLen());
        }
//        std::cout << request.personId << " " << idx.size() << std::endl;
    }

    void query11(std::vector<Query11Response> & _return, const Query11Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        uint64_t country = placeSchema.findName(request.countryName);
        if(country == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
        friends.push_back(std::numeric_limits<uint64_t>::max());
        auto orgs = multihop(engine, country, 1, {(label_t)snb::EdgeSchema::Place2Org});

        std::map<std::tuple<int, int64_t, std::string_view>, uint64_t, std::greater<std::tuple<int, int64_t, std::string_view>>> idx;
//        #pragma omp parallel for
        for(size_t i=0;i<orgs.size();i++)
        {
            uint64_t vid = orgs[i];
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Org2Person_work);
            auto org = (snb::OrgSchema::Org*)engine.get_vertex(vid).data();
            while (nbrs.valid())
            {
                int32_t date = *(uint32_t*)nbrs.edge_data().data();
                if(date < request.workFromYear)
                {
                    auto person_vid = nbrs.dst_id();
                    if(*std::lower_bound(friends.begin(), friends.end(), person_vid) == person_vid)
                    {
                        auto person = (snb::PersonSchema::Person*)engine.get_vertex(person_vid).data();
                        uint64_t person_id = person->id;
                        auto key = std::make_tuple(-date, -(int64_t)person_id, std::string_view(org->name(), org->nameLen()));
                        auto value = person_vid;
//                        #pragma omp critical
                        if(idx.size() < (size_t)request.limit || idx.rbegin()->first < key)
                        {
                            idx.emplace(key, value);
                            while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                        }
                    }

                }
                nbrs.next();
            }
        }

        for(auto p : idx)
        {
            _return.emplace_back();
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(p.second).data();
            _return.back().personId = person->id;
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().organizationName = std::get<2>(p.first);
            _return.back().organizationWorkFromYear = -std::get<0>(p.first);
        }
//        std::cout << request.personId << " " << idx.size() << std::endl;
    }

    void query12(std::vector<Query12Response> & _return, const Query12Request& request)
    {
        _return.clear();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        uint64_t tagclassId = tagclassSchema.findName(request.tagClassName);
        if(tagclassId == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid).data() == nullptr) return;
        auto friends = multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
        auto tags = multihop_another_etype(engine, tagclassId, 65536, (label_t)snb::EdgeSchema::TagClass2TagClass_down, (label_t)snb::EdgeSchema::TagClass2Tag);
        tags.push_back(std::numeric_limits<uint64_t>::max());
        std::map<std::pair<int, uint64_t>, std::pair<uint64_t, std::vector<uint64_t>>> idx;

//        #pragma omp parallel for
        for(size_t i=0;i<friends.size();i++)
        {
            uint64_t vid = friends[i];
            int count = 0;
            std::set<uint64_t> tagSet;
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
            while (nbrs.valid())
            {
                bool flag = false;
                auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                uint64_t vid = message->replyOfPost;
                if(vid != (uint64_t)-1){
                    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
                    while (nbrs.valid())
                    {
                        uint64_t tag = nbrs.dst_id();
                        if(*std::lower_bound(tags.begin(), tags.end(), tag) == tag)
                        {
                            flag = true;
                            tagSet.emplace(tag);
                        }
                        nbrs.next();
                    }
                }
                if(flag) count++;
                nbrs.next();
            }
            if(count)
            {
                auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
                uint64_t person_id = person->id;
                auto key = std::make_pair(-count, person_id);
                std::vector<uint64_t> tagV;
                for(auto p: tagSet) tagV.push_back(p);
                auto value = std::make_pair(vid, tagV);
//                #pragma omp critical
                if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                {
                    idx.emplace(key, value);
                    while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                }

            }
        }

        for(auto p : idx)
        {
            _return.emplace_back();
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(p.second.first).data();
            _return.back().personId = person->id;
            _return.back().personFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string_view(person->lastName(), person->lastNameLen());
            for(auto vid: p.second.second)
            {
                auto tag = (snb::TagSchema::Tag*)engine.get_vertex(vid).data();
                _return.back().tagNames.emplace_back(tag->name(), tag->nameLen());
            }
            std::sort(_return.back().tagNames.begin(), _return.back().tagNames.end());
            _return.back().replyCount = -p.first.first;
        }
//        std::cout << request.personId << " " << idx.size() << std::endl;
    }

    void query13(Query13Response& _return, const Query13Request& request)
    {
        _return.shortestPathLength = -1;
        uint64_t vid1 = personSchema.findId(request.person1Id);
        uint64_t vid2 = personSchema.findId(request.person2Id);
        if(vid1 == (uint64_t)-1) return;
        if(vid2 == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid1).data() == nullptr) return;
        if(engine.get_vertex(vid2).data() == nullptr) return;
        _return.shortestPathLength = pairwiseShortestPath(engine, vid1, vid2, (label_t)snb::EdgeSchema::Person2Person);
    }

    void query14(std::vector<Query14Response> & _return, const Query14Request& request)
    {
        _return.clear();
        uint64_t vid1 = personSchema.findId(request.person1Id);
        uint64_t vid2 = personSchema.findId(request.person2Id);
        if(vid1 == (uint64_t)-1) return;
        if(vid2 == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        if(engine.get_vertex(vid1).data() == nullptr) return;
        if(engine.get_vertex(vid2).data() == nullptr) return;
        auto paths = pairwiseShortestPath_path(engine, vid1, vid2, (label_t)snb::EdgeSchema::Person2Person);
        std::sort(paths.begin(), paths.end(), [](const std::pair<double, std::vector<uint64_t>> &a, const std::pair<double, std::vector<uint64_t>> &b)
        {
            return a.first > b.first;
        });
        std::unordered_map<uint64_t, uint64_t> idx;
        for(auto p:paths)
        {
            _return.emplace_back();
            _return.back().pathWeight = p.first;
            for(auto vid:p.second)
            {
                auto iter = idx.find(vid);
                if(iter == idx.end()){
                    auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
                    iter = idx.emplace(vid, person->id).first;
                }
                _return.back().personIdsInPath.emplace_back(iter->second);
            }
        }

    }


    void shortQuery1(ShortQuery1Response& _return, const ShortQuery1Request& request)
    {
        _return = ShortQuery1Response();
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
        if(!person) return;
        _return.firstName = std::string_view(person->firstName(), person->firstNameLen());
        _return.lastName = std::string_view(person->lastName(), person->lastNameLen());
        _return.birthday = to_time(person->birthday);
        _return.locationIp = std::string_view(person->locationIP(), person->locationIPLen());
        _return.browserUsed = std::string_view(person->browserUsed(), person->browserUsedLen());
        auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
        _return.cityId = place->id;
        _return.gender = std::string_view(person->gender(), person->genderLen());
        _return.creationDate = person->creationDate;
    }

    void shortQuery2(std::vector<ShortQuery2Response> & _return, const ShortQuery2Request& request)
    {
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
        if(!person) return;
        
        std::map<std::pair<int64_t, int64_t>, snb::MessageSchema::Message*> idx;
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                auto key = std::make_pair(-date, -(int64_t)message->id);
                auto value = message;
                if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                {
                    idx.emplace(key, value);
                    while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                }
                else
                {
                    flag = idx.rbegin()->first.first > key.first;
                }
                nbrs.next();
            }
        }
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
            bool flag = true;
            while (nbrs.valid() && flag)
            {
                int64_t date = *(uint64_t*)nbrs.edge_data().data();
                auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                auto key = std::make_pair(-date, -(int64_t)message->id);
                auto value = message;
                if(idx.size() < (size_t)request.limit || idx.rbegin()->first > key)
                {
                    idx.emplace(key, value);
                    while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                }
                else
                {
                    flag = idx.rbegin()->first.first > key.first;
                }
                nbrs.next();
            }
        }
        for(auto p : idx)
        {
            _return.emplace_back();
            auto message = p.second;
            _return.back().messageId = message->id;
            _return.back().messageCreationDate = message->creationDate;
            _return.back().messageContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
            for(;message->replyOfComment != (uint64_t)-1;message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfComment).data());
            if(message->replyOfPost != (uint64_t)-1) message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfPost).data();
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
            _return.back().originalPostId = message->id;
            _return.back().originalPostAuthorId = person->id;
            _return.back().originalPostAuthorFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().originalPostAuthorLastName = std::string_view(person->lastName(), person->lastNameLen());
        }
    }

    void shortQuery3(std::vector<ShortQuery3Response> & _return, const ShortQuery3Request& request)
    {
        uint64_t vid = personSchema.findId(request.personId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
        if(!person) return;
        
        std::vector<std::tuple<int64_t, uint64_t, snb::PersonSchema::Person*>> idx;

        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Person);
        while (nbrs.valid())
        {
            int64_t date = *(uint64_t*)nbrs.edge_data().data();
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(nbrs.dst_id()).data();
            idx.emplace_back(-date, person->id, person);
            nbrs.next();
        }
        std::sort(idx.begin(), idx.end());
        for(auto p : idx)
        {
            _return.emplace_back();
            auto person = std::get<2>(p);
            _return.back().personId = person->id;
            _return.back().firstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().lastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().friendshipCreationDate = -std::get<0>(p);
        }
    }

    void shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request& request)
    {
        uint64_t vid = postSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
        if(!message) return;
        _return.messageCreationDate = message->creationDate;
        _return.messageContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
    }

    void shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request& request)
    {
        uint64_t vid = postSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
        if(!message) return;
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
        _return.personId = person->id;
        _return.firstName = std::string_view(person->firstName(), person->firstNameLen());
        _return.lastName = std::string_view(person->lastName(), person->lastNameLen());
    }

    void shortQuery6(ShortQuery6Response& _return, const ShortQuery6Request& request)
    {
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
        _return.forumTitle = std::string_view(forum->title(), forum->titleLen());
        _return.moderatorId = person->id;
        _return.moderatorFirstName = std::string_view(person->firstName(), person->firstNameLen());
        _return.moderatorLastName = std::string_view(person->lastName(), person->lastNameLen());
    }

    void shortQuery7(std::vector<ShortQuery7Response>& _return, const ShortQuery7Request& request)
    {
        uint64_t vid = postSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
        if(!message) return;
        auto person_vid = message->creator;

        auto friends = multihop(engine, person_vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
        friends.push_back(std::numeric_limits<uint64_t>::max());
        std::vector<std::tuple<int64_t, uint64_t, snb::MessageSchema::Message*, snb::PersonSchema::Person*>> idx;

        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Message2Message_down);
        while (nbrs.valid())
        {
            auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
            idx.emplace_back(-(int64_t)message->creationDate, person->id, message, person);
            nbrs.next();
        }

        for(auto p : idx)
        {
            _return.emplace_back();
            auto message = std::get<2>(p);
            auto person = std::get<3>(p);
            _return.back().commentId = message->id;
            _return.back().commentCreationDate = message->creationDate;
            _return.back().commentContent = message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen());
            _return.back().replyAuthorId = person->id;
            _return.back().replyAuthorFirstName = std::string_view(person->firstName(), person->firstNameLen());
            _return.back().replyAuthorLastName = std::string_view(person->lastName(), person->lastNameLen());
            _return.back().replyAuthorKnowsOriginalMassageAuthor = (*std::lower_bound(friends.begin(), friends.end(), message->creator) == message->creator);
        }
    }

    void update1(const interactive::Update1Request& request)
    {
        uint64_t vid = personSchema.findId(request.personId);
        if(vid != (uint64_t)-1) return;
        uint64_t place_vid = placeSchema.findId(request.cityId);
        auto birthday = std::chrono::system_clock::time_point(std::chrono::milliseconds(request.birthday));
        auto person_buf = snb::PersonSchema::createPerson(request.personId, request.personFirstName, request.personLastName, request.gender,  
                std::chrono::duration_cast<std::chrono::hours>(birthday.time_since_epoch()).count(), 
                request.emails, request.languages, request.browserUsed, request.locationIp,
                request.creationDate, place_vid);
        std::vector<uint64_t> tag_vid, study_vid, work_vid;
        for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
        for(auto org:request.studyAt_id) study_vid.emplace_back(orgSchema.findId(org));
        for(auto org:request.workAt_id) work_vid.emplace_back(orgSchema.findId(org));
        while(true)
        {
            auto txn = graph->begin_transaction();
            try
            {
                vid = txn.new_vertex();
                personSchema.insertId(request.personId, vid);
                txn.put_vertex(vid, std::string_view(person_buf.data(), person_buf.size()));
                txn.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Person, vid, std::string_view());
                for(auto tag:tag_vid)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Tag, tag, std::string_view());
                    txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Person, vid, std::string_view());
                }
                for(size_t i=0;i<study_vid.size();i++)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Org_study, study_vid[i], std::string_view((char*)&request.studyAt_year[i], sizeof(request.studyAt_year[i])));
                    txn.put_edge(study_vid[i], (label_t)snb::EdgeSchema::Org2Person_study, study_vid[i], std::string_view((char*)&request.studyAt_year[i], sizeof(request.studyAt_year[i])));
                }
                for(size_t i=0;i<work_vid.size();i++)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Org_work, work_vid[i], std::string_view((char*)&request.workAt_year[i], sizeof(request.workAt_year[i])));
                    txn.put_edge(work_vid[i], (label_t)snb::EdgeSchema::Org2Person_work, work_vid[i], std::string_view((char*)&request.workAt_year[i], sizeof(request.workAt_year[i])));
                }
                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update2(const Update2Request& request)
    {
        uint64_t person_vid = personSchema.findId(request.personId);
        uint64_t post_vid = postSchema.findId(request.postId);
        if(person_vid == (uint64_t)-1) return;
        if(post_vid == (uint64_t)-1) return;
        while(true)
        {
            auto txn = graph->begin_transaction();
            if(!txn.get_vertex(person_vid).data()) return;
            if(!txn.get_vertex(post_vid).data()) return;
            try
            {
                txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_like, post_vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));
                txn.put_edge(post_vid, (label_t)snb::EdgeSchema::Post2Person_like, person_vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update3(const Update3Request& request)
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
                txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_like, comment_vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));
                txn.put_edge(comment_vid, (label_t)snb::EdgeSchema::Comment2Person_like, person_vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update4(const Update4Request& request)
    {
        uint64_t vid = forumSchema.findId(request.forumId);
        if(vid != (uint64_t)-1) return;
        uint64_t moderator_vid = personSchema.findId(request.moderatorPersonId);
        if(moderator_vid == (uint64_t)-1) return;
        auto forum_buf = snb::ForumSchema::createForum(request.forumId, request.forumTitle, request.creationDate, moderator_vid);
        std::vector<uint64_t> tag_vid;
        for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
        while(true)
        {
            auto txn = graph->begin_transaction();
            if(!txn.get_vertex(moderator_vid).data()) return;
            try
            {
                vid = txn.new_vertex();
                forumSchema.insertId(request.forumId, vid);
                txn.put_vertex(vid, std::string_view(forum_buf.data(), forum_buf.size()));
                txn.put_edge(moderator_vid, (label_t)snb::EdgeSchema::Person2Forum_moderator, vid, std::string_view());
                for(auto tag:tag_vid)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Forum2Tag, tag, std::string_view());
                    txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Forum, vid, std::string_view());
                }

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update5(const Update5Request& request)
    {
        uint64_t person_vid = personSchema.findId(request.personId);
        uint64_t forum_vid = forumSchema.findId(request.forumId);
        if(person_vid == (uint64_t)-1) return;
        if(forum_vid == (uint64_t)-1) return;
        while(true)
        {
            auto txn = graph->begin_transaction();
            if(!txn.get_vertex(person_vid).data()) return;
            if(!txn.get_vertex(forum_vid).data()) return;
            try
            {
                auto nbrs = txn.get_edges(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                uint64_t posts = 0;
                while (nbrs.valid())
                {
                    auto message = (snb::MessageSchema::Message*)txn.get_vertex(nbrs.dst_id()).data();
                    if(message->forumid == forum_vid) posts++;
                    nbrs.next();
                }

                snb::Buffer buf(sizeof(uint64_t)+sizeof(uint64_t));
                *(uint64_t*)buf.data() = request.joinDate;
                *(uint64_t*)(buf.data()+sizeof(uint64_t)) = posts;

                txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Forum_member, forum_vid, std::string_view(buf.data(), buf.size()));
                txn.put_edge(forum_vid, (label_t)snb::EdgeSchema::Forum2Person_member, person_vid, std::string_view(buf.data(), buf.size()));

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update6(const Update6Request& request)
    {
        uint64_t vid = postSchema.findId(request.postId);
        if(vid != (uint64_t)-1) return;
        uint64_t person_vid = personSchema.findId(request.authorPersonId);
        uint64_t forum_vid = forumSchema.findId(request.forumId);
        uint64_t place_vid = placeSchema.findId(request.countryId);
        if(person_vid == (uint64_t)-1) return;
        if(forum_vid == (uint64_t)-1) return;
        if(place_vid == (uint64_t)-1) return;
        std::vector<uint64_t> tag_vid;
        for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
        auto message_buf = snb::MessageSchema::createMessage(request.postId, request.imageFile, 
                request.creationDate, request.locationIp, request.browserUsed, 
                request.language, request.content, person_vid, forum_vid, place_vid, 
                (uint64_t)-1, (uint64_t)-1, snb::MessageSchema::Message::Type::Post);
        while(true)
        {
            auto txn = graph->begin_transaction();
            if(!txn.get_vertex(person_vid).data()) return;
            if(!txn.get_vertex(forum_vid).data()) return;
            try
            {
                vid = txn.new_vertex();
                postSchema.insertId(request.postId, vid);
                txn.put_vertex(vid, std::string_view(message_buf.data(), message_buf.size()));

                {
                    std::vector<std::pair<uint64_t, uint64_t>> edges;
                    auto nbrs = txn.get_edges(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                    while (nbrs.valid())
                    {
                        uint64_t date = *(uint64_t*)(nbrs.edge_data().data());
                        if(date <= (uint64_t)request.creationDate) break;
                        edges.emplace_back(nbrs.dst_id(), date);
                        nbrs.next();
                    }
                    for(auto p:edges) txn.del_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator, p.first);
                    txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator, vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));
                    std::reverse(edges.begin(), edges.end());
                    for(auto p:edges) txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_creator, p.first, std::string_view((char*)&p.second, sizeof(p.second)));
                }

                {
                    auto bytes = txn.get_edge(person_vid, (label_t)snb::EdgeSchema::Person2Forum_member, forum_vid);
                    if(bytes.data())
                    {
                        std::string_view buf(bytes);
                        *(uint64_t*)(buf.data()+sizeof(uint64_t)) += 1;
                        txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Forum_member, forum_vid, std::string_view(buf.data(), buf.size()));
                        txn.put_edge(forum_vid, (label_t)snb::EdgeSchema::Forum2Person_member, person_vid, std::string_view(buf.data(), buf.size()));
                    }
                }

                txn.put_edge(forum_vid, (label_t)snb::EdgeSchema::Forum2Post, vid, std::string_view());

                for(auto tag:tag_vid)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Post2Tag, tag, std::string_view());
                    txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Post, vid, std::string_view());
                }

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update7(const Update7Request& request)
    {
        uint64_t vid = commentSchema.findId(request.commentId);
        if(vid != (uint64_t)-1) return;
        uint64_t person_vid = personSchema.findId(request.authorPersonId);
        uint64_t place_vid = placeSchema.findId(request.countryId);
        uint64_t post_vid = postSchema.findId(request.replyToPostId);
        uint64_t comment_vid = commentSchema.findId(request.replyToCommentId);
        if(person_vid == (uint64_t)-1) return;
        if(place_vid == (uint64_t)-1) return;
        if(post_vid == (uint64_t)-1 && comment_vid == (uint64_t)-1) return;
        uint64_t message_vid = (post_vid != (uint64_t)-1) ? post_vid : comment_vid;
        std::vector<uint64_t> tag_vid;
        for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
        auto message_buf = snb::MessageSchema::createMessage(request.commentId, std::string(), 
                request.creationDate, request.locationIp, request.browserUsed, 
                std::string(), request.content, person_vid, (uint64_t)-1, place_vid, 
                post_vid, comment_vid, snb::MessageSchema::Message::Type::Comment);
        while(true)
        {
            auto txn = graph->begin_transaction();
            if(!txn.get_vertex(person_vid).data()) return;
            if(!txn.get_vertex(message_vid).data()) return;
            try
            {
                vid = txn.new_vertex();
                commentSchema.insertId(request.commentId, vid);
                txn.put_vertex(vid, std::string_view(message_buf.data(), message_buf.size()));

                {
                    std::vector<std::pair<uint64_t, uint64_t>> edges;
                    auto nbrs = txn.get_edges(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
                    while (nbrs.valid())
                    {
                        uint64_t date = *(uint64_t*)(nbrs.edge_data().data());
                        if(date <= (uint64_t)request.creationDate) break;
                        edges.emplace_back(nbrs.dst_id(), date);
                        nbrs.next();
                    }
                    for(auto p:edges) txn.del_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, p.first);
                    txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));
                    std::reverse(edges.begin(), edges.end());
                    for(auto p:edges) txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Comment_creator, p.first, std::string_view((char*)&p.second, sizeof(p.second)));
                }

                {
                    std::vector<std::pair<uint64_t, uint64_t>> edges;
                    auto nbrs = txn.get_edges(message_vid, (label_t)snb::EdgeSchema::Message2Message_down);
                    while (nbrs.valid())
                    {
                        uint64_t date = *(uint64_t*)(nbrs.edge_data().data());
                        if(date <= (uint64_t)request.creationDate) break;
                        edges.emplace_back(nbrs.dst_id(), date);
                        nbrs.next();
                    }
                    for(auto p:edges) txn.del_edge(message_vid, (label_t)snb::EdgeSchema::Message2Message_down, p.first);
                    txn.put_edge(message_vid, (label_t)snb::EdgeSchema::Message2Message_down, vid, std::string_view((char*)&request.creationDate, sizeof(request.creationDate)));
                    std::reverse(edges.begin(), edges.end());
                    for(auto p:edges) txn.put_edge(message_vid, (label_t)snb::EdgeSchema::Message2Message_down, p.first, std::string_view((char*)&p.second, sizeof(p.second)));
                }

                {
                    auto friend_vid = ((snb::MessageSchema::Message*)txn.get_vertex(message_vid).data())->creator;
                    auto bytes = txn.get_edge(person_vid, (label_t)snb::EdgeSchema::Person2Person, friend_vid);
                    if(bytes.data())
                    {
                        std::string_view buf(bytes);
                        *(double*)(buf.data()+sizeof(uint64_t)) += (post_vid == message_vid)?1.0:0.5;
                        txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Person, friend_vid, std::string_view(buf.data(), buf.size()));
                        txn.put_edge(friend_vid, (label_t)snb::EdgeSchema::Person2Person, person_vid, std::string_view(buf.data(), buf.size()));
                    }
                }

                for(auto tag:tag_vid)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Comment2Tag, tag, std::string_view());
                    txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Comment, vid, std::string_view());
                }

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

    void update8(const Update8Request& request)
    {
        uint64_t left = personSchema.findId(request.person1Id);
        uint64_t right = personSchema.findId(request.person2Id);
        if(left == (uint64_t)-1) return;
        if(right == (uint64_t)-1) return;
        while(true)
        {
            auto txn = graph->begin_transaction();
            if(!txn.get_vertex(left).data()) return;
            if(!txn.get_vertex(right).data()) return;
            try
            {
                double weight = 0;
                {
                    auto nbrs = txn.get_edges(left, (label_t)snb::EdgeSchema::Person2Comment_creator);
                    while (nbrs.valid())
                    {
                        auto comment = (snb::MessageSchema::Message*)txn.get_vertex(nbrs.dst_id()).data();
                        auto rvid = comment->replyOfPost==(uint64_t)-1?comment->replyOfComment:comment->replyOfPost;
                        auto reply = (snb::MessageSchema::Message*)txn.get_vertex(rvid).data();
                        if(reply->creator == right) weight += (rvid == comment->replyOfPost)?1.0:0.5;
                        nbrs.next();
                    }

                }
                {
                    auto nbrs = txn.get_edges(right, (label_t)snb::EdgeSchema::Person2Comment_creator);
                    while (nbrs.valid())
                    {
                        auto comment = (snb::MessageSchema::Message*)txn.get_vertex(nbrs.dst_id()).data();
                        auto rvid = comment->replyOfPost==(uint64_t)-1?comment->replyOfComment:comment->replyOfPost;
                        auto reply = (snb::MessageSchema::Message*)txn.get_vertex(rvid).data();
                        if(reply->creator == left) weight += (rvid == comment->replyOfPost)?1.0:0.5;
                        nbrs.next();
                    }
                }
                snb::Buffer buf(sizeof(uint64_t)+sizeof(double));
                *(uint64_t*)buf.data() = request.creationDate;
                *(double*)(buf.data()+sizeof(uint64_t)) = weight;

                txn.put_edge(left, (label_t)snb::EdgeSchema::Person2Person, right, std::string_view(buf.data(), buf.size()));
                txn.put_edge(right, (label_t)snb::EdgeSchema::Person2Person, left, std::string_view(buf.data(), buf.size()));

                txn.commit();
                break;
            } 
            catch (Transaction::RollbackExcept &e) 
            {
                txn.abort();
            }
        }
    }

};

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
        return new InteractiveHandler;
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

    graph = new Graph(graphPath+"/graph.mmap", graphPath+"graph.wal");

    rocksdb::DB *rocksdb_db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.table_cache_numshardbits = 10;
    options.compression = rocksdb::kNoCompression;
    options.bottommost_compression = rocksdb::kNoCompression;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.index_type = rocksdb::BlockBasedTableOptions::kHashSearch;
    table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinarySearch;
    table_options.block_cache = rocksdb::NewLRUCache(4096ul * 1024 * 1024);
    rocksdb::ColumnFamilyOptions id_options, name_options;
    name_options.prefix_extractor.reset(rocksdb::NewNoopTransform());
    name_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    name_options.compression = rocksdb::kNoCompression;
    name_options.bottommost_compression = rocksdb::kNoCompression;
    id_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(uint64_t)));
    id_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    id_options.compression = rocksdb::kNoCompression;
    id_options.bottommost_compression = rocksdb::kNoCompression;

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
