#include <ctime>
#include <thread>
#include <chrono>
#include <vector>
#include <cassert>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tbb/parallel_sort.h>
#include "core/schema.h" 
#include "core/import.h"
#include "LiveGraph/core/graph.hpp"

livegraph::Graph *graph = nullptr;

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

        auto place_buf = snb::PlaceSchema::createPlace(std::stoull(place_v[0]), place_v[1], place_v[2], type, isPartOf_vid);

        loader.PutVertex(place_vid, livegraph::Bytes(place_buf.data(), place_buf.size()));

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

        auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), message_v[1], 
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                message_v[3], message_v[4], message_v[5], message_v[6], 
                creator_vid, forumid_vid, place_vid, (uint64_t)-1, (uint64_t)-1, type);

        loader.PutVertex(message_vid, livegraph::Bytes(message_buf.data(), message_buf.size()));

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

        auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), std::string(), 
                std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                message_v[2], message_v[3], std::string(), message_v[4], 
                creator_vid, (uint64_t)-1, place_vid, replyOfPost_vid, replyOfComment_vid, type);

        loader.PutVertex(message_vid, livegraph::Bytes(message_buf.data(), message_buf.size()));

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

        auto tag_buf = snb::TagSchema::createTag(std::stoull(tag_v[0]), tag_v[1], tag_v[2], hasType_vid);

        loader.PutVertex(tag_vid, livegraph::Bytes(tag_buf.data(), tag_buf.size()));

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

        auto tagclass_buf = snb::TagClassSchema::createTagClass(std::stoull(tagclass_v[0]), tagclass_v[1], tagclass_v[2], isSubclassOf_vid);

        loader.PutVertex(tagclass_vid, livegraph::Bytes(tagclass_buf.data(), tagclass_buf.size()));

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

//        const snb::ForumSchema::Forum *forum = (const snb::ForumSchema::Forum*)forum_buf.data();
//        std::cout << forum->id << "|" << std::string(forum->title(), forum->titleLen()) << "|" 
//            << forum->moderator << "|";
//        std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(forum->creationDate)));
//        std::cout << std::ctime(&t);

    }
}

int main(int argc, char** argv)
{
    std::string graphPath = argv[1];
    std::string dataPath = argv[2];

    graph = new livegraph::Graph(graphPath);

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

    std::cerr << "Finish prepareVIndex" << std::endl;

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
        for(auto &t:pool) t.join();
    }

    std::cerr << "Finish importVertex" << std::endl;

    delete graph;
    return 0;
}
