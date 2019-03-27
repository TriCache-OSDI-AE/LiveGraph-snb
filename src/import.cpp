#include <ctime>
#include <chrono>
#include <vector>
#include <cassert>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <iostream>

#include "core/import.h"

namespace snb
{
    std::vector<std::string> static inline split(const std::string &s, char delim) {
        std::stringstream ss(s);
        std::string item;
        std::vector<std::string> elems;
        while (std::getline(ss, item, delim)) {
            elems.push_back(std::move(item)); // if C++11 (based on comment from @mchiasson)
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
        for(std::string line;std::getline(ifs, line);first = false)
        {
            if(first)
            {
                assert(line.substr(0, 3) == "id|");
                continue;
            }
            auto v = split(path, csv_split);
        }

    }

    void importPerson(snb::PersonSchema &personSchema, std::string path)
    {
        std::ifstream ifs_person(path+personPathSuffix);
        std::ifstream ifs_emails(path+"/person_email_emailaddress_0_0.csv");
        std::ifstream ifs_speaks(path+"/person_speaks_language_0_0.csv");
    
        std::vector<std::string> all_persons, all_emails, all_speaks;
        for(std::string line;std::getline(ifs_person, line);) all_persons.push_back(line);
        for(std::string line;std::getline(ifs_emails, line);) all_emails.push_back(line);
        for(std::string line;std::getline(ifs_speaks, line);) all_speaks.push_back(line);
        assert(all_persons[0] == "id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|place");
        assert(all_emails[0] == "Person.id|email");
        assert(all_speaks[0] == "Person.id|language");
    
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
    
            auto person_buf = snb::PersonSchema::createPerson(std::stoull(person_v[0]), person_v[1], person_v[2], person_v[3],  
                    std::chrono::duration_cast<std::chrono::hours>(birthday.time_since_epoch()).count(), emails_v, speaks_v, person_v[7], person_v[6],
                    std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(), 
                    std::stoull(person_v[8]));
    
            const snb::PersonSchema::Person *person = (const snb::PersonSchema::Person*)person_buf.data();
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
        std::ifstream ifs_place(path+placePathSuffix);
    
        std::vector<std::string> all_places;
        for(std::string line;std::getline(ifs_place, line);) all_places.push_back(line);
        assert(all_places[0] == "id|name|url|type|isPartOf");
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
            auto place_buf = snb::PlaceSchema::createPlace(std::stoull(place_v[0]), place_v[1], place_v[2], type, place_v.size()>4?std::stoull(place_v[4]):(uint64_t)-1);
            const snb::PlaceSchema::Place *place = (const snb::PlaceSchema::Place*)place_buf.data();
    //        std::cout << place->id << "|" << std::string(place->name(), place->nameLen()) << "|" 
    //            << std::string(place->url(), place->urlLen()) << "|"  << (int)place->type << "|" << place->isPartOf << std::endl;
    
        }
    }
    
    void importOrg(snb::OrgSchema &orgSchema, std::string path)
    {
        std::ifstream ifs_org(path+orgPathSuffix);
    
        std::vector<std::string> all_orgs;
        for(std::string line;std::getline(ifs_org, line);) all_orgs.push_back(line);
        assert(all_orgs[0] == "id|type|name|url|place");
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
            auto org_buf = snb::OrgSchema::createOrg(std::stoull(org_v[0]), type, org_v[2], org_v[3], std::stoull(org_v[4]));
            const snb::OrgSchema::Org *org = (const snb::OrgSchema::Org*)org_buf.data();
    //        std::cout << org->id << "|" << std::string(org->name(), org->nameLen()) << "|" 
    //            << std::string(org->url(), org->urlLen()) << "|"  << (int)org->type << "|" << org->place << std::endl;
    
        }
    }
    
    void importPost(snb::MessageSchema &postSchema, std::string path)
    {
        std::ifstream ifs_message(path+postPathSuffix);
    
        std::vector<std::string> all_messages;
        for(std::string line;std::getline(ifs_message, line);) all_messages.push_back(line);
        assert(all_messages[0] == "id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place");
        std::vector<std::vector<std::string>> message_vs;
        for(size_t i=1;i<all_messages.size();i++)
        {
            message_vs.emplace_back(split(all_messages[i], csv_split));
        }
        std::sort(message_vs.begin(), message_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
            return a[2] < b[2];
        });
        for(auto &message_v : message_vs)
        {
            snb::MessageSchema::Message::Type type = snb::MessageSchema::Message::Type::Post;
            assert(std::stoull(message_v[7]) <= message_v[6].length());
            std::chrono::system_clock::time_point creationDate = from_time(message_v[2]);
    
            auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), message_v[1], 
                    std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                    message_v[3], message_v[4], message_v[5], message_v[6], 
                    std::stoull(message_v[8]), std::stoull(message_v[9]), std::stoull(message_v[10]), (uint64_t)-1, (uint64_t)-1, type);
            const snb::MessageSchema::Message *message = (const snb::MessageSchema::Message*)message_buf.data();
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
    
    void importComment(snb::MessageSchema &commentSchema, std::string path)
    {
        std::ifstream ifs_message(path+commentPathSuffix);
    
        std::vector<std::string> all_messages;
        for(std::string line;std::getline(ifs_message, line);) all_messages.push_back(line);
        assert(all_messages[0] == "id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment");
        std::vector<std::vector<std::string>> message_vs;
        for(size_t i=1;i<all_messages.size();i++)
        {
            message_vs.emplace_back(split(all_messages[i], csv_split));
        }
        std::sort(message_vs.begin(), message_vs.end(), [](const std::vector<std::string>&a, const std::vector<std::string>&b){
            return a[1] < b[1];
        });
        for(auto &message_v : message_vs)
        {
            snb::MessageSchema::Message::Type type = snb::MessageSchema::Message::Type::Comment;
            assert(std::stoull(message_v[5]) <= message_v[4].length());
            std::chrono::system_clock::time_point creationDate = from_time(message_v[1]);
    
            auto message_buf = snb::MessageSchema::createMessage(std::stoull(message_v[0]), std::string(), 
                    std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                    message_v[2], message_v[3], std::string(), message_v[4], 
                    std::stoull(message_v[6]), (uint64_t)-1, std::stoull(message_v[7]), !message_v[8].empty()?std::stoull(message_v[8]):(uint64_t)-1, message_v.size()>9?std::stoull(message_v[9]):(uint64_t)-1, type);
            const snb::MessageSchema::Message *message = (const snb::MessageSchema::Message*)message_buf.data();
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

    void importTag(snb::TagSchema &tagSchema, std::string path)
    {
        std::ifstream ifs_tag(path+tagPathSuffix);
    
        std::vector<std::string> all_tags;
        for(std::string line;std::getline(ifs_tag, line);) all_tags.push_back(line);
        assert(all_tags[0] == "id|name|url|hasType");
        for(size_t i=1;i<all_tags.size();i++)
        {
            std::vector<std::string> tag_v = split(all_tags[i], csv_split);
            auto tag_buf = snb::TagSchema::createTag(std::stoull(tag_v[0]), tag_v[1], tag_v[2], std::stoull(tag_v[3]));
            const snb::TagSchema::Tag *tag = (const snb::TagSchema::Tag*)tag_buf.data();
//            std::cout << tag->id << "|" << std::string(tag->name(), tag->nameLen()) << "|" 
//                << std::string(tag->url(), tag->urlLen()) << "|"  << tag->hasType << std::endl;
    
        }
    }

    void importTagClass(snb::TagClassSchema &tagclassSchema, std::string path)
    {
        std::ifstream ifs_tagclass(path+tagclassPathSuffix);
    
        std::vector<std::string> all_tagclasss;
        for(std::string line;std::getline(ifs_tagclass, line);) all_tagclasss.push_back(line);
        assert(all_tagclasss[0] == "id|name|url|isSubclassOf");
        for(size_t i=1;i<all_tagclasss.size();i++)
        {
            std::vector<std::string> tagclass_v = split(all_tagclasss[i], csv_split);
            auto tagclass_buf = snb::TagClassSchema::createTagClass(std::stoull(tagclass_v[0]), tagclass_v[1], tagclass_v[2], tagclass_v.size()>3?std::stoull(tagclass_v[3]):(uint64_t)-1);
            const snb::TagClassSchema::TagClass *tagclass = (const snb::TagClassSchema::TagClass*)tagclass_buf.data();
//            std::cout << tagclass->id << "|" << std::string(tagclass->name(), tagclass->nameLen()) << "|" 
//                << std::string(tagclass->url(), tagclass->urlLen()) << "|"  << tagclass->isSubclassOf << std::endl;
    
        }
    }

    void importForum(snb::ForumSchema &forumSchema, std::string path)
    {
        std::ifstream ifs_forum(path+forumPathSuffix);
    
        std::vector<std::string> all_forums;
        for(std::string line;std::getline(ifs_forum, line);) all_forums.push_back(line);
        assert(all_forums[0] == "id|title|creationDate|moderator");
        for(size_t i=1;i<all_forums.size();i++)
        {
            std::vector<std::string> forum_v = split(all_forums[i], csv_split);
            std::chrono::system_clock::time_point creationDate = from_time(forum_v[2]);
            auto forum_buf = snb::ForumSchema::createForum(std::stoull(forum_v[0]), forum_v[1],
                    std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count(),
                    std::stoull(forum_v[3]));
            const snb::ForumSchema::Forum *forum = (const snb::ForumSchema::Forum*)forum_buf.data();
//            std::cout << forum->id << "|" << std::string(forum->title(), forum->titleLen()) << "|" 
//                << forum->moderator << "|";
//            std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(forum->creationDate)));
//            std::cout << std::ctime(&t);
    
        }
    }

}

