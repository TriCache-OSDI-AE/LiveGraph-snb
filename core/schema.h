#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <cassert>
#include <tbb/concurrent_hash_map.h>

namespace snb
{
    typedef std::vector<char> Buffer;
    class Schema
    {
    private:
        tbb::concurrent_hash_map<uint64_t, uint64_t> vindex;
    public:
        void insertId(uint64_t id, uint64_t vid)
        {   
            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor a;
            vindex.insert(a, id);
            a->second = vid;
        }

        uint64_t findId(uint64_t id)
        {
            tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor a;
            bool exist = vindex.find(a, id);
            assert(exist);
            return a->second;
        }
    };
    class PersonSchema : public Schema
    {
    public:
        struct Person
        {
            uint64_t id;
            uint64_t place;
            uint64_t creationDate;
            uint32_t birthday;
            uint16_t lastName_offset;
            uint16_t gender_offset;
            uint16_t emails_offset;
            uint16_t speaks_offset;
            uint16_t browserUsed_offset;
            uint16_t locationIP_offset;
            uint16_t length;
            char data[0];

            const char* firstName() const
            {
                return data;
            }
            uint16_t firstNameLen() const
            {
                return lastName_offset;
            }
            const char* lastName() const
            {
                return data+lastName_offset;
            }
            uint16_t lastNameLen() const
            {
                return gender_offset-lastName_offset;
            }
            const char* gender() const
            {
                return data+gender_offset;
            }
            uint16_t genderLen() const
            {
                return emails_offset-gender_offset;
            }
            const char* emails() const
            {
                return data+emails_offset;
            }
            uint16_t emailsLen() const
            {
                return speaks_offset-emails_offset;
            }
            const char* speaks() const
            {
                return data+speaks_offset;
            }
            uint16_t speaksLen() const
            {
                return browserUsed_offset-speaks_offset;
            }
            const char* browserUsed() const
            {
                return data+browserUsed_offset;
            }
            uint16_t browserUsedLen() const
            {
                return locationIP_offset-browserUsed_offset;
            }
            const char* locationIP() const
            {
                return data+locationIP_offset;
            }
            uint16_t locationIPLen() const
            {
                return length-locationIP_offset;
            }
        } __attribute__((packed));

        Buffer static createPerson(uint64_t id, std::string firstName, std::string lastName, std::string gender, uint32_t birthday, std::vector<std::string> emails, std::vector<std::string> speaks, std::string browserUsed, std::string locationIP, uint64_t creationDate, uint64_t place)
        {
            size_t size = sizeof(Person);
            size += firstName.length();
            size += lastName.length();
            size += gender.length();
            for(auto email:emails) size += email.length()+1;
            for(auto speak:speaks) size += speak.length()+1;
            size += browserUsed.length();
            size += locationIP.length();

            Buffer buf(size);
            Person *person = (Person *)buf.data();
            person->id = id;
            person->place = place;
            person->creationDate = creationDate;
            person->birthday = birthday;

            uint16_t offset = 0;
            memcpy(person->data+offset, firstName.c_str(), firstName.length());
            offset += firstName.length(); person->lastName_offset = offset;

            memcpy(person->data+offset, lastName.c_str(), lastName.length());
            offset += lastName.length(); person->gender_offset = offset;

            memcpy(person->data+offset, gender.c_str(), gender.length());
            offset += gender.length(); person->emails_offset = offset;

            for(auto email:emails)
            {
                memcpy(person->data+offset, email.c_str(), email.length());
                offset += email.length(); 
                person->data[offset++] = '\000';
            }
            person->speaks_offset = offset;

            for(auto speak:speaks)
            {
                memcpy(person->data+offset, speak.c_str(), speak.length());
                offset += speak.length(); 
                person->data[offset++] = '\000';
            }
            person->browserUsed_offset = offset;

            memcpy(person->data+offset, browserUsed.c_str(), browserUsed.length());
            offset += browserUsed.length(); person->locationIP_offset = offset;

            memcpy(person->data+offset, locationIP.c_str(), locationIP.length());
            offset += locationIP.length(); person->length = offset;

            assert(person->length + sizeof(Person) == size);
            return buf;
        }

    };

    class PlaceSchema : public Schema
    {
    public:
        struct Place
        {
            uint64_t id;
            uint64_t isPartOf;
            uint16_t url_offset;
            uint16_t length;
            enum class Type : char
            {
                City,
                Country,
                Continent
            } type;
            char data[0];

            const char* name() const
            {
                return data;
            }
            uint16_t nameLen() const
            {
                return url_offset;
            }
            const char* url() const
            {
                return data+url_offset;
            }
            uint16_t urlLen() const
            {
                return length-url_offset;
            }
        } __attribute__((packed));

        Buffer static createPlace(uint64_t id, std::string name, std::string url, Place::Type type, uint64_t isPartOf)
        {
            size_t size = sizeof(Place);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            Place *place = (Place *)buf.data();
            place->id = id;
            place->isPartOf = isPartOf;
            place->type = type;

            uint16_t offset = 0;
            memcpy(place->data+offset, name.c_str(), name.length());
            offset += name.length(); place->url_offset = offset;

            memcpy(place->data+offset, url.c_str(), url.length());
            offset += url.length(); place->length = offset;

            assert(place->length + sizeof(Place) == size);
            return buf;
        }

    };

    class OrgSchema : public Schema
    {
    public:
        struct Org
        {
            uint64_t id;
            uint64_t place;
            uint16_t url_offset;
            uint16_t length;
            enum class Type : char
            {
                Company,
                University
            } type;
            char data[0];

            const char* name() const
            {
                return data;
            }
            uint16_t nameLen() const
            {
                return url_offset;
            }
            const char* url() const
            {
                return data+url_offset;
            }
            uint16_t urlLen() const
            {
                return length-url_offset;
            }
        } __attribute__((packed));

        Buffer static createOrg(uint64_t id, Org::Type type, std::string name, std::string url, uint64_t place)
        {
            size_t size = sizeof(Org);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            Org *org = (Org *)buf.data();
            org->id = id;
            org->place = place;
            org->type = type;

            uint16_t offset = 0;
            memcpy(org->data+offset, name.c_str(), name.length());
            offset += name.length(); org->url_offset = offset;

            memcpy(org->data+offset, url.c_str(), url.length());
            offset += url.length(); org->length = offset;

            assert(org->length + sizeof(Org) == size);
            return buf;
        }
    };

    class MessageSchema : public Schema
    {
    public:
        struct Message
        {
            uint64_t id;
            uint64_t creationDate;
            uint64_t creator;
            uint64_t forumid;
            uint64_t place;
            uint64_t replyOfPost;
            uint64_t replyOfComment;
            uint16_t locationIP_offset;
            uint16_t browserUsed_offset;
            uint16_t language_offset;
            uint16_t content_offset;
            uint16_t length;
            enum class Type : char
            {
                Comment,
                Post
            } type;
            char data[0];

            const char* imageFile() const
            {
                return data;
            }
            uint16_t imageFileLen() const
            {
                return locationIP_offset;
            }
            const char* locationIP() const
            {
                return data+locationIP_offset;
            }
            uint16_t locationIPLen() const
            {
                return browserUsed_offset-locationIP_offset;
            }
            const char* browserUsed() const
            {
                return data+browserUsed_offset;
            }
            uint16_t browserUsedLen() const
            {
                return language_offset-browserUsed_offset;
            }
            const char* language() const
            {
                return data+language_offset;
            }
            uint16_t languageLen() const
            {
                return content_offset-language_offset;
            }
            const char* content() const
            {
                return data+content_offset;
            }
            uint16_t contentLen() const
            {
                return length-content_offset;
            }
        } __attribute__((packed));

        Buffer static createMessage(uint64_t id, std::string imageFile, uint64_t creationDate, std::string locationIP, std::string browserUsed, std::string language, std::string content, uint64_t creator, uint64_t forumid, uint64_t place, uint64_t replyOfPost, uint64_t replyOfComment, Message::Type type)
        {
            size_t size = sizeof(Message);
            size += imageFile.length();
            size += locationIP.length();
            size += browserUsed.length();
            size += language.length();
            size += content.length();

            Buffer buf(size);
            Message *message = (Message *)buf.data();
            message->id = id;
            message->creationDate = creationDate;
            message->creator = creator;
            message->forumid = forumid;
            message->place = place;
            message->replyOfPost = replyOfPost;
            message->replyOfComment = replyOfComment;
            message->type = type;

            uint16_t offset = 0;
            memcpy(message->data+offset, imageFile.c_str(), imageFile.length());
            offset += imageFile.length(); message->locationIP_offset = offset;

            memcpy(message->data+offset, locationIP.c_str(), locationIP.length());
            offset += locationIP.length(); message->browserUsed_offset = offset;

            memcpy(message->data+offset, browserUsed.c_str(), browserUsed.length());
            offset += browserUsed.length(); message->language_offset = offset;

            memcpy(message->data+offset, language.c_str(), language.length());
            offset += language.length(); message->content_offset = offset;

            memcpy(message->data+offset, content.c_str(), content.length());
            offset += content.length(); message->length = offset;

            assert(message->length + sizeof(Message) == size);
            return buf;
        }
    };

    class TagSchema : public Schema
    {
    public:
        struct Tag
        {
            uint64_t id;
            uint64_t hasType;
            uint16_t url_offset;
            uint16_t length;
            char data[0];

            const char* name() const
            {
                return data;
            }
            uint16_t nameLen() const
            {
                return url_offset;
            }
            const char* url() const
            {
                return data+url_offset;
            }
            uint16_t urlLen() const
            {
                return length-url_offset;
            }
        } __attribute__((packed));

        Buffer static createTag(uint64_t id, std::string name, std::string url, uint64_t hasType)
        {
            size_t size = sizeof(Tag);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            Tag *tag = (Tag *)buf.data();
            tag->id = id;
            tag->hasType = hasType;

            uint16_t offset = 0;
            memcpy(tag->data+offset, name.c_str(), name.length());
            offset += name.length(); tag->url_offset = offset;

            memcpy(tag->data+offset, url.c_str(), url.length());
            offset += url.length(); tag->length = offset;

            assert(tag->length + sizeof(Tag) == size);
            return buf;
        }
    };

    class TagClassSchema : public Schema
    {
    public:
        struct TagClass
        {
            uint64_t id;
            uint64_t isSubclassOf;
            uint16_t url_offset;
            uint16_t length;
            char data[0];

            const char* name() const
            {
                return data;
            }
            uint16_t nameLen() const
            {
                return url_offset;
            }
            const char* url() const
            {
                return data+url_offset;
            }
            uint16_t urlLen() const
            {
                return length-url_offset;
            }
        } __attribute__((packed));

        Buffer static createTagClass(uint64_t id, std::string name, std::string url, uint64_t isSubclassOf)
        {
            size_t size = sizeof(TagClass);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            TagClass *tagclass = (TagClass *)buf.data();
            tagclass->id = id;
            tagclass->isSubclassOf = isSubclassOf;

            uint16_t offset = 0;
            memcpy(tagclass->data+offset, name.c_str(), name.length());
            offset += name.length(); tagclass->url_offset = offset;

            memcpy(tagclass->data+offset, url.c_str(), url.length());
            offset += url.length(); tagclass->length = offset;

            assert(tagclass->length + sizeof(TagClass) == size);
            return buf;
        }
    };

    class ForumSchema : public Schema
    {
    public:
        struct Forum
        {
            uint64_t id;
            uint64_t creationDate;
            uint64_t moderator;
            uint16_t length;
            char data[0];

            const char* title() const
            {
                return data;
            }
            uint16_t titleLen() const
            {
                return length;
            }
        } __attribute__((packed));

        Buffer static createForum(uint64_t id, std::string title, uint64_t creationDate, uint64_t moderator)
        {
            size_t size = sizeof(Forum);
            size += title.length();

            Buffer buf(size);
            Forum *forum = (Forum *)buf.data();
            forum->id = id;
            forum->creationDate = creationDate;
            forum->moderator = moderator;

            uint16_t offset = 0;
            memcpy(forum->data+offset, title.c_str(), title.length());
            offset += title.length(); forum->length = offset;

            assert(forum->length + sizeof(Forum) == size);
            return buf;
        }
    };

    enum class EdgeSchema : int16_t
    {
        Forum2Post,                //imported
        //Post2Forum,              //inlined
        //Message2Person_creator,  //DateTime, inlined
        Person2Post_creator,       //DateTime, imported
        Person2Comment_creator,    //DateTime, imported
        Person2Tag,                //imported
        Tag2Person,                //imported
        Forum2Person_member,       //DateTime, imported
        Person2Forum_member,       //DateTime, imported
        //Forum2Person_moderator,  //inlined
        Person2Forum_moderator,    //imported
        Post2Tag,                  //imported
        Tag2Post,                  //imported
        Comment2Tag,               //imported
        Tag2Comment,               //imported
        Forum2Tag,                 //imported
        Tag2Forum,                 //imported
        //Tag2TagClass,            //inlined
        TagClass2Tag,              //imported
        Place2Place_down,          //imported
        //Place2Place_up,          //inlined
        //Person2Place,            //inlined
        Place2Person,              //imported
        //Org2Place,               //inlined
        Place2Org,                 //imported
        //TagClass2TagClass_up,    //inlined
        TagClass2TagClass_down,    //imported
        Person2Person,             //DateTime, imported
        Person2Post_like,          //DateTime, imported
        Post2Person_like,          //DateTime, imported
        Person2Comment_like,       //DateTime, imported
        Comment2Person_like,       //DateTime, imported
        //Message2Message_up,      //DateTime, inlined
        Message2Message_down,      //DateTime, imported
        Person2Org_study,          //Year, imported
        Org2Person_study,          //Year, imported
        Person2Org_work,           //Year, imported
        Org2Person_work,           //Year, imported
    };
}
