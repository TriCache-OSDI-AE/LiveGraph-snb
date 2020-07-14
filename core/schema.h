#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <cassert>
#include <tbb/concurrent_hash_map.h>
#include <rocksdb/db.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include <rocksdb/options.h>

namespace snb
{
    typedef std::vector<char> Buffer;
    enum class VertexSchema : int64_t
    {
        person,
        place,
        org,
        message,
        tag,
        tagclass,
        forum
    };

    class Schema
    {
    private:
        rocksdb::DB* db;
        rocksdb::ColumnFamilyHandle* vindex;
        rocksdb::ColumnFamilyHandle* rvindex;
        rocksdb::ColumnFamilyHandle* nameindex;
        rocksdb::ReadOptions read_options;
        rocksdb::WriteOptions write_options;
    public:
        void insertId(uint64_t id, uint64_t vid)
        {   
            rocksdb::Slice key((char *)&id, sizeof(id));
            rocksdb::Slice val((char *)&vid, sizeof(vid));
            db->Put(write_options, vindex, key, val);
            db->Put(write_options, rvindex, val, key);
        }

        uint64_t findId(uint64_t id)
        {
            rocksdb::Slice key((char *)&id, sizeof(id));
            std::string val;
            rocksdb::Status status = db->Get(read_options, vindex, key, &val);
            if(!status.ok()) return (uint64_t)-1;
            return *(uint64_t*)val.data();
        }

        uint64_t rfindId(uint64_t vid)
        {
            rocksdb::Slice key((char *)&vid, sizeof(vid));
            std::string val;
            rocksdb::Status status = db->Get(read_options, rvindex, key, &val);
            if(!status.ok()) return (uint64_t)-1;
            return *(uint64_t*)val.data();
        }

        void insertName(std::string name, uint64_t vid)
        {   
            rocksdb::Slice key(name);
            rocksdb::Slice val((char *)&vid, sizeof(vid));
            db->Put(write_options, nameindex, key, val);
        }

        uint64_t findName(std::string name)
        {
            rocksdb::Slice key(name);
            std::string val;
            rocksdb::Status status = db->Get(read_options, nameindex, key, &val);
            if(!status.ok()) return (uint64_t)-1;
            return *(uint64_t*)val.data();
        }
        Schema()
            :db(nullptr), vindex(nullptr), rvindex(nullptr), nameindex(nullptr), read_options(), write_options()
        {
        }
        Schema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :db(_db), vindex(_vindex), rvindex(_rvindex), nameindex(_nameindex), read_options(), write_options()
        {
            read_options.prefix_same_as_start = true;
            write_options.sync = false;
            write_options.disableWAL = true;
        }
    };
    class PersonSchema : public Schema
    {
    public:
        struct Person
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

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
            person->vtype = VertexSchema::person;
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

        PersonSchema()
            :Schema()
        {
        }
        PersonSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

        }

    };

    class PlaceSchema : public Schema
    {
    public:

        struct Place
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

        Buffer static createPlace(uint64_t id, std::string name, std::string url, Place::Type type, uint64_t isPartOf)
        {
            size_t size = sizeof(Place);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            Place *place = (Place *)buf.data();
            place->vtype = VertexSchema::place;
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

        PlaceSchema()
            :Schema()
        {
        }
        PlaceSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

        }
    };

    class OrgSchema : public Schema
    {
    public:
        struct Org
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

        Buffer static createOrg(uint64_t id, Org::Type type, std::string name, std::string url, uint64_t place)
        {
            size_t size = sizeof(Org);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            Org *org = (Org *)buf.data();
            org->vtype = VertexSchema::org;
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

        OrgSchema()
            :Schema()
        {
        }
        OrgSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

        }
    };

    class MessageSchema : public Schema
    {
    public:
        struct Message
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

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
            message->vtype = VertexSchema::message;
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

        MessageSchema()
            :Schema()
        {
        }
        MessageSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

        }
    };

    class TagSchema : public Schema
    {
    public:
        struct Tag
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

        Buffer static createTag(uint64_t id, std::string name, std::string url, uint64_t hasType)
        {
            size_t size = sizeof(Tag);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            Tag *tag = (Tag *)buf.data();
            tag->vtype = VertexSchema::tag;
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

        TagSchema()
            :Schema()
        {
        }
        TagSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

        }
    };

    class TagClassSchema : public Schema
    {
    public:
        struct TagClass
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

        Buffer static createTagClass(uint64_t id, std::string name, std::string url, uint64_t isSubclassOf)
        {
            size_t size = sizeof(TagClass);
            size += name.length();
            size += url.length();

            Buffer buf(size);
            TagClass *tagclass = (TagClass *)buf.data();
            tagclass->vtype = VertexSchema::tagclass;
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

        TagClassSchema()
            :Schema()
        {
        }
        TagClassSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

        }
    };

    class ForumSchema : public Schema
    {
    public:
        struct Forum
        {
            VertexSchema vtype;
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
        };// __attribute__((packed));

        Buffer static createForum(uint64_t id, std::string title, uint64_t creationDate, uint64_t moderator)
        {
            size_t size = sizeof(Forum);
            size += title.length();

            Buffer buf(size);
            Forum *forum = (Forum *)buf.data();
            forum->vtype = VertexSchema::forum;
            forum->id = id;
            forum->creationDate = creationDate;
            forum->moderator = moderator;

            uint16_t offset = 0;
            memcpy(forum->data+offset, title.c_str(), title.length());
            offset += title.length(); forum->length = offset;

            assert(forum->length + sizeof(Forum) == size);
            return buf;
        }

        ForumSchema()
            :Schema()
        {
        }
        ForumSchema(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_vindex, rocksdb::ColumnFamilyHandle *_rvindex, rocksdb::ColumnFamilyHandle *_nameindex)
            :Schema(_db, _vindex, _rvindex, _nameindex)
        {

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
        Forum2Person_member,       //DateTime(unordered), uint64_t, imported
        Person2Forum_member,       //DateTime(unordered), uint64_t, imported
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
        Person2Person,             //DateTime(unordered), double, imported
        Person2Post_like,          //DateTime(unordered), imported
        Post2Person_like,          //DateTime(unordered), imported
        Person2Comment_like,       //DateTime(unordered), imported
        Comment2Person_like,       //DateTime(unordered), imported
        //Message2Message_up,      //DateTime, inlined
        Message2Message_down,      //DateTime, imported
        Person2Org_study,          //Year(unordered), imported
        Org2Person_study,          //Year(unordered), imported
        Person2Org_work,           //Year(unordered), imported
        Org2Person_work,           //Year(unordered), imported
    };
}
