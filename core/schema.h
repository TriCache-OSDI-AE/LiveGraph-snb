#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <cassert>
#include <tbb/concurrent_hash_map.h>

namespace snb
{
    typedef std::vector<char> Buffer;
    class PersonSchema
    {
    private:
        tbb::concurrent_hash_map<uint64_t, uint64_t> vindex;
    public:
        struct Person
        {
            uint64_t id;
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

        Buffer static createPerson(uint64_t id, std::string firstName, std::string lastName, std::string gender, uint32_t birthday, std::vector<std::string> emails, std::vector<std::string> speaks, std::string browserUsed, std::string locationIP, uint64_t creationDate)
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

        void insertPersonId(uint64_t id, uint64_t vid)
        {   
            tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor a;
            vindex.insert(a, id);
            a->second = vid;
        }

        uint64_t findPersonId(uint64_t id)
        {
            tbb::concurrent_hash_map<uint64_t, uint64_t>::const_accessor a;
            bool exist = vindex.find(a, id);
            if(!exist)return (uint64_t)-1;
            return a->second;
        }
    };
}
