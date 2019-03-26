#include <regex>
#include <ctime>
#include <chrono>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <iostream>
#include "core/schema.h" 

std::vector<std::string> split(const std::string &s, char delim) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, delim)) {
        elems.push_back(std::move(item)); // if C++11 (based on comment from @mchiasson)
    }
    return elems;
}
std::chrono::system_clock::time_point static from_date(const std::string &str)
{
    const std::string date_parse("%Y-%m-%d");
    std::tm tm{};
    std::stringstream ss(str);
    ss >> std::get_time(&tm, date_parse.c_str());
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}
std::chrono::system_clock::time_point static from_time(const std::string &str)
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

int main(int argc, char** argv)
{
    const char csv_split('|');
    const char zero_split('\000');
    std::string path = argv[1];
    snb::PersonSchema personSchema;
    {
        std::ifstream ifs_person(path+"/person_0_0.csv");
        std::ifstream ifs_emails(path+"/person_email_emailaddress_0_0.csv");
        std::ifstream ifs_speaks(path+"/person_speaks_language_0_0.csv");

        std::vector<std::string> all_persons, all_emails, all_speaks;
        for(std::string line;std::getline(ifs_person, line);) all_persons.push_back(line);
        for(std::string line;std::getline(ifs_emails, line);) all_emails.push_back(line);
        for(std::string line;std::getline(ifs_speaks, line);) all_speaks.push_back(line);
        assert(all_persons[0] == "id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed");
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
                    std::chrono::duration_cast<std::chrono::milliseconds>(creationDate.time_since_epoch()).count());

//            const snb::PersonSchema::Person *person = (const snb::PersonSchema::Person*)person_buf.data();
//            std::cout << person->id << "|" << std::string(person->firstName(), person->firstNameLen()) << "|" 
//                << std::string(person->lastName(), person->lastNameLen()) << "|" 
//                << std::string(person->gender(), person->genderLen()) << "|" 
//                << std::string(person->browserUsed(), person->browserUsedLen()) << "|" 
//                << std::string(person->locationIP(), person->locationIPLen()) << "|";
//            std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::hours(person->birthday)));
//            std::cout << std::ctime(&t) << "|";
//            t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::milliseconds(person->creationDate)));
//            std::cout << std::ctime(&t) << "|";
//            emails_v = split(std::string(person->emails(), person->emailsLen()), zero_split);
//            for(auto e:emails_v) std::cout << e << ",";
//            std::cout << "|";
//            speaks_v = split(std::string(person->speaks(), person->speaksLen()), zero_split);
//            for(auto e:speaks_v) std::cout << e << ",";
//            std::cout << std::endl;
        }
    }

}
