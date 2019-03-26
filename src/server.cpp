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
                    person_v.size()>8?std::stoull(person_v[8]):(uint64_t)-1);

            const snb::PersonSchema::Person *person = (const snb::PersonSchema::Person*)person_buf.data();
//            std::cout << person->id << "|" << person->place << "|" << std::string(person->firstName(), person->firstNameLen()) << "|" 
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

    snb::PlaceSchema placeSchema;
    {
        std::ifstream ifs_place(path+"/place_0_0.csv");

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
            std::cout << place->id << "|" << std::string(place->name(), place->nameLen()) << "|" 
                << std::string(place->url(), place->urlLen()) << "|"  << (int)place->type << "|" << place->isPartOf << std::endl;

        }
    }
}
