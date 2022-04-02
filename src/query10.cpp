#include "manual.hpp"

void InteractiveHandler::query10(std::vector<Query10Response> & _return, const Query10Request& request)
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

    for(size_t i=0;i<friends.size();i++)
    {
        uint64_t vid = friends[i];
        auto person = (snb::PersonSchema::Person*)(engine.get_vertex(vid)).data();
        std::pair<int, int> monday;

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
        _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().commonInterestSore = -p.first.first;
        _return.back().personGender = std::string(person->gender(), person->genderLen());
        _return.back().personCityName = std::string(place->name(), place->nameLen());
    }

}
