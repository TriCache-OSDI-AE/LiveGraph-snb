#pragma once

#include "core/lg.hpp"
#include "core/schema.hpp" 
#include "core/util.hpp"
#include <iostream>
#include <unordered_set>

#include "Interactive.h"
using namespace ::interactive;

template <typename T, typename = std::enable_if_t<std::is_trivial_v<T>>> inline std::string to_string(T data)
{    
    return std::string(reinterpret_cast<char *>(&data), sizeof(T));
}


class InteractiveHandler : virtual public InteractiveIf
{
public:
    InteractiveHandler(Graph * const graph, snb::PersonSchema &personSchema,  snb::PlaceSchema &placeSchema,  snb::OrgSchema &orgSchema,  snb::MessageSchema &postSchema,  snb::MessageSchema &commentSchema,  snb::TagSchema &tagSchema,  snb::TagClassSchema &tagclassSchema,  snb::ForumSchema &forumSchema)
        :graph(graph), personSchema(personSchema), placeSchema(placeSchema), orgSchema(orgSchema), postSchema(postSchema), commentSchema(commentSchema), tagSchema(tagSchema), tagclassSchema(tagclassSchema), forumSchema(forumSchema)
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
                    nbrs.next();
                }
            }
        }
        for(auto p : idx) _return.emplace_back(p.second);
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
                        //flag = true;
                    }
                    else
                    {
                        flag = idx.size() < (size_t)request.limit || idx.rbegin()->first.first > key.first;
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
                        //flag = true;
                    }
                    else
                    {
                        flag = idx.size() < (size_t)request.limit || idx.rbegin()->first.first > key.first;
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
            _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
            _return.back().likeCreationDate = like_time;
            _return.back().commentOrPostId = message->id;
            _return.back().commentOrPostContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
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
            _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
            _return.back().commentId = message->id;;
            _return.back().commentCreationDate = message->creationDate;
            _return.back().commentContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
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
            _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
            _return.back().messageId = message->id;;
            _return.back().messageCreationDate = message->creationDate;
            _return.back().messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
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
            _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
            _return.back().commonInterestSore = -p.first.first;
            _return.back().personGender = std::string(person->gender(), person->genderLen());
            _return.back().personCityName = std::string(place->name(), place->nameLen());
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
        _return.firstName = std::string(person->firstName(), person->firstNameLen());
        _return.lastName = std::string(person->lastName(), person->lastNameLen());
        _return.birthday = to_time(person->birthday);
        _return.locationIp = std::string(person->locationIP(), person->locationIPLen());
        _return.browserUsed = std::string(person->browserUsed(), person->browserUsedLen());
        auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
        _return.cityId = place->id;
        _return.gender = std::string(person->gender(), person->genderLen());
        _return.creationDate = person->creationDate;
    }

    void shortQuery2(std::vector<ShortQuery2Response> & _return, const ShortQuery2Request& request)
    {
        _return.clear();
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
            _return.back().messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
            for(;message->replyOfComment != (uint64_t)-1;message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfComment).data());
            if(message->replyOfPost != (uint64_t)-1) message = (snb::MessageSchema::Message*)engine.get_vertex(message->replyOfPost).data();
            auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
            _return.back().originalPostId = message->id;
            _return.back().originalPostAuthorId = person->id;
            _return.back().originalPostAuthorFirstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().originalPostAuthorLastName = std::string(person->lastName(), person->lastNameLen());
        }

    }

    void shortQuery3(std::vector<ShortQuery3Response> & _return, const ShortQuery3Request& request)
    {
        _return.clear();
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
            _return.back().firstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().lastName = std::string(person->lastName(), person->lastNameLen());
            _return.back().friendshipCreationDate = -std::get<0>(p);
        }

    }

    void shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request& request)
    {
        _return = ShortQuery4Response();
        uint64_t vid = postSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
        if(!message) return;
        _return.messageCreationDate = message->creationDate;
        _return.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
    }

    void shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request& request)
    {
        _return = ShortQuery5Response();
        uint64_t vid = postSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
        if(vid == (uint64_t)-1) return;
        auto engine = graph->begin_read_only_transaction();
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
        if(!message) return;
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
        _return.personId = person->id;
        _return.firstName = std::string(person->firstName(), person->firstNameLen());
        _return.lastName = std::string(person->lastName(), person->lastNameLen());
    }

    void shortQuery6(ShortQuery6Response& _return, const ShortQuery6Request& request)
    {
        _return = ShortQuery6Response();
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
        _return.forumTitle = std::string(forum->title(), forum->titleLen());
        _return.moderatorId = person->id;
        _return.moderatorFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.moderatorLastName = std::string(person->lastName(), person->lastNameLen());
    }

    void shortQuery7(std::vector<ShortQuery7Response>& _return, const ShortQuery7Request& request)
    {
        _return.clear();
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
            _return.back().commentContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
            _return.back().replyAuthorId = person->id;
            _return.back().replyAuthorFirstName = std::string(person->firstName(), person->firstNameLen());
            _return.back().replyAuthorLastName = std::string(person->lastName(), person->lastNameLen());
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
                if(vid == (uint64_t)-1)
                {
                    vid = txn.new_vertex();
                    personSchema.insertId(request.personId, vid);
                }
                txn.put_vertex(vid, std::string_view(person_buf.data(), person_buf.size()));
                txn.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Person, vid, {});
                for(auto tag:tag_vid)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Tag, tag, {});
                    txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Person, vid, {});
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
                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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
                if(vid == (uint64_t)-1)
                {
                    vid = txn.new_vertex();
                    forumSchema.insertId(request.forumId, vid);
                }
                txn.put_vertex(vid, std::string_view(forum_buf.data(), forum_buf.size()));
                txn.put_edge(moderator_vid, (label_t)snb::EdgeSchema::Person2Forum_moderator, vid, std::string_view());
                for(auto tag:tag_vid)
                {
                    txn.put_edge(vid, (label_t)snb::EdgeSchema::Forum2Tag, tag, std::string_view());
                    txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Forum, vid, std::string_view());
                }

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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
                if(vid == (uint64_t)-1)
                {
                    vid = txn.new_vertex();
                    postSchema.insertId(request.postId, vid);
                }
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
                        auto buf = std::string(bytes);
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

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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
                if(vid == (uint64_t)-1)
                {
                    vid = txn.new_vertex();
                    commentSchema.insertId(request.commentId, vid);
                }
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
                        auto buf = std::string(bytes);
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

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
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

                auto epoch = txn.commit();
                break;
            } 
            catch (typename decltype(txn)::RollbackExcept &e) 
            {
                txn.abort();
            }
        }

    }

private:
    Graph * const graph;
    snb::PersonSchema &personSchema;
    snb::PlaceSchema &placeSchema;
    snb::OrgSchema &orgSchema;
    snb::MessageSchema &postSchema;
    snb::MessageSchema &commentSchema;
    snb::TagSchema &tagSchema;
    snb::TagClassSchema &tagclassSchema;
    snb::ForumSchema &forumSchema;
    static const bool enableSchemaCheck = false;

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

};

