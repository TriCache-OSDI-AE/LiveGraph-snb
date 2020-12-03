#pragma once

#include "core/lg.hpp"
#include "core/schema.hpp" 
#include "core/util.hpp"
#include "neo4j_op.hpp"
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

    /*
    void test()
    {
        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek_1_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(32985348886934));
        NodeIndexSeek node_index_seek_2_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(15393162803709));
        auto cartesian_product = CartesianProduct(node_index_seek_1_1, node_index_seek_2_1);
        auto shortest_path_all = ShortestPathAll(cartesian_product, [](const auto &tuple) INLINE {
            return tuple;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);

        auto empty = Empty<std::tuple<uint64_t, uint64_t>>();
        auto argument = Argument(empty, [](const auto &tuple) INLINE
        {
            const auto &[src, dst] = tuple;
            return std::make_tuple(src, dst);
        });
        auto expand_1 = ExpandAll(argument, [](const auto &tuple) INLINE {
            const auto &[src, dst] = tuple;
            return src;
        }, txn, (label_t)snb::EdgeSchema::Person2Comment_creator);
        auto filter_1 = Filter(expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment] = tuple;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Comment;
        });
        auto expand_2 = ExpandAll(filter_1, [](const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment] = tuple;
            return comment;
        }, txn, (label_t)snb::EdgeSchema::Message2Message_up);
        auto filter_2 = Filter(expand_2, [&txn](const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment, commentReplyof, _message] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_message).data();
            return *type == snb::VertexSchema::Message;
        });
        auto expand_3 = ExpandInto(filter_2, [](const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment, commentReplyof, _message] = tuple;
            return std::make_tuple(_message, dst);
        }, txn, (label_t)snb::EdgeSchema::Message2Person_creator);
        auto eager_aggragation = EagerAggragation(expand_3, [](const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment, commentReplyof, _message, _messageHasCreator] = tuple;
            return std::make_tuple(src, dst);
        }, [&txn](const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment, commentReplyof, _message, _messageHasCreator] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
            return message->type == snb::MessageSchema::Message::Type::Comment ? 0.5 : 1.0;
        }, [&txn](auto &ret, const auto &tuple) INLINE {
            const auto &[src, dst, srcCreateComment, comment, commentReplyof, _message, _messageHasCreator] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
            ret += message->type == snb::MessageSchema::Message::Type::Comment ? 0.5 : 1.0;
        }, [](const auto &ret) INLINE {
            return std::make_tuple(ret);
        });
        auto cur_value = 0.0;
        auto sub_plan = eager_aggragation.gen_plan([&cur_value](const auto &tuple) INLINE {
            cur_value += std::get<2>(tuple);
        });

        auto projection = Projection(shortest_path_all, [&sub_plan, &cur_value, &txn](const auto &tuple) INLINE {
            const auto &[person1, person2, p, path] = tuple;
            double sum = 0;
            for(const auto &p : path)
            {
                const auto &[src, dst, value] = p;
                sub_plan(std::make_tuple(src, dst));
                sum += cur_value; cur_value = 0;
                sub_plan(std::make_tuple(dst, src));
                sum += cur_value; cur_value = 0;
            }
            std::vector<uint64_t> personIds;
            for(const auto &_person : p)
            {
                auto person = (snb::PersonSchema::Person*)txn.get_vertex(_person).data();
                personIds.emplace_back(person->id);
            }
            return std::make_tuple(sum, personIds);
        });
        auto sort = Sort(projection, [](const auto &tuple) INLINE {
            const auto &[sum, p] = tuple;
            return std::make_tuple(sum);
        }, [](const auto &x, const auto &y) INLINE {
            return std::get<0>(x) > std::get<0>(y);
        });

        uint64_t sum = 0;
        auto plan = sort.gen_plan([&sum](const auto &tuple) INLINE {
            sum++;
            std::cout << tuple << std::endl;
        });

        plan();

        std::cout << sum << std::endl;

    }
    */

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
    }

    void shortQuery2(std::vector<ShortQuery2Response> & _return, const ShortQuery2Request& request)
    {
        _return.clear();
    }

    void shortQuery3(std::vector<ShortQuery3Response> & _return, const ShortQuery3Request& request)
    {
        _return.clear();
    }

    void shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request& request)
    {
        _return = ShortQuery4Response();
    }

    void shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request& request)
    {
        _return = ShortQuery5Response();
    }

    void shortQuery6(ShortQuery6Response& _return, const ShortQuery6Request& request)
    {
        _return = ShortQuery6Response();
    }

    void shortQuery7(std::vector<ShortQuery7Response>& _return, const ShortQuery7Request& request)
    {
        _return.clear();
    }

    void update1(const interactive::Update1Request& request)
    {
    }

    void update2(const Update2Request& request)
    {
    }

    void update3(const Update3Request& request)
    {
    }

    void update4(const Update4Request& request)
    {
    }

    void update5(const Update5Request& request)
    {
    }

    void update6(const Update6Request& request)
    {
    }

    void update7(const Update7Request& request)
    {
    }

    void update8(const Update8Request& request)
    {
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

