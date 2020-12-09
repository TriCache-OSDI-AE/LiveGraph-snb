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

        Transaction txn = graph->begin_read_only_transaction();

        //NodeIndexSeek node_index_seek_1_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        //NodeIndexSeek node_index_seek_2_1(personSchema.db, personSchema.nameindex, request.firstName);
        //auto cartesian_product = CartesianProduct(node_index_seek_1_1, node_index_seek_2_1);

        //auto shortest_path = ShortestPath(cartesian_product, [](const auto &tuple) INLINE {
        //    return std::make_tuple(std::get<0>(tuple), std::get<1>(tuple));
        //}, txn, (label_t)snb::EdgeSchema::Person2Person, 3);
        //auto projection_2_1 = Projection(shortest_path, [](const auto &tuple) INLINE {
        //    const auto &[anon_22, _friend, p, path] = tuple;
        //    return std::make_tuple(path, _friend, p, anon_22, p.size()-1);
        //});
        //auto anon_filter_1_1 = Filter(projection_2_1, [](const auto &tuple) INLINE {
        //    const auto &[path, _friend, p, anon_22, distance] = tuple;
        //    return distance > 0;
        //});
        
        NodeIndexSeek node_index_seek_1_1(personSchema.db, personSchema.nameindex, request.firstName);
        NodeIndexSeek node_index_seek_2_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));

        auto shortest_path = SingleSourceShortestPath(node_index_seek_2_1, [](const auto &tuple) INLINE {
            return std::get<0>(tuple);
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 3);
        auto hash_join = SortJoin(node_index_seek_1_1, shortest_path, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;
        }, [](const auto &tuple) INLINE {
            const auto &[anon_22, _friend, length] = tuple;
            return _friend;
        });
        auto projection_2_1 = Projection(hash_join, [](const auto &tuple) INLINE {
            const auto &[_friend_l, anon_22, _friend, length] = tuple;
            return std::make_tuple(0, _friend, 0, anon_22, length);
        });

        auto projection_2_2 = Projection(projection_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[path, _friend, p, anon_22, distance] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(person->id, path, std::string_view(person->lastName(), person->lastNameLen()),_friend, p, anon_22, distance);
        });
        auto cache_2_1 = CacheProperties(projection_2_2, [](const auto &tuple) INLINE {
            const auto &[friend_id, path, friend_lastName, _friend, p, anon_22, distance] = tuple;
            auto new_cache = std::make_tuple(friend_id, friend_lastName);
            return std::tuple_cat(tuple, std::make_tuple(new_cache));
        });
        auto top = Top(cache_2_1, [](const auto &tuple) INLINE {
            const auto &[friend_id, path, friend_lastName, _friend, p, anon_22, distance, cache] = tuple;
            return std::make_tuple(distance, friend_lastName, friend_id);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) < std::get<0>(y)) return true;
            if(std::get<0>(x) > std::get<0>(y)) return false;
            if(std::get<1>(x) < std::get<1>(y)) return true;
            if(std::get<1>(x) > std::get<1>(y)) return false;
            return std::get<2>(x) < std::get<2>(y);
        }, request.limit);


        auto argument_3 = Argument(top, [](const auto &tuple) INLINE
        {
            const auto &[friend_id, path, friend_lastName, _friend, p, anon_22, distance, cache] = tuple;
            return std::make_tuple(_friend, distance);
        });
        //auto expand_3_1 = ExpandAll(argument_3, [](const auto &tuple) INLINE {
        //    const auto &[_friend, distance] = tuple;
        //    return _friend;
        //}, txn, (label_t)snb::EdgeSchema::Person2Place);
        auto expand_3_1 = Projection(argument_3, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, distance] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(_friend, distance, 0, person->place);
        });
        auto filter_3_1 = Filter(expand_3_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, distance, anon_233, friendCity] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(friendCity).data();
            return *type == snb::VertexSchema::Place;
        });

        auto argument_4 = Argument(filter_3_1, [](const auto &tuple) INLINE
        {
            const auto &[_friend, distance, anon_233, friendCity] = tuple;
            return std::make_tuple(_friend);
        });
        auto expand_4_1 = ExpandAll(argument_4, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;
        }, txn, (label_t)snb::EdgeSchema::Person2Org_study);
        auto filter_4_1 = Filter(expand_4_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, studyAt, uni] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(uni).data();
            return *type == snb::VertexSchema::Org;
        });
        auto cache_4_1 = CacheProperties(filter_4_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, studyAt, uni] = tuple;
            auto org = (snb::OrgSchema::Org*)txn.get_vertex(uni).data();
            auto new_cache = std::make_tuple(org->id, *(int32_t*)std::get<2>(studyAt).data(), std::string_view(org->name(), org->nameLen()));
            return std::tuple_cat(tuple, std::make_tuple(new_cache));
        });
        //auto expand_4_2 = ExpandAll(cache_4_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, studyAt, uni, cache] = tuple;
        //    return uni;
        //}, txn, (label_t)snb::EdgeSchema::Org2Place);
        auto expand_4_2 = Projection(cache_4_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, studyAt, uni, cache] = tuple;
            auto org = (snb::OrgSchema::Org*)txn.get_vertex(uni).data();
            return std::make_tuple(_friend, studyAt, uni, cache, 0, org->place);
        });
        auto cache_4_2 = CacheProperties(expand_4_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, studyAt, uni, cache, anon_333, uniCity] = tuple;
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(uniCity).data();
            auto new_cache = std::tuple_cat(cache, std::make_tuple(std::string_view(place->name(), place->nameLen())));
            return std::tuple_cat(std::make_tuple(_friend, anon_333, uni, uniCity, studyAt), std::make_tuple(new_cache));
        });
        auto filter_4_2 = Filter(cache_4_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_333, uni, uniCity, studyAt, cache] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(uniCity).data();
            return *type == snb::VertexSchema::Place;
        });
        auto optional_4 = Optional(filter_4_2);

        auto apply_1 = Apply(filter_3_1, optional_4, [](const auto &left, const auto &right) INLINE {
            const auto &[_friend, distance, anon_233, friendCity] = left;
            const auto &[_friend_r, anon_333, uni, uniCity, studyAt, cache] = right;
            return std::make_tuple(friendCity, _friend, anon_333, uni, uniCity, anon_233, studyAt, distance, cache);

        });

        auto apply_2 = Apply(top, apply_1, [](const auto &left, const auto &right) INLINE {
            const auto &[friend_id, path, friend_lastName, _friend, p, anon_22, distance, cache] = left;
            const auto &[friendCity, _friend_r, anon_333, uni, uniCity, anon_233, studyAt, distance_r, cache_r] = right;
            return std::make_tuple(friend_id, friendCity, path, friend_lastName, _friend, anon_333, uni, p, uniCity, anon_22, anon_233, studyAt, distance, std::tuple_cat(cache, cache_r));
        });

        auto ordered_aggragation_1 = OrderedAggragation(apply_2, [](const auto &tuple) INLINE {
            const auto &[friend_id, friendCity, path, friend_lastName, _friend, anon_333, uni, p, uniCity, anon_22, anon_233, studyAt, distance, cache] = tuple;
            return std::make_tuple(distance, _friend, friendCity, std::make_tuple(std::get<0>(cache), std::get<1>(cache)));
        }, [](const auto &tuple) INLINE {
            const auto &[friend_id, friendCity, path, friend_lastName, _friend, anon_333, uni, p, uniCity, anon_22, anon_233, studyAt, distance, cache] = tuple;
            std::vector<std::tuple<uint64_t, std::string_view, int32_t, std::string_view>> ret;
            if(!std::get<4>(cache).empty()) ret.emplace_back(std::get<2>(cache), std::get<4>(cache), std::get<3>(cache), std::get<5>(cache));
            return std::make_tuple(ret);
        }, [](auto &cur_tuple, const auto &tuple) INLINE {
            const auto &[friend_id, friendCity, path, friend_lastName, _friend, anon_333, uni, p, uniCity, anon_22, anon_233, studyAt, distance, cache] = tuple;
            auto &[ret] = cur_tuple;
            if(!std::get<4>(cache).empty()) ret.emplace_back(std::get<2>(cache), std::get<4>(cache), std::get<3>(cache), std::get<5>(cache));
        }, [](const auto &tuple) INLINE {
            return tuple;
        });

        auto argument_5 = Argument(ordered_aggragation_1, [](const auto &tuple) INLINE
        {
            const auto &[distance, _friend, friendCity, cache, unis] = tuple;
            return std::make_tuple(_friend);
        });
        auto expand_5_1 = ExpandAll(argument_5, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;

        }, txn, (label_t)snb::EdgeSchema::Person2Org_work);
        auto filter_5_1 = Filter(expand_5_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, workAt, company] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(company).data();
            return *type == snb::VertexSchema::Org;
        });
        //auto expand_5_2 = ExpandAll(filter_5_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company] = tuple;
        //    return company;
        //}, txn, (label_t)snb::EdgeSchema::Org2Place);
        auto expand_5_2 = Projection(filter_5_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, workAt, company] = tuple;
            auto org = (snb::OrgSchema::Org*)txn.get_vertex(company).data();
            return std::make_tuple(_friend, workAt, company, 0, org->place);
        });
        auto filter_5_2 = Filter(expand_5_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, workAt, company, anon_573, companyCountry] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(companyCountry).data();
            return *type == snb::VertexSchema::Place;
        });
        auto optional_5 = Optional(filter_5_2);

        auto apply_3 = Apply(ordered_aggragation_1, optional_5, [](const auto &left, const auto &right) INLINE {
            const auto &[distance, _friend, friendCity, cache, unis] = left;
            const auto &[_friend_r, workAt, company, anon_573, companyCountry] = right;
            return std::make_tuple(friendCity, _friend, company, unis, anon_573, companyCountry, distance, workAt, cache);
        });

        auto ordered_aggragation_2 = OrderedAggragation(apply_3, [](const auto &tuple) INLINE {
            const auto &[friendCity, _friend, company, unis, anon_573, companyCountry, distance, workAt, cache] = tuple;
            return std::make_tuple(distance, _friend, unis, friendCity, cache);
        }, [&txn](const auto &tuple) INLINE {
            const auto &[friendCity, _friend, company, unis, anon_573, companyCountry, distance, workAt, cache] = tuple;
            std::vector<std::tuple<uint64_t, std::string_view, int32_t, std::string_view>> ret;
            if(!(company == 0 && companyCountry == 0))
            {
                auto org = (snb::OrgSchema::Org*)txn.get_vertex(company).data();
                auto place = (snb::PlaceSchema::Place*)txn.get_vertex(companyCountry).data();
                ret.emplace_back(org->id, std::string_view(org->name(), org->nameLen()), *(int32_t*)std::get<2>(workAt).data(), std::string_view(place->name(), place->nameLen()));
            }
            return std::make_tuple(ret);
        }, [&txn](auto &cur_tuple, const auto &tuple) INLINE {
            const auto &[friendCity, _friend, company, unis, anon_573, companyCountry, distance, workAt, cache] = tuple;
            auto &[ret] = cur_tuple;
            if(!(company == 0 && companyCountry == 0))
            {
                auto org = (snb::OrgSchema::Org*)txn.get_vertex(company).data();
                auto place = (snb::PlaceSchema::Place*)txn.get_vertex(companyCountry).data();
                ret.emplace_back(org->id, std::string_view(org->name(), org->nameLen()), *(int32_t*)std::get<2>(workAt).data(), std::string_view(place->name(), place->nameLen()));
            }
        }, [](const auto &tuple) INLINE {
            return tuple;
        });

        auto projection_1_1 = Projection(ordered_aggragation_2, [](const auto &tuple) INLINE {
            const auto &[distance, _friend, unis, friendCity, cache, companies] = tuple;
            return std::make_tuple(std::get<0>(cache), friendCity, companies, std::get<1>(cache), _friend, unis, distance);
        });
        auto partial_sort = PartialSort(projection_1_1, [](const auto &tuple) INLINE {
            const auto &[friend_id, friendCity, companies, friend_lastName, _friend, unis, distance] = tuple;
            return std::make_tuple(distance);
        }, [](const auto &tuple) INLINE {
            const auto &[friend_id, friendCity, companies, friend_lastName, _friend, unis, distance] = tuple;
            return std::make_tuple(friend_lastName, friend_id);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) < std::get<0>(y)) return true;
            if(std::get<0>(x) > std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        });

        auto projection_result = Projection(partial_sort, [&txn](const auto &tuple) INLINE {
            const auto &[friend_id, friendCity, companies, friend_lastName, _friend, unis, distance] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(friendCity).data();
            
            return std::make_tuple(friend_id, friend_lastName, distance, person->birthday, person->creationDate, 
                    std::string_view(person->gender(), person->genderLen()), 
                    std::string_view(person->browserUsed(), person->browserUsedLen()), 
                    std::string_view(person->locationIP(), person->locationIPLen()), 
                    std::string_view(person->emails(), person->emailsLen()), 
                    std::string_view(person->speaks(), person->speaksLen()), 
                    std::string_view(place->name(), place->nameLen()), 
                    unis, companies);
        });


        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[id, lastName, distance, birthday, creationDate, gender, browser, ip, emails, speaks, place, unis, companies] = tuple;
            _return.emplace_back();
            _return.back().friendId = id;
            _return.back().friendLastName = lastName;
            _return.back().distanceFromPerson = distance;
            _return.back().friendBirthday = to_time(birthday);
            _return.back().friendCreationDate = creationDate;
            _return.back().friendGender = std::string_view(gender);
            _return.back().friendBrowserUsed = std::string_view(browser);
            _return.back().friendLocationIp = std::string_view(ip);
            _return.back().friendEmails = split(std::string(emails), zero_split);
            _return.back().friendLanguages = split(std::string(speaks), zero_split);
            _return.back().friendCityName = place;
            auto ordered_unis = unis;
            std::sort(ordered_unis.begin(), ordered_unis.end());
            for(const auto &t:ordered_unis)
            {
                const auto &[org_id, name, year, place] = t;
                _return.back().friendUniversities_name.emplace_back(name);
                _return.back().friendUniversities_year.emplace_back(year);
                _return.back().friendUniversities_city.emplace_back(place);
            }
            auto ordered_companies = companies;
            std::sort(ordered_companies.begin(), ordered_companies.end());
            for(const auto &t:ordered_companies)
            {
                const auto &[org_id, name, year, place] = t;
                _return.back().friendCompanies_name.emplace_back(name);
                _return.back().friendCompanies_year.emplace_back(year);
                _return.back().friendCompanies_city.emplace_back(place);

            }
        });
        
        plan();
    }

    void query2(std::vector<Query2Response> & _return, const Query2Request& request)
    {
        _return.clear();
        
        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));

        auto expand_1_1 = ExpandAll(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[anon_7] = tuple;
            return anon_7;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);

        auto filter_1_1 = Filter(expand_1_1, [&txn](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });

        auto argument_2_1 = Argument(filter_1_1, [](const auto &tuple) INLINE
        {
            const auto &[anon_7, anon_36, _friend] = tuple;
            return std::make_tuple(_friend);
        });
        auto expand_2_1 = ExpandAllFilterLooseLimit(argument_2_1, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;
        }, [&request](const auto &edge_data) INLINE {
            return (int64_t)*(uint64_t*)edge_data.data() <= request.maxDate;
        }, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator}, request.limit);
        //auto expand_2_1 = ExpandAll(argument_2_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend] = tuple;
        //    return _friend;
        //}, txn, (label_t)snb::EdgeSchema::Person2Post_creator);
        auto anon_filter_2_1 = Filter(expand_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_61, _message] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_message).data();
            return *type == snb::VertexSchema::Message;
        });
        auto anon_filter_cache_2_1 = CacheProperties(anon_filter_2_1, [](const auto &tuple) INLINE {
            const auto &[_friend, anon_61, _message] = tuple;
            auto new_cache = std::make_tuple(*(uint64_t*)std::get<2>(anon_61).data());
            return std::tuple_cat(tuple, std::make_tuple(new_cache));
        });
        auto filter_2_1 = Filter(anon_filter_cache_2_1, [&request](const auto &tuple) INLINE {
            const auto &[_friend, anon_61, _message, cache] = tuple;
            return true;
            //return (int64_t)std::get<0>(cache) <= request.maxDate;
        });
        auto projection_2_1 = Projection(filter_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_61, _message, cache] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
            return std::make_tuple(_friend, std::get<0>(cache), anon_61, message->id, _message);
        });
        //auto partial_top_2 = PartialTop(projection_2_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, creationDate, anon_61, messageId, _message] = tuple;
        //    return std::make_tuple(creationDate);
        //}, [](const auto &tuple) INLINE {
        //    const auto &[_friend, creationDate, anon_61, messageId, _message] = tuple;
        //    return std::make_tuple(creationDate, messageId);
        //}, [](const auto &x, const auto &y) INLINE {
        //    if(std::get<0>(x) > std::get<0>(y)) return true;
        //    if(std::get<0>(x) < std::get<0>(y)) return false;
        //    return std::get<1>(x) < std::get<1>(y);
        //}, request.limit);

        //auto argument_3_1 = Argument(filter_1_1, [](const auto &tuple) INLINE
        //{
        //    const auto &[anon_7, anon_36, _friend] = tuple;
        //    return std::make_tuple(_friend);
        //});
        //auto expand_3_1 = ExpandAll(argument_3_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend] = tuple;
        //    return _friend;
        //}, txn, (label_t)snb::EdgeSchema::Person2Comment_creator);
        //auto anon_filter_3_1 = Filter(expand_3_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_61, _message] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(_message).data();
        //    return *type == snb::VertexSchema::Message;
        //});
        //auto anon_filter_cache_3_1 = CacheProperties(anon_filter_3_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_61, _message] = tuple;
        //    auto new_cache = std::make_tuple(*(uint64_t*)std::get<2>(anon_61).data());
        //    return std::tuple_cat(tuple, std::make_tuple(new_cache));
        //});
        //auto filter_3_1 = Filter(anon_filter_cache_3_1, [&request](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_61, _message, cache] = tuple;
        //    return (int64_t)std::get<0>(cache) <= request.maxDate;
        //});
        //auto projection_3_1 = Projection(filter_3_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_61, _message, cache] = tuple;
        //    auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
        //    return std::make_tuple(_friend, std::get<0>(cache), anon_61, message->id, _message);
        //});
        //auto partial_top_3 = PartialTop(projection_3_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, creationDate, anon_61, messageId, _message] = tuple;
        //    return std::make_tuple(creationDate);
        //}, [](const auto &tuple) INLINE {
        //    const auto &[_friend, creationDate, anon_61, messageId, _message] = tuple;
        //    return std::make_tuple(creationDate, messageId);
        //}, [](const auto &x, const auto &y) INLINE {
        //    if(std::get<0>(x) > std::get<0>(y)) return true;
        //    if(std::get<0>(x) < std::get<0>(y)) return false;
        //    return std::get<1>(x) < std::get<1>(y);
        //}, request.limit);

        //auto union_2 = Union(partial_top_2, partial_top_3);

        auto apply_1 = Apply(filter_1_1, projection_2_1, [](const auto &left, const auto &right) INLINE {
            const auto &[anon_7, anon_36, _friend] = left;
            const auto &[_friend_r, creationDate, anon_61, messageId, _message] = right;
            return std::make_tuple(_friend, creationDate, anon_36, anon_7, anon_61, messageId, _message);
        });

        auto top = Top(apply_1, [](const auto &tuple) INLINE {
            const auto &[_friend, creationDate, anon_36, anon_7, anon_61, messageId, _message] = tuple;
            return std::make_tuple(creationDate, messageId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);

        auto projection_result = Projection(top, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, creationDate, anon_36, anon_7, anon_61, messageId, _message] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(person->id, 
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    messageId,
                    message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen()),
                    creationDate);
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[id, firstName, lastName, messageId, messageContent, messageCreationDate] = tuple;
            _return.emplace_back();
            _return.back().personId = id;
            _return.back().personFirstName = firstName;
            _return.back().personLastName = lastName;
            _return.back().messageId = messageId;
            _return.back().messageContent = messageContent;
            _return.back().messageCreationDate = messageCreationDate;
        });
        
        plan();
    }

    void query3(std::vector<Query3Response> & _return, const Query3Request& request)
    {
        _return.clear();

        int64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto var_length_expand_1 = VarLengthExpandAll(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 1, 2);
        auto anon_filter_1_1 = Filter(var_length_expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto filter_1_1 = Filter(anon_filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            return _person != _friend;
        });
        auto distinct_1 = Distinct(filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            return std::make_tuple(_friend);
        });

        auto argument_a1_1 = Argument(distinct_1, [](const auto &tuple) INLINE
        {
            const auto &[_friend] = tuple;
            return std::make_tuple(_friend);
        });
        //auto expand_a1_1 = ExpandAll(argument_a1_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend] = tuple;
        //    return _friend;
        //}, txn, (label_t)snb::EdgeSchema::Person2Place);
        //auto expand_a1_2 = ExpandAll(expand_a1_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_111, _place] = tuple;
        //    return _place;
        //}, txn, (label_t)snb::EdgeSchema::Place2Place_up);
        auto expand_a1_1 = Projection(argument_a1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(_friend, 0, person->place);
        });
        auto expand_a1_2 = Projection(expand_a1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_111, _place] = tuple;
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(_place).data();
            return std::make_tuple(_friend, anon_111, _place, 0, place->isPartOf);
        });
        auto filter_a1_1 = Filter(expand_a1_2, [&txn, &request](const auto &tuple) INLINE {
            const auto &[_friend, anon_111, _place, anon_112, country] = tuple;
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(country).data();
            return std::string_view(place->name(), place->nameLen()) == request.countryXName || 
                std::string_view(place->name(), place->nameLen()) == request.countryYName;
        });
        auto anti_semi_apply_a1 = AntiSemiApply(distinct_1, filter_a1_1);
        
        //auto expand_1_1 = ExpandAll(anti_semi_apply_a1, [](const auto &tuple) INLINE {
        //    const auto &[_friend] = tuple;
        //    return _friend;
        //}, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator});
        //auto anon_filter_1_2 = Filter(expand_1_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(messageX).data();
        //    return *type == snb::VertexSchema::Message;
        //});
        //auto anon_filter_cache_1_2 = CacheProperties(anon_filter_1_2, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX] = tuple;
        //    auto message = (snb::MessageSchema::Message*)txn.get_vertex(messageX).data();
        //    auto new_cache = std::make_tuple(message->creationDate);
        //    return std::tuple_cat(tuple, std::make_tuple(new_cache));
        //});
        //auto filter_1_2 = Filter(anon_filter_cache_1_2, [&request, &endDate](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache] = tuple;
        //    return (int64_t)std::get<0>(cache) >= request.startDate && (int64_t)std::get<0>(cache) < endDate;
        //});
        //
        //auto expand_1_2 = ExpandAll(filter_1_2, [](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache] = tuple;
        //    return messageX;
        //}, txn, (label_t)snb::EdgeSchema::Message2Place);

        auto expand_1_1 = ExpandAllRange(anti_semi_apply_a1, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;
        }, [&request](const auto &edge_data) INLINE {
            return (int64_t)*(uint64_t*)edge_data.data() >= request.startDate;
        }, [&endDate](const auto &edge_data) INLINE {
            return (int64_t)*(uint64_t*)edge_data.data() < endDate;
        }, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator});
        auto anon_filter_1_2 = Filter(expand_1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(messageX).data();
            return *type == snb::VertexSchema::Message;
        });
        auto anon_filter_cache_1_2 = CacheProperties(anon_filter_1_2, [](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX] = tuple;
            auto new_cache = std::make_tuple((int64_t)*(uint64_t*)std::get<2>(anon_63).data());
            return std::tuple_cat(tuple, std::make_tuple(new_cache));
        });
        auto filter_1_2 = Filter(anon_filter_cache_1_2, [&request, &endDate](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX, cache] = tuple;
            return true;
            //return (int64_t)std::get<0>(cache) >= request.startDate && (int64_t)std::get<0>(cache) < endDate;
        });
        //auto expand_1_2 = ExpandAll(filter_1_2, [](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache] = tuple;
        //    return messageX;
        //}, txn, (label_t)snb::EdgeSchema::Message2Place);
        auto expand_1_2 = Projection(filter_1_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX, cache] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(messageX).data();
            return std::make_tuple(_friend, anon_63, messageX, cache, 0, message->place);
        });
        
        auto anon_filter_1_3 = Filter(expand_1_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(countryX).data();
            return *type == snb::VertexSchema::Place;
        });
        //auto filter_1_3 = Filter(anon_filter_1_3, [&txn, &request](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        //    auto place = (snb::PlaceSchema::Place*)txn.get_vertex(countryX).data();
        //    return std::string_view(place->name(), place->nameLen()) == request.countryXName || 
        //        std::string_view(place->name(), place->nameLen()) == request.countryYName;
        //});
        auto eager_aggragation_1 = EagerAggragation(anon_filter_1_3, [](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
            return std::make_tuple(_friend);
        }, [&txn, &request](const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(countryX).data();
            return std::make_tuple(1ul * (uint64_t)(std::string_view(place->name(), place->nameLen()) == request.countryXName),  
                1ul * (uint64_t)(std::string_view(place->name(), place->nameLen()) == request.countryYName));
        }, [&txn, &request](auto &ret, const auto &tuple) INLINE {
            const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(countryX).data();
            auto &[countX, countY] = ret;
            countX += 1ul * (uint64_t)(std::string_view(place->name(), place->nameLen()) == request.countryXName);
            countY += 1ul * (uint64_t)(std::string_view(place->name(), place->nameLen()) == request.countryYName);
        }, [](const auto &ret) INLINE {
            return ret;
        });
        
        ////auto argument_2 = Argument(expand_1_2, [](const auto &tuple) INLINE
        ////{
        ////    const auto &[anon_7, anon_36, _friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        ////    return std::make_tuple(_friend, countryX);
        ////});
        ////auto expand_2_1 = ExpandAll(argument_2, [](const auto &tuple) INLINE {
        ////    const auto &[_friend, countryX] = tuple;
        ////    return countryX;
        ////}, txn, (label_t)snb::EdgeSchema::Place2Place_down);
        ////auto expand_2_2 = ExpandInto(expand_2_1, [](const auto &tuple) INLINE {
        ////    const auto &[_friend, countryX, anon_208, anon_206] = tuple;
        ////    return std::make_tuple(_friend, anon_206);
        ////}, txn, (label_t)snb::EdgeSchema::Person2Place);

        ////auto anti_semi_apply_1 = AntiSemiApply(expand_1_2, expand_2_2);
        //
        //auto anon_filter_1_3 = Filter(expand_1_2, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(countryX).data();
        //    return *type == snb::VertexSchema::Place;
        //});
        //auto filter_1_3 = Filter(anon_filter_1_3, [&txn, &request](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        //    auto place = (snb::PlaceSchema::Place*)txn.get_vertex(countryX).data();
        //    return std::string_view(place->name(), place->nameLen()) == request.countryXName;
        //});
        //auto eager_aggragation_1 = EagerAggragation(filter_1_3, [](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        //    return std::make_tuple(_friend);
        //}, [](const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        //    return 1ul;
        //}, [](auto &ret, const auto &tuple) INLINE {
        //    const auto &[_friend, anon_63, messageX, cache, anon_110, countryX] = tuple;
        //    ret += 1ul;
        //}, [](const auto &ret) INLINE {
        //    return std::make_tuple(ret);
        //});

        //auto argument_3 = Argument(eager_aggragation_1, [](const auto &tuple) INLINE
        //{
        //    const auto &[_friend, xCount] = tuple;
        //    return std::make_tuple(_friend, xCount);
        //});
        //auto expand_3_1 = ExpandAll(argument_3, [](const auto &tuple) INLINE {
        //    const auto &[_friend, xCount] = tuple;
        //    return _friend;
        //}, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator});
        //auto anon_filter_3_1 = Filter(expand_3_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, xCount, anon_433, messageY] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(messageY).data();
        //    return *type == snb::VertexSchema::Message;
        //});
        //auto anon_filter_cache_3_1 = CacheProperties(anon_filter_3_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, xCount, anon_433, messageY] = tuple;
        //    auto message = (snb::MessageSchema::Message*)txn.get_vertex(messageY).data();
        //    auto new_cache = std::make_tuple(message->creationDate);
        //    return std::tuple_cat(tuple, std::make_tuple(new_cache));
        //});
        //auto filter_3_1 = Filter(anon_filter_cache_3_1, [&request, &endDate](const auto &tuple) INLINE {
        //    const auto &[_friend, xCount, anon_433, messageY, cache] = tuple;
        //    return (int64_t)std::get<0>(cache) >= request.startDate && (int64_t)std::get<0>(cache) < endDate;
        //});
        //auto expand_3_2 = ExpandAll(filter_3_1, [](const auto &tuple) INLINE {
        //    const auto &[_friend, xCount, anon_433, messageY, cache] = tuple;
        //    return messageY;
        //}, txn, (label_t)snb::EdgeSchema::Message2Place);

        ////auto argument_4 = Argument(expand_3_2, [](const auto &tuple) INLINE
        ////{
        ////    const auto &[_friend, xCount, anon_433, messageY, cache, anon_468, countryY] = tuple;
        ////    return std::make_tuple(_friend, countryY);
        ////});
        ////auto expand_4_1 = ExpandAll(argument_4, [](const auto &tuple) INLINE {
        ////    const auto &[_friend, countryY] = tuple;
        ////    return countryY;
        ////}, txn, (label_t)snb::EdgeSchema::Place2Place_down);
        ////auto expand_4_2 = ExpandInto(expand_4_1, [](const auto &tuple) INLINE {
        ////    const auto &[_friend, countryY, anon_575, anon_573] = tuple;
        ////    return std::make_tuple(_friend, anon_573);
        ////}, txn, (label_t)snb::EdgeSchema::Person2Place);

        ////auto anti_semi_apply_2 = AntiSemiApply(expand_3_2, expand_4_2);

        //auto apply_1 = Apply(eager_aggragation_1, expand_3_2, [](const auto &left, const auto &right) INLINE {
        //    const auto &[_friend, xCount] = left;
        //    const auto &[_friend_r, xCount_r, anon_433, messageY, cache, anon_468, countryY] = right;
        //    return std::make_tuple(anon_468, _friend, anon_433, xCount, countryY, messageY);

        //});

        //auto anon_filter_1_4 = Filter(apply_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[anon_468, _friend, anon_433, xCount, countryY, messageY] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(countryY).data();
        //    return *type == snb::VertexSchema::Place;
        //});
        //auto filter_1_4 = Filter(anon_filter_1_4, [&txn, &request](const auto &tuple) INLINE {
        //    const auto &[anon_468, _friend, anon_433, xCount, countryY, messageY] = tuple;
        //    auto place = (snb::PlaceSchema::Place*)txn.get_vertex(countryY).data();
        //    return std::string_view(place->name(), place->nameLen()) == request.countryYName;
        //});
        //auto eager_aggragation_2 = EagerAggragation(filter_1_4, [&txn](const auto &tuple) INLINE {
        //    const auto &[anon_468, _friend, anon_433, xCount, countryY, messageY] = tuple;
        //    auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
        //    return std::make_tuple(person->id,
        //            std::string_view(person->firstName(), person->firstNameLen()),
        //            std::string_view(person->lastName(), person->lastNameLen()),
        //            xCount);
        //}, [](const auto &tuple) INLINE {
        //    const auto &[anon_468, _friend, anon_433, xCount, countryY, messageY] = tuple;
        //    return 1ul;
        //}, [](auto &ret, const auto &tuple) INLINE {
        //    const auto &[anon_468, _friend, anon_433, xCount, countryY, messageY] = tuple;
        //    ret += 1ul;
        //}, [](const auto &ret) INLINE {
        //    return std::make_tuple(ret);
        //});

        auto filter_0 = Filter(eager_aggragation_1, [](const auto &tuple) INLINE {
            const auto &[_friend, xCount, yCount] = tuple;
            return xCount > 0 && yCount > 0;
        });
        auto projection_0 = Projection(filter_0, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, xCount, yCount] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(person->id,
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    xCount, yCount);
        });
        
        auto projection_1 = Projection(projection_0, [](const auto &tuple) INLINE {
            const auto &[friendId, firstName, lastName, xCount, yCount] = tuple;
            return std::make_tuple(xCount+yCount, friendId, firstName, lastName, xCount, yCount);
        });
        auto top = Top(projection_1, [](const auto &tuple) INLINE {
            const auto &[count, friendId, firstName, lastName, xCount, yCount] = tuple;
            return std::make_tuple(xCount, friendId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);
        auto projection_result = Projection(top, [](const auto &tuple) INLINE {
            const auto &[count, friendId, firstName, lastName, xCount, yCount] = tuple;
            return std::make_tuple(friendId, firstName, lastName, xCount, yCount, count);
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[friendId, firstName, lastName, xCount, yCount, count] = tuple;
            _return.emplace_back();
            _return.back().personId = friendId;
            _return.back().personFirstName = firstName;
            _return.back().personLastName = lastName;
            _return.back().xCount = xCount;
            _return.back().yCount = yCount;
            _return.back().count = count;
        });

        plan();
    }

    void query4(std::vector<Query4Response> & _return, const Query4Request& request)
    {
        _return.clear();

        int64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto expand_1_1 = ExpandAll(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);
        auto filter_1_1 = Filter(expand_1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(anon_52).data();
            return *type == snb::VertexSchema::Person;
        });
        auto expand_1_2 = ExpandAll(filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52] = tuple;
            return anon_52;
        }, txn, (label_t)snb::EdgeSchema::Person2Post_creator);
        auto anon_filter_1_2 = Filter(expand_1_2, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post] = tuple;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(post).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Post;
        });
        auto anon_filter_cache_1_2 = CacheProperties(anon_filter_1_2, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post] = tuple;
            //auto message = (snb::MessageSchema::Message*)txn.get_vertex(post).data();
            auto new_cache = std::make_tuple((int64_t)*(uint64_t*)(std::get<2>(anon_61)).data());
            return std::tuple_cat(tuple, std::make_tuple(new_cache));
        });
        auto filter_1_2 = Filter(anon_filter_cache_1_2, [&endDate](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post, cache] = tuple;
            return (int64_t)std::get<0>(cache) < endDate;
        });
        auto expand_1_3 = ExpandAll(filter_1_2, [](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post, cache] = tuple;
            return post;
        }, txn, (label_t)snb::EdgeSchema::Post2Tag);
        auto filter_1_3 = Filter(expand_1_3, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post, cache, anon_89, _tag] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_tag).data();
            return *type == snb::VertexSchema::Tag;
        });
        auto eager_aggragation_1 = EagerAggragation(filter_1_3, [](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post, cache, anon_89, _tag] = tuple;
            return std::make_tuple(_tag);
        }, [&request](const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post, cache, anon_89, _tag] = tuple;
            uint64_t postsOnTag = (int64_t)std::get<0>(cache) >= request.startDate;
            uint64_t oldPosts = (int64_t)std::get<0>(cache) < request.startDate;
            return std::make_tuple(postsOnTag, oldPosts);
        }, [&request](auto &ret, const auto &tuple) INLINE {
            const auto &[_person, anon_42, anon_52, anon_61, post, cache, anon_89, _tag] = tuple;
            auto &[postsOnTag, oldPosts] = ret;
            postsOnTag += (int64_t)std::get<0>(cache) >= request.startDate;
            oldPosts += (int64_t)std::get<0>(cache) < request.startDate;
        }, [](const auto &ret) INLINE {
            return ret;
        });
        auto filter_1_4 = Filter(eager_aggragation_1, [](const auto &tuple) INLINE {
            const auto &[_tag, postsOnTag, oldPosts] = tuple;
            return oldPosts == 0;
        });
        auto projection_1_1 = Projection(filter_1_4, [&txn](const auto &tuple) INLINE {
            const auto &[_tag, postsOnTag, oldPosts] = tuple;
            auto tag = (snb::TagSchema::Tag*)txn.get_vertex(_tag).data();
            return std::make_tuple(std::string_view(tag->name(), tag->nameLen()), postsOnTag);
        });
        auto top = Top(projection_1_1, [](const auto &tuple) INLINE {
            const auto &[tagName, postCount] = tuple;
            return std::make_tuple(postCount, tagName);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);

        auto plan = top.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[tagName, postCount] = tuple;
            _return.emplace_back();
            _return.back().tagName = tagName;
            _return.back().postCount = postCount;
        });

        plan();
    }

    void query5(std::vector<Query5Response> & _return, const Query5Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto var_length_expand_1 = VarLengthExpandPruning(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 1, 2);
        auto anon_filter_1_1 = Filter(var_length_expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto filter_1_1 = Filter(anon_filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            return _person != _friend;
        });
        //auto expand_1_1 = ExpandAll(filter_1_1, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend] = tuple;
        //    return _friend;
        //}, txn, (label_t)snb::EdgeSchema::Person2Forum_member);
        auto expand_1_1 = ExpandAllRange(filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            return _friend;
        }, [&request](const auto &edge_data) INLINE {
            return (int64_t)*(uint64_t*)edge_data.data() > request.minDate;
        }, [](const auto &edge_data) INLINE {
            return true;
        }, txn, (label_t)snb::EdgeSchema::Person2Forum_member);
        auto anon_filter_1_2 = Filter(expand_1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend, membership, _forum] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_forum).data();
            return *type == snb::VertexSchema::Forum;
        });
        auto filter_1_2 = Filter(anon_filter_1_2, [&request](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend, membership, _forum] = tuple;
            return true;
            //return *(int64_t*)std::get<2>(membership).data() > request.minDate;
        });
        auto projection_1_1 = Projection(filter_1_2, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend, membership, _forum] = tuple;
            return std::make_tuple(_person, anon_33, _friend, membership, _forum, *((uint64_t*)(std::get<2>(membership).data()+sizeof(uint64_t))));
        });

        //auto distinct = Distinct(projection_1_1, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend, membership, _forum, postCount] = tuple;
        //    return std::make_tuple(_friend, _forum, postCount);
        //});

        auto eager_aggragation_1 = EagerAggragation(projection_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend, membership, _forum, postCount] = tuple;
            return std::make_tuple(_forum);
        }, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend, membership, _forum, postCount] = tuple;
            return postCount;
        }, [](auto &ret, const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend, membership, _forum, postCount] = tuple;
            ret += postCount;
        }, [](const auto &ret) INLINE {
            return std::make_tuple(ret);
        });
        auto projection_1_2 = Projection(eager_aggragation_1, [&txn](const auto &tuple) INLINE {
            const auto &[_forum, postCount] = tuple;
            auto forum = (snb::ForumSchema::Forum*)txn.get_vertex(_forum).data();
            return std::make_tuple(_forum, postCount, forum->id);
        });
        
        //auto projection_1_2 = Projection(projection_1_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend, membership, _forum, postCount] = tuple;
        //    auto forum = (snb::ForumSchema::Forum*)txn.get_vertex(_forum).data();
        //    return std::make_tuple(_forum, postCount, forum->id);
        //});
        auto top = Top(projection_1_2, [](const auto &tuple) INLINE {
            const auto &[_forum, postCount, forumId] = tuple;
            return std::make_tuple(postCount, forumId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);
        auto projection_result = Projection(top, [&txn](const auto &tuple) INLINE {
            const auto &[_forum, postCount, forumId] = tuple;
            auto forum = (snb::ForumSchema::Forum*)txn.get_vertex(_forum).data();
            return std::make_tuple(std::string_view(forum->title(), forum->titleLen()), postCount);
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[forumTitle, postCount] = tuple;
            _return.emplace_back();
            _return.back().forumTitle = forumTitle;
            _return.back().postCount = postCount;
        });

        plan();
    }

    void query6(std::vector<Query6Response> & _return, const Query6Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto var_length_expand_1 = VarLengthExpandAll(node_index_seek_1, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 1, 2);
        auto anon_filter_1_1 = Filter(var_length_expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto filter_1_1 = Filter(anon_filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _friend] = tuple;
            return _person != _friend;
        });
        auto projection_1_1 = Projection(filter_1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _friend] = tuple;
            return std::make_tuple(_friend);
        });

        NodeIndexSeek node_index_seek_2(tagSchema.db, tagSchema.nameindex, request.tagName);
        auto expand_2_1 = ExpandAll(node_index_seek_2, [](const auto &tuple) INLINE {
            const auto &[knownTag] = tuple;
            return knownTag;
        }, txn, (label_t)snb::EdgeSchema::Tag2Post);
        auto filter_2_1 = Filter(expand_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[knownTag, anon_115, friendPost] = tuple;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(friendPost).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Post;
        });
        //auto expand_2_2 = ExpandAll(filter_2_1, [](const auto &tuple) INLINE {
        //    const auto &[knownTag, anon_115, friendPost] = tuple;
        //    return friendPost;
        //}, txn, (label_t)snb::EdgeSchema::Message2Person_creator);
        auto expand_2_2 = Projection(filter_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[knownTag, anon_115, friendPost] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(friendPost).data();
            return std::make_tuple(knownTag, anon_115, friendPost, 0, message->creator);
        });

        auto hash_join = SortJoin(projection_1_1, expand_2_2, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;
        }, [](const auto &tuple) INLINE {
            const auto &[knownTag, anon_115, friendPost, anon_81, _friend] = tuple;
            return _friend;
        }, false, true);
        auto expand_1_2 = ExpandAll(hash_join, [](const auto &tuple) INLINE {
            const auto &[_friend, knownTag, anon_115, friendPost, anon_81, __friend] = tuple;
            return friendPost;
        }, txn, (label_t)snb::EdgeSchema::Post2Tag);
        auto anon_filter_1_2 = Filter(expand_1_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, knownTag, anon_115, friendPost, anon_81, __friend, anon_214, commonTag] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(commonTag).data();
            return *type == snb::VertexSchema::Tag;
        });
        auto filter_1_2 = Filter(anon_filter_1_2, [](const auto &tuple) INLINE {
            const auto &[_friend, knownTag, anon_115, friendPost, anon_81, __friend, anon_214, commonTag] = tuple;
            return knownTag != commonTag;
        });
        auto eager_aggragation_1 = EagerAggragation(filter_1_2, [](const auto &tuple) INLINE {
            const auto &[_friend, knownTag, anon_115, friendPost, anon_81, __friend, anon_214, commonTag] = tuple;
            return std::make_tuple(commonTag);
        }, [](const auto &tuple) INLINE {
            const auto &[_friend, knownTag, anon_115, friendPost, anon_81, __friend, anon_214, commonTag] = tuple;
            return 1lu;
        }, [](auto &ret, const auto &tuple) INLINE {
            const auto &[_friend, knownTag, anon_115, friendPost, anon_81, __friend, anon_214, commonTag] = tuple;
            ret += 1lu;
        }, [](const auto &ret) INLINE {
            return std::make_tuple(ret);
        });
        auto projection_1 = Projection(eager_aggragation_1, [&txn](const auto &tuple) INLINE {
            const auto &[commonTag, postCount] = tuple;
            auto tag = (snb::TagSchema::Tag*)txn.get_vertex(commonTag).data();
            return std::make_tuple(std::string_view(tag->name(), tag->nameLen()), postCount);
        });
        auto top = Top(projection_1, [](const auto &tuple) INLINE {
            const auto &[tagName, postCount] = tuple;
            return std::make_tuple(postCount, tagName);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);

        auto plan = top.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[tagName, postCount] = tuple;
            _return.emplace_back();
            _return.back().tagName = tagName;
            _return.back().postCount = postCount;
        });

        plan();
    }

    void query7(std::vector<Query7Response> & _return, const Query7Request& request)
    {
        _return.clear();
        
        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto expand_1 = ExpandAll(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator});
        auto filter_1 = Filter(expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_message).data();
            return *type == snb::VertexSchema::Message;
        });
        auto expand_2 = ExpandAllFilterLooseLimit(filter_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message] = tuple;
            return _message;
        }, [](const auto &edge_data) INLINE {
            return true;
        }, txn, {(label_t)snb::EdgeSchema::Post2Person_like, (label_t)snb::EdgeSchema::Comment2Person_like}, request.limit);
        auto filter_2 = Filter(expand_2, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message, like, liker] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(liker).data();
            return *type == snb::VertexSchema::Person;
        });
        auto projection_1 = Projection(filter_2, [](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message, like, liker] = tuple;
            return std::tuple_cat(tuple, std::make_tuple(*(uint64_t*)std::get<2>(like).data()));
        });
        auto projection_2 = Projection(projection_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message, like, liker, likeTime] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
            return std::tuple_cat(tuple, std::make_tuple(message->id));
        });
        //auto sort = Sort(projection_2, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_41, _message, like, liker, likeTime, messageId] = tuple;
        //    return std::make_tuple(likeTime, messageId);
        //}, [](const auto &x, const auto &y) INLINE {
        //    if(std::get<0>(x) > std::get<0>(y)) return true;
        //    if(std::get<0>(x) < std::get<0>(y)) return false;
        //    return std::get<1>(x) < std::get<1>(y);
        //});
        auto eager_aggragation = EagerAggragation(projection_2, [](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message, like, liker, likeTime, messageId] = tuple;
            return std::make_tuple(liker, _person);
        }, [](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message, like, liker, likeTime, messageId] = tuple;
            auto t = std::make_tuple(_message, likeTime);
            return t;
            //std::vector<std::decay_t<decltype(t)>> store;
            //store.emplace_back(t);
            //return store;
        }, [](auto &ret, const auto &tuple) INLINE {
            const auto &[_person, anon_41, _message, like, liker, likeTime, messageId] = tuple;
            auto t = std::make_tuple(_message, likeTime);
            if(likeTime > std::get<1>(ret))
            {
                ret = t;
            }
            //ret.emplace_back(t);
        }, [](const auto &ret) INLINE {
            return std::make_tuple(ret);
        });
        auto projection_3 = Projection(eager_aggragation, [](const auto &tuple) INLINE {
            const auto &[liker, _person, anon_222] = tuple;
            return std::tuple_cat(tuple, std::make_tuple(anon_222));
        });
        auto projection_4 = Projection(projection_3, [&txn](const auto &tuple) INLINE {
            const auto &[liker, _person, anon_222, latestLike] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(liker).data();
            return std::tuple_cat(tuple, std::make_tuple(std::get<1>(latestLike), person->id));
        });
        auto top = Top(projection_4, [](const auto &tuple) INLINE {
            const auto &[liker, _person, anon_222, latestLike, likeCreationTime, personId] = tuple;
            return std::make_tuple(likeCreationTime, personId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);

        auto argument = Argument(top, [](const auto &tuple) INLINE {
            const auto &[liker, _person, anon_222, latestLike, likeCreationTime, personId] = tuple;
            return std::make_tuple(liker, _person);
        });
        auto expand_3 = ExpandInto(argument, [](const auto &tuple) INLINE {
            const auto &[liker, _person] = tuple;
            return std::make_tuple(liker, _person);
        }, txn, (label_t)snb::EdgeSchema::Person2Person);
        auto optional = Optional(expand_3);

        auto apply = Apply(top, optional, [](const auto &left, const auto &right) INLINE {
            const auto &[liker, _person, anon_222, latestLike, likeCreationTime, personId] = left;
            const auto &[liker_r, _person_r, know] = right;
            return std::make_tuple(liker, _person, anon_222, latestLike, likeCreationTime, personId, std::get<0>(know) == 0 && std::get<1>(know) == 0);
        });
        
        auto projection_result = Projection(apply, [&txn](const auto &tuple) INLINE {
            const auto &[liker, _person, anon_222, latestLike, likeCreationTime, personId, isNew] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(liker).data();
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(std::get<0>(latestLike)).data();
            return std::make_tuple(personId,
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    likeCreationTime,
                    message->id,
                    message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen()),
                    message->creationDate,
                    isNew);
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[personId, firstName, lastName, likeCreationTime, messageId, messageContent, messageCreationDate, isNew] = tuple;
            _return.emplace_back();
            _return.back().personId = personId;
            _return.back().personFirstName = firstName;
            _return.back().personLastName = lastName;
            _return.back().likeCreationDate = likeCreationTime;
            _return.back().commentOrPostId = messageId;
            _return.back().commentOrPostContent = messageContent;
            _return.back().minutesLatency = (likeCreationTime-messageCreationDate)/(60lu*1000lu);
            _return.back().isNew = isNew;
        });

        plan();
    }

    void query8(std::vector<Query8Response> & _return, const Query8Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto expand_1 = ExpandAll(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[start] = tuple;
            return start;
        }, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator});
        auto filter_1 = Filter(expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(anon_58).data();
            return *type == snb::VertexSchema::Message;
        });
        auto expand_2 = ExpandAll(filter_1, [](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58] = tuple;
            return anon_58;
        }, txn, (label_t)snb::EdgeSchema::Message2Message_down);
        auto filter_2 = Filter(expand_2, [&txn](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment] = tuple;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Comment;
        });
        auto cache_1 = CacheProperties(filter_2, [&txn](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            auto new_cache = std::make_tuple(message->creationDate, message->id);
            return std::tuple_cat(tuple, std::make_tuple(new_cache));
        });
        auto projection_1 = Projection(cache_1, [](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment, cache] = tuple;
            return std::tuple_cat(tuple, std::make_tuple(std::get<0>(cache), std::get<1>(cache)));
        });
        auto sort = Top(projection_1, [](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId] = tuple;
            return std::make_tuple(commentCreationDate, commentId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);
        //auto expand_3 = ExpandAll(sort, [](const auto &tuple) INLINE {
        //    const auto &[start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId] = tuple;
        //    return comment;
        //}, txn, (label_t)snb::EdgeSchema::Message2Person_creator);
        auto expand_3 = Projection(sort, [&txn](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            return std::make_tuple(start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId, std::make_tuple(comment, message->creator, to_string<uint64_t>(message->creationDate)), message->creator);
        });
        auto anon_filter_3 = Filter(expand_3, [&txn](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId, anon_99, _person] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_person).data();
            return *type == snb::VertexSchema::Person;
        });
        auto filter_3 = Filter(anon_filter_3, [](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId, anon_99, _person] = tuple;
            return !(std::get<0>(anon_41) == std::get<1>(anon_99) && std::get<1>(anon_41) == std::get<0>(anon_99) && std::get<2>(anon_41) == std::get<2>(anon_99));
        });
        auto limit = Limit(filter_3, request.limit);

        auto projection_result = Projection(limit, [&txn](const auto &tuple) INLINE {
            const auto &[start, anon_41, anon_58, anon_68, comment, cache, commentCreationDate, commentId, anon_99, _person] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_person).data();
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            return std::make_tuple(person->id,
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    message->creationDate,
                    message->id,
                    std::string_view(message->content(), message->contentLen()));
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[personId, firstName, lastName, messageCreationDate, messageId, messageContent] = tuple;
            _return.emplace_back();
            _return.back().personId = personId;
            _return.back().personFirstName = firstName;
            _return.back().personLastName = lastName;
            _return.back().commentId = messageId;
            _return.back().commentContent = messageContent;
            _return.back().commentCreationDate = messageCreationDate;
        });

        plan();
    }

    void query9(std::vector<Query9Response> & _return, const Query9Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto var_length_expand_1 = VarLengthExpandPruning(node_index_seek, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 1, 2);
        auto filter_1 = Filter(var_length_expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_7, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto expand_2 = ExpandAllFilterLooseLimit(filter_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_7, _friend] = tuple;
            return _friend;
        }, [&request](const auto &edge_data) INLINE {
            return (int64_t)*(uint64_t*)edge_data.data() < request.maxDate;
        }, txn, {(label_t)snb::EdgeSchema::Person2Post_creator, (label_t)snb::EdgeSchema::Person2Comment_creator}, request.limit);
        auto anon_top_2 = LooseTop(expand_2, [](const auto &tuple) INLINE {
            const auto &[_person, anon_7, _friend, anon_65, _message] = tuple;
            return std::make_tuple(*(uint64_t*)std::get<2>(anon_65).data());
        }, [](const auto &x, const auto &y) INLINE {
            return std::get<0>(x) > std::get<0>(y);
        }, request.limit);
        auto anon_filter_2 = Filter(anon_top_2, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_7, _friend, anon_65, _message] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_message).data();
            return *type == snb::VertexSchema::Message;
        });
        auto anon_filter_cache_2 = CacheProperties(anon_filter_2, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_7, _friend, anon_65, _message] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(_message).data();
            auto new_cache = std::make_tuple(message->id, message->creationDate, message->contentLen()?std::string_view(message->content(), message->contentLen()):std::string_view(message->imageFile(), message->imageFileLen()));
            return std::make_tuple(_person, anon_7, _friend, anon_65, _message, new_cache);
        });
        auto filter_2 = Filter(anon_filter_cache_2, [&request](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, _message, cache] = tuple;
            const auto &[messageId, messageCreationDate, messageContent] = cache;
            return true;
            //return (int64_t)messageCreationDate < request.maxDate;
        });
        auto top = Top(filter_2, [](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, _message, cache] = tuple;
            const auto &[messageId, messageCreationDate, messageContent] = cache;
            return std::make_tuple(messageCreationDate, messageId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);
        auto projection = Projection(top, [&txn](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, _message, cache] = tuple;
            const auto &[messageId, messageCreationDate, messageContent] = cache;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(person->id, 
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    messageId,
                    messageContent,
                    messageCreationDate);
        });

        auto plan = projection.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[personId, personFirstName, personLastName, messageId, messageContent, messageCreationDate] = tuple;
            _return.emplace_back();
            _return.back().personId = personId;
            _return.back().personFirstName = personFirstName;
            _return.back().personLastName = personLastName;
            _return.back().messageId = messageId;
            _return.back().messageContent = messageContent;
            _return.back().messageCreationDate = messageCreationDate;
        });

        plan();
    }

    void query10(std::vector<Query10Response> & _return, const Query10Request& request)
    {
        _return.clear();

        auto month = request.month;
        auto nextMonth = month%12 + 1;

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto var_length_expand_1 = VarLengthExpandAll(node_index_seek_1, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 2, 2);
        auto anon_filter_1_1 = Filter(var_length_expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto filter_1_1 = Filter(anon_filter_1_1, [&txn, &month, &nextMonth](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _friend] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            auto monday = to_monday(person->birthday);
            return _person != _friend && ((monday.first==month && monday.second>=21) || (monday.first==nextMonth && monday.second<22));
        });

        auto argument_2 = Argument(filter_1_1, [](const auto &tuple) INLINE
        {
            const auto &[_person, anon_41, _friend] = tuple;
            return std::make_tuple(_person, _friend);
        });
        auto expand_2_1 = ExpandInto(argument_2, [](const auto &tuple) INLINE {
            const auto &[_person, _friend] = tuple;
            return tuple;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);

        auto anti_semi_apply_1 = AntiSemiApply(filter_1_1, expand_2_1);
        //auto expand_1_1 = ExpandAll(anti_semi_apply_1, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_41, _friend] = tuple;
        //    return _friend;
        //}, txn, (label_t)snb::EdgeSchema::Person2Place);
        auto distinct = Distinct(anti_semi_apply_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_41, _friend] = tuple;
            return std::make_tuple(_friend, _person);
        });
        auto optional_expand = OptionalExpandAll(distinct, [](const auto &tuple) INLINE {
            const auto &[_friend, _person] = tuple;
            return _friend;
        }, txn, (label_t)snb::EdgeSchema::Person2Post_creator);
        auto anon_optional_expand_filter = Filter(optional_expand, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, _person, anon_353, post] = tuple;
            if(anon_353 == path_type() && post == 0) return true;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(post).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Post;
        });

        auto argument_3 = Argument(anon_optional_expand_filter, [](const auto &tuple) INLINE
        {
            const auto &[_friend, _person, anon_353, post] = tuple;
            return std::make_tuple(_person, anon_353, post);
        });
        auto filter_3_1 = Filter(argument_3, [](const auto &tuple) INLINE {
            const auto &[_person, anon_353, post] = tuple;
            return !(anon_353 == path_type() && post == 0);
        });
        auto expand_3_1 = ExpandAll(filter_3_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_353, post] = tuple;
            return post;
        }, txn, (label_t)snb::EdgeSchema::Post2Tag);
        auto filter_3_2 = Filter(expand_3_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_353, post, hasTag, _tag] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_tag).data();
            return *type == snb::VertexSchema::Tag;
        });
        //auto expand_3_2 = ExpandInto(filter_3_2, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_353, post, hasTag, _tag] = tuple;
        //    return std::make_tuple(_person, _tag);
        //}, txn, (label_t)snb::EdgeSchema::Person2Tag);
        //auto limit = Limit(expand_3_2, 1);
        auto optional_3 = Optional(filter_3_2);

        auto apply_1 = Apply(anon_optional_expand_filter, optional_3, [](const auto &left, const auto &right) INLINE {
            const auto &[_friend, _person, anon_353, post] = left;
            //const auto &[_person_r, anon_353_r, post_r, hasTag, _tag, hasInterest] = right;
            //return std::make_tuple(_friend, _person, anon_353, post, hasTag, _tag, hasInterest);
            const auto &[_person_r, anon_353_r, post_r, hasTag, _tag] = right;
            return std::make_tuple(_friend, _person, anon_353, post, hasTag, _tag);
        });

        NodeIndexSeek node_index_seek_4(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto expand_4_1 = ExpandAll(node_index_seek_4, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Tag);
        auto anon_filter_4_1 = Filter(expand_4_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_55, _tag] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_tag).data();
            return *type == snb::VertexSchema::Tag;
        });

        auto hash_join = RightOuterSortJoin(anon_filter_4_1, apply_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_55, _tag] = tuple;
            return _tag;
        }, [](const auto &tuple) INLINE {
            const auto &[_friend, _person, anon_353, post, hasTag, _tag] = tuple;
            return _tag;
        });

        auto order_aggragation_1 = OrderedAggragation(hash_join, [](const auto &tuple) INLINE {
            const auto &[_person, anon_55, _tag, _friend, _person_r, anon_353, post, hasTag, _tag_r] = tuple;
            return std::make_tuple(_friend, anon_353, post);
        }, [](const auto &tuple) INLINE {
            const auto &[_person, anon_55, _tag, _friend, _person_r, anon_353, post, hasTag, _tag_r] = tuple;
            int64_t hasInterest = !(anon_353 == path_type() && post == 0) && !(hasTag == path_type() && _tag_r == 0) && (_tag == _tag_r);
            return hasInterest;
        }, [](auto &ret, const auto &tuple) INLINE {
            const auto &[_person, anon_55, _tag, _friend, _person_r, anon_353, post, hasTag, _tag_r] = tuple;
            int64_t hasInterest = !(anon_353 == path_type() && post == 0) && !(hasTag == path_type() && _tag_r == 0) && (_tag == _tag_r);
            ret += hasInterest;
        }, [](const auto &ret) INLINE {
            return std::make_tuple(ret);
        });

        auto order_aggragation_2 = OrderedAggragation(order_aggragation_1, [](const auto &tuple) INLINE {
            const auto &[_friend, anon_353, post, hasInterest] = tuple;
            return std::make_tuple(_friend);
        }, [](const auto &tuple) INLINE {
            const auto &[_friend, anon_353, post, hasInterest] = tuple;
            int64_t postCount = !(anon_353 == path_type() && post == 0);
            int64_t commonPostCount = (hasInterest > 0);
            return std::make_tuple(postCount, commonPostCount);
        }, [](auto &ret, const auto &tuple) INLINE {
            const auto &[_friend, anon_353, post, hasInterest] = tuple;
            auto &[postCount, commonPostCount] = ret;
            postCount += !(anon_353 == path_type() && post == 0);
            commonPostCount += (hasInterest > 0);
        }, [](const auto &ret) INLINE {
            return ret;
        });
        auto projection_1 = Projection(order_aggragation_2, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, postCount, commonPostCount] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(_friend, person->place, postCount, commonPostCount, commonPostCount-(postCount-commonPostCount), person->id);
        });
        auto top = Top(projection_1, [](const auto &tuple) INLINE {
            const auto &[_friend, city, postCount, commonPostCount, commonInterestScore, personId] = tuple;
            return std::make_tuple(commonInterestScore, personId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);

        auto projection_result = Projection(top, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, city, postCount, commonPostCount, commonInterestScore, personId] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            auto place = (snb::PlaceSchema::Place*)txn.get_vertex(city).data();
            
            return std::make_tuple(personId, 
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    commonInterestScore,
                    std::string_view(person->gender(), person->genderLen()), 
                    std::string_view(place->name(), place->nameLen()));
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[personId, personFirstName, personLastName, commonInterestScore, personGender, placeName] = tuple;
            _return.emplace_back();
            _return.back().personId = personId;
            _return.back().personFirstName = personFirstName;
            _return.back().personLastName = personLastName;
            _return.back().commonInterestSore = commonInterestScore;
            _return.back().personGender = personGender;
            _return.back().personCityName = placeName;
        });

        plan();
    }

    void query11(std::vector<Query11Response> & _return, const Query11Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        //NodeIndexSeek node_index_seek(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        //auto var_length_expand = VarLengthExpandPruning(node_index_seek, [](const auto &tuple) INLINE {
        //    const auto &[_person] = tuple;
        //    return _person;
        //}, txn, (label_t)snb::EdgeSchema::Person2Person, 1, 2);
        //auto anon_filter_1 = Filter(var_length_expand, [&txn](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
        //    return *type == snb::VertexSchema::Person;
        //});
        //auto filter_1 = Filter(anon_filter_1, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend] = tuple;
        //    return _person != _friend;
        //});
        //auto distinct = Distinct(filter_1, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend] = tuple;
        //    return std::make_tuple(_friend);
        //});
        ////auto expand_1 = ExpandAll(distinct, [](const auto &tuple) INLINE {
        ////    const auto &[_friend] = tuple;
        ////    return _friend;
        ////}, txn, (label_t)snb::EdgeSchema::Person2Org_work);
        //auto expand_1 = ExpandAllFilterLooseLimit(distinct, [](const auto &tuple) INLINE {
        //    const auto &[_friend] = tuple;
        //    return _friend;
        //}, [&request](const auto &edge_data) INLINE {
        //    return (int32_t)*(uint32_t*)edge_data.data() < request.workFromYear;
        //}, txn, (label_t)snb::EdgeSchema::Person2Org_work, request.limit);
        //auto anon_filter_2 = Filter(expand_1, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto org = (snb::OrgSchema::Org*)txn.get_vertex(company).data();
        //    return org->vtype == snb::VertexSchema::Org && org->type == snb::OrgSchema::Org::Type::Company;
        //});
        //auto anon_filter_cache_2 = CacheProperties(anon_filter_2, [](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company] = tuple;
        //    auto new_cache = std::make_tuple(*(int32_t*)std::get<2>(workAt).data());
        //    return std::tuple_cat(tuple, std::make_tuple(new_cache));
        //});
        //auto filter_2 = Filter(anon_filter_cache_2, [&request](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company, cache] = tuple;
        //    return true;
        //    //return std::get<0>(cache) < request.workFromYear;
        //});
        //auto expand_2 = ExpandAll(filter_2, [](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company, cache] = tuple;
        //    return company;
        //}, txn, (label_t)snb::EdgeSchema::Org2Place);
        //auto anon_filter_3 = Filter(expand_2, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company, cache, anon_173, anon_192] = tuple;
        //    if(!enableSchemaCheck) return true;
        //    auto type = (snb::VertexSchema*)txn.get_vertex(anon_192).data();
        //    return *type == snb::VertexSchema::Place;
        //});
        //auto filter_3 = Filter(anon_filter_3, [&txn, &request](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company, cache, anon_173, anon_192] = tuple;
        //    auto place = (snb::PlaceSchema::Place*)txn.get_vertex(anon_192).data();
        //    return std::string_view(place->name(), place->nameLen()) == request.countryName;
        //});
        //auto projection_1 = Projection(filter_3, [&txn](const auto &tuple) INLINE {
        //    const auto &[_friend, workAt, company, cache, anon_173, anon_192] = tuple;
        //    auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
        //    auto org = (snb::OrgSchema::Org*)txn.get_vertex(company).data();
        //    return std::make_tuple(person->id, _friend, anon_173, std::string_view(org->name(), org->nameLen()), 
        //            anon_192, company, std::get<0>(cache), workAt);
        //});
        //auto top = Top(projection_1, [](const auto &tuple) INLINE {
        //    const auto &[friendId, _friend, anon_173, orgName, anon_192, company, orgWorkFromYear, workAt] = tuple;
        //    return std::make_tuple(orgWorkFromYear, friendId, orgName);
        //}, [](const auto &x, const auto &y) INLINE {
        //    if(std::get<0>(x) < std::get<0>(y)) return true;
        //    if(std::get<0>(x) > std::get<0>(y)) return false;
        //    if(std::get<1>(x) < std::get<1>(y)) return true;
        //    if(std::get<1>(x) > std::get<1>(y)) return false;
        //    return std::get<2>(x) > std::get<2>(y);
        //}, request.limit);
        //auto projection_result = Projection(top, [&txn](const auto &tuple) INLINE {
        //    const auto &[friendId, _friend, anon_173, orgName, anon_192, company, orgWorkFromYear, workAt] = tuple;
        //    auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
        //    
        //    return std::make_tuple(friendId,
        //            std::string_view(person->firstName(), person->firstNameLen()),
        //            std::string_view(person->lastName(), person->lastNameLen()),
        //            orgName, orgWorkFromYear);
        //});

        //auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
        //    const auto &[personId, personFirstName, personLastName, orgName, orgWorkFromYear] = tuple;
        //    _return.emplace_back();
        //    _return.back().personId = personId;
        //    _return.back().personFirstName = personFirstName;
        //    _return.back().personLastName = personLastName;
        //    _return.back().organizationName = orgName;
        //    _return.back().organizationWorkFromYear = orgWorkFromYear;
        //});

        NodeIndexSeek node_index_seek_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto var_length_expand_1 = VarLengthExpandAll(node_index_seek_1, [](const auto &tuple) INLINE {
            const auto &[_person] = tuple;
            return _person;
        }, txn, (label_t)snb::EdgeSchema::Person2Person, 1, 2);

        auto anon_filter_1 = Filter(var_length_expand_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto filter_1 = Filter(anon_filter_1, [](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            return _person != _friend;
        });
        auto projection_1_1 = Projection(filter_1, [&txn](const auto &tuple) INLINE {
            const auto &[_person, anon_33, _friend] = tuple;
            return std::make_tuple(_friend);
        });

        NodeIndexSeek node_index_seek_2(placeSchema.db, placeSchema.nameindex, request.countryName);
        auto expand_2_1 = ExpandAll(node_index_seek_2, [](const auto &tuple) INLINE {
            const auto &[_country] = tuple;
            return _country;
        }, txn, (label_t)snb::EdgeSchema::Place2Org);
        auto anon_filter_2_1 = Filter(expand_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[_country, anon_111, _org] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_org).data();
            return *type == snb::VertexSchema::Org;
        });
        auto expand_2_2 = ExpandAll(anon_filter_2_1, [](const auto &tuple) INLINE {
            const auto &[_country, anon_111, _org] = tuple;
            return _org;
        }, txn, (label_t)snb::EdgeSchema::Org2Person_work);
        auto projection_2_1 = Projection(expand_2_2, [&txn](const auto &tuple) INLINE {
            const auto &[_country, anon_111, _org, workAt, _friend] = tuple;
            return std::make_tuple(_org, (int32_t)*(uint32_t*)std::get<2>(workAt).data(), _friend);
        });
        auto filter_2_1 = Filter(projection_2_1, [&request](const auto &tuple) INLINE {
            const auto &[_org, orgWorkFromYear, _friend] = tuple;
            return orgWorkFromYear < request.workFromYear;
        });

        auto hash_join = SortJoin(projection_1_1, filter_2_1, [](const auto &tuple) INLINE {
            const auto &[_friend] = tuple;
            return _friend;
        }, [](const auto &tuple) INLINE {
            const auto &[_org, orgWorkFromYear, _friend] = tuple;
            return _friend;
        }, false, true);

        auto loose_top = LooseTop(hash_join, [](const auto &tuple) INLINE {
            const auto &[_friend_r, _org, orgWorkFromYear, _friend] = tuple;
            return std::make_tuple(orgWorkFromYear);
        }, [](const auto &x, const auto &y) INLINE {
            return (std::get<0>(x) < std::get<0>(y));
        }, request.limit);

        auto projection_1 = Projection(loose_top, [&txn](const auto &tuple) INLINE {
            const auto &[_friend_r, _org, orgWorkFromYear, _friend] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            auto org = (snb::OrgSchema::Org*)txn.get_vertex(_org).data();
            return std::make_tuple(person->id, _friend, std::string_view(org->name(), org->nameLen()), 
                    _org, orgWorkFromYear);
        });

        //auto hash_join = SortJoin(filter_1, filter_2_1, [](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend] = tuple;
        //    return _friend;
        //}, [](const auto &tuple) INLINE {
        //    const auto &[_country, anon_111, _org, workAt, _friend] = tuple;
        //    return _friend;
        //});

        //auto projection_1 = Projection(hash_join, [&txn](const auto &tuple) INLINE {
        //    const auto &[_person, anon_33, _friend_r, _country, anon_111, _org, workAt, _friend] = tuple;
        //    auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
        //    auto org = (snb::OrgSchema::Org*)txn.get_vertex(_org).data();
        //    return std::make_tuple(person->id, _friend, std::string_view(org->name(), org->nameLen()), 
        //            _org, *(uint32_t*)std::get<2>(workAt).data(), workAt);
        //});

        auto top = Top(projection_1, [](const auto &tuple) INLINE {
            const auto &[friendId, _friend, orgName, company, orgWorkFromYear] = tuple;
            return std::make_tuple(orgWorkFromYear, friendId, orgName);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) < std::get<0>(y)) return true;
            if(std::get<0>(x) > std::get<0>(y)) return false;
            if(std::get<1>(x) < std::get<1>(y)) return true;
            if(std::get<1>(x) > std::get<1>(y)) return false;
            return std::get<2>(x) > std::get<2>(y);
        }, request.limit);
        
        auto projection_result = Projection(top, [&txn](const auto &tuple) INLINE {
            const auto &[friendId, _friend, orgName, company, orgWorkFromYear] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            
            return std::make_tuple(friendId,
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()),
                    orgName, orgWorkFromYear);
        });

        auto plan = projection_result.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[personId, personFirstName, personLastName, orgName, orgWorkFromYear] = tuple;
            _return.emplace_back();
            _return.back().personId = personId;
            _return.back().personFirstName = personFirstName;
            _return.back().personLastName = personLastName;
            _return.back().organizationName = orgName;
            _return.back().organizationWorkFromYear = orgWorkFromYear;
        });

        
        plan();
    }

    void query12(std::vector<Query12Response> & _return, const Query12Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek_1(tagclassSchema.db, tagclassSchema.nameindex, request.tagClassName);
        //auto node_label_scan = NodeByLabelScan(tagclassSchema.db, tagclassSchema.nameindex);
        auto var_length_expand = VarLengthExpandAll(node_index_seek_1, [](const auto &tuple) INLINE {
            const auto &[_baseTagClass] = tuple;
            return _baseTagClass;
        }, txn, (label_t)snb::EdgeSchema::TagClass2TagClass_down, 0, std::numeric_limits<uint64_t>::max());
        auto anon_filter_1_1 = Filter(var_length_expand, [&txn](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_tagClass).data();
            return *type == snb::VertexSchema::TagClass;
        });
        auto filter_1_1 = Filter(anon_filter_1_1, [&txn, &request](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass] = tuple;
            return true;
            //auto baseTagClass = (snb::TagClassSchema::TagClass*)txn.get_vertex(_baseTagClass).data();
            //auto tagClass = (snb::TagClassSchema::TagClass*)txn.get_vertex(_tagClass).data();
            //return std::string_view(tagClass->name(), tagClass->nameLen()) == request.tagClassName || 
            //       std::string_view(baseTagClass->name(), baseTagClass->nameLen()) == request.tagClassName;
        });
        auto expand_1_1 = ExpandAll(filter_1_1, [](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass] = tuple;
            return _tagClass;
        }, txn, (label_t)snb::EdgeSchema::TagClass2Tag);
        auto filter_1_2 = Filter(expand_1_1, [&txn](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass, anon_145, _tag] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_tag).data();
            return *type == snb::VertexSchema::Tag;
        });

        NodeIndexSeek node_index_seek_2(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.personId));
        auto expand_2_1 = ExpandAll(node_index_seek_2, [](const auto &tuple) INLINE {
            const auto &[anon_7] = tuple;
            return anon_7;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);
        auto filter_2_1 = Filter(expand_2_1, [&txn](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend] = tuple;
            if(!enableSchemaCheck) return true;
            auto type = (snb::VertexSchema*)txn.get_vertex(_friend).data();
            return *type == snb::VertexSchema::Person;
        });
        auto expand_2_2 = ExpandAll(filter_2_1, [](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend] = tuple;
            return _friend;
        }, txn, (label_t)snb::EdgeSchema::Person2Comment_creator);
        auto filter_2_2 = Filter(expand_2_2, [&txn](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, comment] = tuple;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Comment;
        });
        //auto expand_2_3 = ExpandAll(filter_2_2, [](const auto &tuple) INLINE {
        //    const auto &[anon_7, anon_36, _friend, cache, anon_61, comment] = tuple;
        //    return comment;
        //}, txn, (label_t)snb::EdgeSchema::Message2Message_up);
        auto expand_2_3 = Projection(filter_2_2, [&txn](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, comment] = tuple;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(comment).data();
            return std::make_tuple(anon_7, anon_36, _friend, anon_61, comment, 0, message->replyOfPost);
        });
        auto filter_2_3 = Filter(expand_2_3, [&txn](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, comment, anon_94, anon_108] = tuple;
            if(anon_108 == (uint64_t)-1) return false;
            if(!enableSchemaCheck) return true;
            auto message = (snb::MessageSchema::Message*)txn.get_vertex(anon_108).data();
            return message->vtype == snb::VertexSchema::Message && message->type == snb::MessageSchema::Message::Type::Post;
        });
        auto expand_2_4 = ExpandAll(filter_2_3, [](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, comment, anon_94, anon_108] = tuple;
            return anon_108;
        }, txn, (label_t)snb::EdgeSchema::Post2Tag);

        auto hash_join = SortJoin(filter_1_2, expand_2_4, [](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass, anon_145, _tag] = tuple;
            return _tag;
        }, [](const auto &tuple) INLINE {
            const auto &[anon_7, anon_36, _friend, anon_61, comment, anon_94, anon_108, anon_115, _tag] = tuple;
            return _tag;
        });
        auto eager_aggragation = EagerAggragation(hash_join, [](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass, anon_145, _tag, anon_7, anon_36, _friend, anon_61, comment, anon_94, anon_108, anon_115, _tag_r] = tuple;
            return std::make_tuple(_friend);
        }, [&txn](const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass, anon_145, _tag, anon_7, anon_36, _friend, anon_61, comment, anon_94, anon_108, anon_115, _tag_r] = tuple;
            std::set<std::string_view> tagName_store;
            std::unordered_set<std::decay_t<decltype(comment)>> comment_store;
            auto tag = (snb::TagSchema::Tag*)txn.get_vertex(_tag).data();
            tagName_store.emplace(std::string_view(tag->name(), tag->nameLen()));
            comment_store.emplace(comment);
            return std::make_tuple(tagName_store, comment_store);
        }, [&txn](auto &ret, const auto &tuple) INLINE {
            const auto &[_baseTagClass, anon_178, _tagClass, anon_145, _tag, anon_7, anon_36, _friend, anon_61, comment, anon_94, anon_108, anon_115, _tag_r] = tuple;
            auto &[tagName_store, comment_store] = ret;
            auto tag = (snb::TagSchema::Tag*)txn.get_vertex(_tag).data();
            tagName_store.emplace(std::string_view(tag->name(), tag->nameLen()));
            comment_store.emplace(comment);
        }, [](const auto &ret) INLINE {
            const auto &[tagName_store, comment_store] = ret;
            std::vector<std::string_view> tagNames;
            for(const auto &name : tagName_store)
                tagNames.emplace_back(name);
            return std::make_tuple(tagNames, comment_store.size());
        });
        auto projection_1 = Projection(eager_aggragation, [&txn](const auto &tuple) INLINE {
            const auto &[_friend, tagNames, replyCount] = tuple;
            auto person = (snb::PersonSchema::Person*)txn.get_vertex(_friend).data();
            return std::make_tuple(person->id, 
                    std::string_view(person->firstName(), person->firstNameLen()),
                    std::string_view(person->lastName(), person->lastNameLen()), tagNames, replyCount);
        });
        auto top = Top(projection_1, [](const auto &tuple) INLINE {
            const auto &[personId, personFirstName, personLastName, tagNames, replyCount] = tuple;
            return std::make_tuple(replyCount, personId);
        }, [](const auto &x, const auto &y) INLINE {
            if(std::get<0>(x) > std::get<0>(y)) return true;
            if(std::get<0>(x) < std::get<0>(y)) return false;
            return std::get<1>(x) < std::get<1>(y);
        }, request.limit);

        auto plan = top.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[personId, personFirstName, personLastName, tagNames, replyCount] = tuple;
            _return.emplace_back();
            _return.back().personId = personId;
            _return.back().personFirstName = personFirstName;
            _return.back().personLastName = personLastName;
            for(const auto &name : tagNames)
                _return.back().tagNames.emplace_back(name);
            //_return.back().tagNames = tagNames;
            _return.back().replyCount = replyCount;
        });

        plan();
    }

    void query13(Query13Response& _return, const Query13Request& request)
    {
        _return = Query13Response();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek_1_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.person1Id));
        NodeIndexSeek node_index_seek_2_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.person2Id));
        auto cartesian_product = CartesianProduct(node_index_seek_1_1, node_index_seek_2_1);

        auto shortest_path = ShortestPath(cartesian_product, [](const auto &tuple) INLINE {
            return tuple;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);
        auto projection = Projection(shortest_path, [](const auto &tuple) INLINE {
            const auto &[_person1, _person2, length] = tuple;
            return std::make_tuple(length);
        });

        auto plan = projection.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[length] = tuple;
            _return.shortestPathLength = length;
        });

        plan();
    }

    void query14(std::vector<Query14Response> & _return, const Query14Request& request)
    {
        _return.clear();

        Transaction txn = graph->begin_read_only_transaction();

        NodeIndexSeek node_index_seek_1_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.person1Id));
        NodeIndexSeek node_index_seek_2_1(personSchema.db, personSchema.vindex, to_string<uint64_t>(request.person2Id));
        auto cartesian_product = CartesianProduct(node_index_seek_1_1, node_index_seek_2_1);
        auto shortest_path_all = ShortestPathAll(cartesian_product, [](const auto &tuple) INLINE {
            return tuple;
        }, txn, (label_t)snb::EdgeSchema::Person2Person);

        auto projection = Projection(shortest_path_all, [&txn](const auto &tuple) INLINE {
            const auto &[person1, person2, p, path] = tuple;
            double sum = 0;
            for(const auto &p : path)
            {
                const auto &[src, dst, value] = p;
                sum += *((double*)(value.data()+sizeof(uint64_t)));
            }
            std::vector<int64_t> personIds;
            for(const auto &_person : p)
            {
                auto person = (snb::PersonSchema::Person*)txn.get_vertex(_person).data();
                personIds.emplace_back(person->id);
            }
            return std::make_tuple(sum, personIds);
        });
        auto sort = Sort(projection, [](const auto &tuple) INLINE {
            const auto &[sum, personIds] = tuple;
            return std::make_tuple(sum);
        }, [](const auto &x, const auto &y) INLINE {
            return std::get<0>(x) > std::get<0>(y);
        });

        auto plan = sort.gen_plan([&_return](const auto &tuple) INLINE {
            const auto &[sum, personIds] = tuple;
            _return.emplace_back();
            _return.back().personIdsInPath = personIds;
            _return.back().pathWeight = sum;
        });

        plan();
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

};

