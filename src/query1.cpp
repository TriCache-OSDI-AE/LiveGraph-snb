#include "manual.hpp"

void InteractiveHandler::query1(std::vector<Query1Response> & _return, const Query1Request& request)
{
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    std::vector<std::tuple<int, std::string, uint64_t, uint64_t, snb::PersonSchema::Person*>> idx;

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
                    auto firstName = std::string(person->firstName(), person->firstNameLen());
                    if(firstName == request.firstName)
                    {
                        auto lastName = std::string(person->lastName(), person->lastNameLen());
                        idx.push_back(std::make_tuple(k, lastName, person->id, nbrs.dst_id(), person));
                    }
                }
                nbrs.next();
            }
        }
        frontier.swap(next_frontier);
    }

    std::sort(idx.begin(), idx.end());



    for(size_t i=0;i<std::min((size_t)request.limit, idx.size());i++)
    {
        _return.emplace_back();
        auto vid = std::get<3>(idx[i]);
        auto person = std::get<4>(idx[i]);
        _return.back().friendId = person->id;
        _return.back().friendLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().distanceFromPerson = std::get<0>(idx[i])+1;
        _return.back().friendBirthday = to_time(person->birthday);
        _return.back().friendCreationDate = person->creationDate;
        _return.back().friendGender = std::string(person->gender(), person->genderLen());
        _return.back().friendBrowserUsed = std::string(person->browserUsed(), person->browserUsedLen());
        _return.back().friendLocationIp = std::string(person->locationIP(), person->locationIPLen());
        _return.back().friendEmails = split(std::string(person->emails(), person->emailsLen()), zero_split);
        _return.back().friendLanguages = split(std::string(person->speaks(), person->speaksLen()), zero_split);
        auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
        _return.back().friendCityName = std::string(place->name(), place->nameLen());
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
