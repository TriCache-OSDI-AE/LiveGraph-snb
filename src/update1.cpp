#include "manual.hpp"

void InteractiveHandler::update1(const interactive::Update1Request& request)
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
            txn.put_vertex(vid, std::string(person_buf.data(), person_buf.size()));
            txn.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Person, vid, {});
            for(auto tag:tag_vid)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Tag, tag, {});
                txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Person, vid, {});
            }
            for(size_t i=0;i<study_vid.size();i++)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Org_study, study_vid[i], std::string((char*)&request.studyAt_year[i], sizeof(request.studyAt_year[i])));
                txn.put_edge(study_vid[i], (label_t)snb::EdgeSchema::Org2Person_study, study_vid[i], std::string((char*)&request.studyAt_year[i], sizeof(request.studyAt_year[i])));
            }
            for(size_t i=0;i<work_vid.size();i++)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Org_work, work_vid[i], std::string((char*)&request.workAt_year[i], sizeof(request.workAt_year[i])));
                txn.put_edge(work_vid[i], (label_t)snb::EdgeSchema::Org2Person_work, work_vid[i], std::string((char*)&request.workAt_year[i], sizeof(request.workAt_year[i])));
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
