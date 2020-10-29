#pragma once

#include "core/schema.hpp" 
#include "core/util.hpp"
#include "core/lg.hpp"

#include "Interactive.h"
using namespace ::interactive;

class InteractiveHandler : virtual public InteractiveIf
{
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

public:
    InteractiveHandler(Graph * const graph, snb::PersonSchema &personSchema,  snb::PlaceSchema &placeSchema,  snb::OrgSchema &orgSchema,  snb::MessageSchema &postSchema,  snb::MessageSchema &commentSchema,  snb::TagSchema &tagSchema,  snb::TagClassSchema &tagclassSchema,  snb::ForumSchema &forumSchema)
        :graph(graph), personSchema(personSchema), placeSchema(placeSchema), orgSchema(orgSchema), postSchema(postSchema), commentSchema(commentSchema), tagSchema(tagSchema), tagclassSchema(tagclassSchema), forumSchema(forumSchema)
    {
    }

    void query1(std::vector<Query1Response> & _return, const Query1Request& request)
    {
        _return.clear();
    }

    void query2(std::vector<Query2Response> & _return, const Query2Request& request)
    {
        _return.clear();
    }

    void query3(std::vector<Query3Response> & _return, const Query3Request& request)
    {
        _return.clear();
    }

    void query4(std::vector<Query4Response> & _return, const Query4Request& request)
    {
        _return.clear();
    }

    void query5(std::vector<Query5Response> & _return, const Query5Request& request)
    {
        _return.clear();
    }

    void query6(std::vector<Query6Response> & _return, const Query6Request& request)
    {
        _return.clear();
    }

    void query7(std::vector<Query7Response> & _return, const Query7Request& request)
    {
        _return.clear();
    }

    void query8(std::vector<Query8Response> & _return, const Query8Request& request)
    {
        _return.clear();
    }

    void query9(std::vector<Query9Response> & _return, const Query9Request& request)
    {
        _return.clear();
    }

    void query10(std::vector<Query10Response> & _return, const Query10Request& request)
    {
        _return.clear();
    }

    void query11(std::vector<Query11Response> & _return, const Query11Request& request)
    {
        _return.clear();
    }

    void query12(std::vector<Query12Response> & _return, const Query12Request& request)
    {
        _return.clear();
    }

    void query13(Query13Response& _return, const Query13Request& request)
    {
        _return = Query13Response();
    }

    void query14(std::vector<Query14Response> & _return, const Query14Request& request)
    {
        _return.clear();
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

};

