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

    void query1(std::vector<Query1Response> & _return, const Query1Request& request);

    void query2(std::vector<Query2Response> & _return, const Query2Request& request);

    void query3(std::vector<Query3Response> & _return, const Query3Request& request);

    void query4(std::vector<Query4Response> & _return, const Query4Request& request);

    void query5(std::vector<Query5Response> & _return, const Query5Request& request);

    void query6(std::vector<Query6Response> & _return, const Query6Request& request);

    void query7(std::vector<Query7Response> & _return, const Query7Request& request);

    void query8(std::vector<Query8Response> & _return, const Query8Request& request);

    void query9(std::vector<Query9Response> & _return, const Query9Request& request);

    void query10(std::vector<Query10Response> & _return, const Query10Request& request);

    void query11(std::vector<Query11Response> & _return, const Query11Request& request);

    void query12(std::vector<Query12Response> & _return, const Query12Request& request);

    void query13(Query13Response& _return, const Query13Request& request);

    void query14(std::vector<Query14Response> & _return, const Query14Request& request);

    void shortQuery1(ShortQuery1Response& _return, const ShortQuery1Request& request);

    void shortQuery2(std::vector<ShortQuery2Response> & _return, const ShortQuery2Request& request);

    void shortQuery3(std::vector<ShortQuery3Response> & _return, const ShortQuery3Request& request);

    void shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request& request);

    void shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request& request);

    void shortQuery6(ShortQuery6Response& _return, const ShortQuery6Request& request);

    void shortQuery7(std::vector<ShortQuery7Response>& _return, const ShortQuery7Request& request);

    void update1(const interactive::Update1Request& request);

    void update2(const Update2Request& request);

    void update3(const Update3Request& request);

    void update4(const Update4Request& request);

    void update5(const Update5Request& request);

    void update6(const Update6Request& request);

    void update7(const Update7Request& request);

    void update8(const Update8Request& request);

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
            
            if (forward_q.size() <= backward_q.size()) 
            {
                
                for (uint64_t vid : forward_q) 
                {
                    auto out_edges = txn.get_edges(vid, etype);
                    while (out_edges.valid()) 
                    {
                        uint64_t dst = out_edges.dst_id();
                        if (child.find(dst) != child.end()) 
                        {
                            
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

