#pragma once
#include "core/lg.hpp"
#include "core/schema.hpp" 
#include "core/util.hpp"
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <iostream>

#define INLINE __attribute__((always_inline))

template <typename... Ts> std::ostream &operator<<(std::ostream &os, std::tuple<Ts...> const &theTuple)
{
    std::apply(
        [&os](Ts const &... tupleArgs) {
            os << '{';
            std::size_t n{0};
            ((os << tupleArgs << (++n != sizeof...(Ts) ? ", " : "")), ...);
            os << '}';
        },
        theTuple);
    return os;
}
template <typename T> std::ostream &operator<<(std::ostream &os, std::vector<T> const &array)
{
    os << '[';
    std::size_t n = 0;
    std::for_each(array.cbegin(), array.cend(), [&] (const T &t) {os << t << (++n != array.size() ? ", " : "");});
    os << ']';
    return os;
}

namespace utils
{
    namespace
    {

        // Code from boost
        // Reciprocal of the golden ratio helps spread entropy
        //     and handles duplicates.
        // See Mike Seymour in magic-numbers-in-boosthash-combine:
        //     https://stackoverflow.com/questions/4948780

        template <class T>
        inline void hash_combine(std::size_t& seed, T const& v)
        {
            seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
        }

        // Recursive template code derived from Matthieu M.
        template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
        struct HashValueImpl
        {
          static void apply(size_t& seed, Tuple const& tuple)
          {
            HashValueImpl<Tuple, Index-1>::apply(seed, tuple);
            hash_combine(seed, std::get<Index>(tuple));
          }
        };

        template <class Tuple>
        struct HashValueImpl<Tuple,0>
        {
          static void apply(size_t& seed, Tuple const& tuple)
          {
            hash_combine(seed, std::get<0>(tuple));
          }
        };
    }

    template <typename T>
    struct hash 
    {
        std::hash<T> _impl;
        size_t operator()(const T & tt) const
        {                                              
            return _impl(tt);
        }                                              

    };

    template <typename ... TT>
    struct hash<std::tuple<TT...>> 
    {
        size_t operator()(const std::tuple<TT...> & tt) const
        {                                              
            size_t seed = 0;                             
            HashValueImpl<std::tuple<TT...> >::apply(seed, tt);    
            return seed;                                 
        }                                              

    };

    template<typename ... input_t>
    using tuple_cat_t = decltype(std::tuple_cat(std::declval<input_t>()...));

}

using path_type = std::tuple<uint64_t, uint64_t, std::string_view>;

class NodeByLabelScan
{
    rocksdb::DB *db;
    rocksdb::ColumnFamilyHandle* index;
    rocksdb::ReadOptions read_options;
public:
    using type = std::tuple<uint64_t>;

    NodeByLabelScan(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_index) 
        : db(_db), index(_index), read_options()
    {
    }

    template <typename ParentConsumeType> 
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE { 
            parent_consume(tuple); 
        };

        return [&, consume](const auto &... tuple) INLINE { 
            auto* iter = db->NewIterator(read_options, index);
            iter->SeekToFirst();
            while(iter->Valid())
            {
                consume(std::make_tuple(*(uint64_t*)iter->value().data()));
                iter->Next();
            }

        };
    }
};

class NodeIndexSeek
{
    rocksdb::DB *db;
    rocksdb::ColumnFamilyHandle* index;
    rocksdb::ReadOptions read_options;
    const std::string prefix;
public:
    using type = std::tuple<uint64_t>;

    NodeIndexSeek(rocksdb::DB *_db, rocksdb::ColumnFamilyHandle *_index, const std::string _prefix) 
        : db(_db), index(_index), read_options(), prefix(_prefix)
    {
    }

    template <typename ParentConsumeType> 
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE { 
            parent_consume(tuple); 
        };

        return [&, consume](const auto &... tuple) INLINE { 
            auto* iter = db->NewIterator(read_options, index);
            iter->Seek(prefix);
            while(iter->Valid() && (iter->key().starts_with(prefix)))
            {
                if(iter->key() == prefix || iter->key().starts_with(prefix+snb::key_spacing))
                {
                    consume(std::make_tuple(*(uint64_t*)iter->value().data()));
                }
                iter->Next();
            }

        };
    }
};

template <typename LeftInputType, typename RightInputType> 
class CartesianProduct
{
    std::vector<typename RightInputType::type> right_data; //order by left, right
    LeftInputType &left;
    RightInputType &right;

public:
    using type = utils::tuple_cat_t<typename LeftInputType::type, typename RightInputType::type>;

    CartesianProduct(LeftInputType &_left, RightInputType &_right) 
        : left(_left), right(_right) 
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto right_consume = [&](const auto &tuple) INLINE {
            right_data.emplace_back(tuple);
        };
        auto left_consume = [&, parent_consume](const auto &tuple) INLINE {
            for(const auto &r : right_data)
            {
                parent_consume(std::tuple_cat(tuple, r));
            }
        };
        auto left_plan = left.gen_plan(left_consume);
        auto right_plan = right.gen_plan(right_consume);
        return [&, left_plan, right_plan](const auto &... tuple) INLINE { 
            right_plan(tuple...);
            left_plan(tuple...); 
            right_data.clear();
        };
    }
};

template <typename InputType, typename ProjectionFuncType>
class Projection
{
    InputType &input;
    ProjectionFuncType projection_func;

public:
    using type = std::invoke_result_t<ProjectionFuncType, typename InputType::type>;

    Projection(InputType &_input, const ProjectionFuncType &_projection_func) 
        : input(_input), projection_func(_projection_func) 
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            parent_consume(projection_func(tuple));
        };

        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename KeyFuncType, typename CompareFuncType>
class Top
{
    InputType &input;
    KeyFuncType key_func;
    CompareFuncType compare_func;
    const uint64_t max_size;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    std::multimap<key_type, typename InputType::type, CompareFuncType> store;

public:
    using type = typename InputType::type;

    Top(InputType &_input, const KeyFuncType &_key_func, const CompareFuncType &_compare_func, const uint64_t &_max_size) 
        : input(_input), key_func(_key_func), compare_func(_compare_func), max_size(_max_size), store(compare_func)
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&](const auto &tuple) INLINE {
            auto key = key_func(tuple);
            if(store.size() < max_size || compare_func(key, store.rbegin()->first))
            {
                store.emplace(key_func(tuple), tuple);
                if(store.size() > max_size)
                    store.erase(std::prev(store.end()));
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            for(const auto &pair : store)
            {
                parent_consume(pair.second);
            }
            store.clear();
        };
    }
};

template <typename InputType, typename OrderedKeyFuncType, typename KeyFuncType, typename CompareFuncType>
class PartialTop
{
    InputType &input;
    OrderedKeyFuncType ordered_key_func;
    KeyFuncType key_func;
    CompareFuncType compare_func;
    const uint64_t max_size;
    bool first;
    bool full;
    using ordered_key_type = std::invoke_result_t<OrderedKeyFuncType, typename InputType::type>;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    ordered_key_type current;
    std::multimap<key_type, typename InputType::type, CompareFuncType> store;

public:
    using type = typename InputType::type;

    PartialTop(InputType &_input, const OrderedKeyFuncType &_ordered_key_func, const KeyFuncType &_key_func, const CompareFuncType &_compare_func, const uint64_t &_max_size) 
        : input(_input), ordered_key_func(_ordered_key_func), key_func(_key_func), compare_func(_compare_func), current(), max_size(_max_size), first(true), full(false), store(compare_func)
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            if(full) return;
            auto key = ordered_key_func(tuple);
            if(first)
            {
                first = false;
                if(store.size() < max_size)
                {
                    current = key;
                    store.emplace(key_func(tuple), tuple);
                }
                else
                {
                    full = true;
                }
            }
            else if(current != key)
            {
                if(store.size() < max_size)
                {
                    current = key;
                    store.emplace(key_func(tuple), tuple);
                }
                else
                {
                    full = true;
                }
            }
            else
            {
                if(store.size() < max_size || compare_func(key, store.rbegin()->first))
                {
                    store.emplace(key_func(tuple), tuple);
                    if(store.size() > max_size)
                        store.erase(std::prev(store.end()));
                }
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            if(!first)
            {
                for(const auto &pair : store)
                {
                    parent_consume(pair.second);
                }
                store.clear();
                first = true;
                full = false;
            }
        };
    }
};

template <typename InputType, typename FilterFuncType>
class Filter
{
    InputType &input;
    FilterFuncType filter_func;

public:
    using type = typename InputType::type;

    Filter(InputType &_input, const FilterFuncType &_filter_func) 
        : input(_input), filter_func(_filter_func) 
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            if (filter_func(tuple))
                parent_consume(tuple);
        };

        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType>
class Limit
{
    InputType &input;
    const uint64_t limit;
    uint64_t count;

public:
    using type = typename InputType::type;

    Limit(InputType &_input, const uint64_t &_limit) 
        : input(_input), limit(_limit), count()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            if(count < limit)
            {
                parent_consume(tuple);
                ++count;
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
            count = 0;
        };
    }
};

template <typename InputType>
class Optional
{
    InputType &input;
    bool exist;

public:
    using type = typename InputType::type;

    Optional(InputType &_input) 
        : input(_input), exist(false)
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            exist = true;
            parent_consume(tuple);
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            if(!exist)
                parent_consume(type());
            exist = false;
        };
    }
};

template <typename InputType, typename KeyFuncType, typename InitFuncType, typename MergeFuncType, typename ReturnAggFuncType>
class OrderedAggragation
{
    InputType &input;
    KeyFuncType key_func;
    InitFuncType init_func;
    MergeFuncType merge_func;
    ReturnAggFuncType return_agg_func;
    bool first;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    using value_type = std::invoke_result_t<InitFuncType, typename InputType::type>;
    key_type current;
    value_type aggragation;

public:
    using type = utils::tuple_cat_t<key_type, std::invoke_result_t<ReturnAggFuncType, value_type>>;

    OrderedAggragation(InputType &_input, const KeyFuncType &_key_func, const InitFuncType &_init_func, const MergeFuncType &_merge_func, const ReturnAggFuncType &_return_agg_func) 
        : input(_input), key_func(_key_func), init_func(_init_func), merge_func(_merge_func), return_agg_func(_return_agg_func), first(true), current(), aggragation()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = key_func(tuple);
            if(first)
            {
                first = false;
                current = key;
                aggragation = init_func(tuple);
            }
            else if(current != key)
            {
                parent_consume(std::tuple_cat(current, return_agg_func(aggragation)));
                current = key;
                aggragation = init_func(tuple);
            }
            else
            {
                merge_func(aggragation, tuple);
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            if(!first)
            {
                parent_consume(std::tuple_cat(current, return_agg_func(aggragation)));
                first = true;
            }
        };
    }
};

template <typename InputType, typename KeyFuncType, typename InitFuncType, typename MergeFuncType, typename ReturnAggFuncType>
class EagerAggragation
{
    InputType &input;
    KeyFuncType key_func;
    InitFuncType init_func;
    MergeFuncType merge_func;
    ReturnAggFuncType return_agg_func;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    using value_type = std::invoke_result_t<InitFuncType, typename InputType::type>;
    std::unordered_map<key_type, value_type, utils::hash<key_type>> store;

public:
    using type = utils::tuple_cat_t<key_type, std::invoke_result_t<ReturnAggFuncType, value_type>>;

    EagerAggragation(InputType &_input, const KeyFuncType &_key_func, const InitFuncType &_init_func, const MergeFuncType &_merge_func, const ReturnAggFuncType &_return_agg_func) 
        : input(_input), key_func(_key_func), init_func(_init_func), merge_func(_merge_func), return_agg_func(_return_agg_func), store() 
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = key_func(tuple);
            auto iter = store.find(key);
            if(iter == store.end())
            {
                store.emplace_hint(iter, key, init_func(tuple));
            }
            else
            {
                merge_func(iter->second, tuple);
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            for(const auto &p : store)
            {
                parent_consume(std::tuple_cat(p.first, return_agg_func(p.second)));
            }
            store.clear();
        };
    }
};

template <typename InputType, typename KeyFuncType, typename CompareFuncType>
class Sort
{
    InputType &input;
    KeyFuncType key_func;
    CompareFuncType compare_func;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    std::vector<std::tuple<key_type, typename InputType::type>> store;

public:
    using type = typename InputType::type;

    Sort(InputType &_input, const KeyFuncType &_key_func, const CompareFuncType &_compare_func) 
        : input(_input), key_func(_key_func), compare_func(_compare_func), store()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&](const auto &tuple) INLINE {
            store.emplace_back(key_func(tuple), tuple);
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            std::sort(store.begin(), store.end(), [&](const auto &x, const auto &y) INLINE {
                return compare_func(std::get<0>(x), std::get<0>(y));
            });
            for(const auto &pair : store)
            {
                parent_consume(std::get<1>(pair));
            }
            store.clear();
        };
    }
};

template <typename InputType, typename OrderedKeyFuncType, typename KeyFuncType, typename CompareFuncType>
class PartialSort
{
    InputType &input;
    OrderedKeyFuncType ordered_key_func;
    KeyFuncType key_func;
    CompareFuncType compare_func;
    bool first;
    using ordered_key_type = std::invoke_result_t<OrderedKeyFuncType, typename InputType::type>;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    ordered_key_type current;
    std::vector<std::pair<key_type, typename InputType::type>> store;

public:
    using type = typename InputType::type;

    PartialSort(InputType &_input, const OrderedKeyFuncType &_ordered_key_func, const KeyFuncType &_key_func, const CompareFuncType &_compare_func) 
        : input(_input), ordered_key_func(_ordered_key_func), key_func(_key_func), compare_func(_compare_func), current(), first(true), store()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = ordered_key_func(tuple);
            if(first)
            {
                first = false;
                current = key;
                store.clear();
                store.emplace_back(key_func(tuple), tuple);
            }
            else if(current != key)
            {
                std::sort(store.begin(), store.end(), [](const auto &x, const auto &y) INLINE {return x.first < y.first;});
                for(const auto &p : store)
                    parent_consume(p.second);
                current = key;
                store.clear();
                store.emplace_back(key_func(tuple), tuple);
            }
            else
            {
                store.emplace_back(key_func(tuple), tuple);
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            if(!first)
            {
                std::sort(store.begin(), store.end(), [](const auto &x, const auto &y) INLINE {return x.first < y.first;});
                for(const auto &p : store)
                    parent_consume(p.second);
                first = true;
                store.clear();
            }
        };
    }
};

template <typename InputType, typename KeyFuncType>
class Distinct
{
    InputType &input;
    KeyFuncType key_func;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    std::unordered_set<key_type, utils::hash<key_type>> store;

public:
    using type = key_type;

    Distinct(InputType &_input, const KeyFuncType &_key_func) 
        : input(_input), key_func(_key_func), store() 
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = key_func(tuple);
            auto iter = store.find(key);
            if(iter == store.end())
            {
                store.emplace_hint(iter, key);
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            for(const auto &p : store)
            {
                parent_consume(p);
            }
            store.clear();
        };
    }
};

template <typename LeftInputType, typename RightInputType, typename LeftKeyFuncType, typename RightKeyFuncType>
class HashJoin
{
    LeftInputType &left;
    RightInputType &right;
    LeftKeyFuncType left_key_func;
    RightKeyFuncType right_key_func;
    using key_type = std::invoke_result_t<LeftKeyFuncType, typename LeftInputType::type>;
    static_assert(std::is_same_v<std::invoke_result_t<RightKeyFuncType, typename RightInputType::type>, key_type>);

    std::unordered_multimap<key_type, typename LeftInputType::type, utils::hash<key_type>> store;

public:
    using type = utils::tuple_cat_t<typename LeftInputType::type, typename RightInputType::type>;

    HashJoin(LeftInputType &_left, RightInputType &_right, const LeftKeyFuncType &_left_key_func, const RightKeyFuncType &_right_key_func) 
        :left(_left), right(_right), left_key_func(_left_key_func), right_key_func(_right_key_func), store()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto left_consume = [&](const auto &tuple) INLINE {
            auto key = left_key_func(tuple);
            store.emplace(key, tuple);
        };
        auto right_consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = right_key_func(tuple);
            auto range = store.equal_range(key);
            for(auto iter = range.first; iter != range.second; ++iter)
            {
                parent_consume(std::tuple_cat(iter->second, tuple));
            }
        };

        auto left_plan = left.gen_plan(left_consume);
        auto right_plan = right.gen_plan(right_consume);

        return [&, left_plan, right_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
            right_plan(tuple...); 
            store.clear();
        };
    }
};

template <typename LeftInputType, typename RightInputType, typename LeftKeyFuncType, typename RightKeyFuncType>
class SortJoin
{
    LeftInputType &left;
    RightInputType &right;
    LeftKeyFuncType left_key_func;
    RightKeyFuncType right_key_func;
    using key_type = std::invoke_result_t<LeftKeyFuncType, typename LeftInputType::type>;
    static_assert(std::is_same_v<std::invoke_result_t<RightKeyFuncType, typename RightInputType::type>, key_type>);
    const bool ordered;
    const bool unique;

    std::vector<std::pair<key_type, typename LeftInputType::type>> store;
    //std::unordered_map<key_type, typename LeftInputType::type, utils::hash<key_type>> store;

public:
    using type = utils::tuple_cat_t<typename LeftInputType::type, typename RightInputType::type>;

    SortJoin(LeftInputType &_left, RightInputType &_right, const LeftKeyFuncType &_left_key_func, const RightKeyFuncType &_right_key_func, bool _ordered = false, bool _unique = false) 
        :left(_left), right(_right), left_key_func(_left_key_func), right_key_func(_right_key_func), ordered(_ordered), unique(_unique), store()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto left_consume = [&](const auto &tuple) INLINE {
            auto key = left_key_func(tuple);
            store.emplace_back(key, tuple);
        };
        auto right_consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = right_key_func(tuple);
            auto begin = std::lower_bound(store.begin(), store.end(), key, [&](const auto &a, const auto &b) INLINE {
                return a.first < b;
            });
            for(auto iter = begin; iter != store.end(); iter++)
            {
                if((*iter).first == key)
                {
                    parent_consume(std::tuple_cat((*iter).second, tuple));
                }
                else
                {
                    break;
                }
                if(unique) break;
            }
        };

        auto left_plan = left.gen_plan(left_consume);
        auto right_plan = right.gen_plan(right_consume);

        return [&, left_plan, right_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
            if(!ordered) std::sort(store.begin(), store.end(), [&](const auto &a, const auto &b) INLINE {
                return a.first < b.first;
            });
            right_plan(tuple...); 
            store.clear();
        };
    }
};

template <typename LeftInputType, typename RightInputType, typename LeftKeyFuncType, typename RightKeyFuncType>
class RightOuterSortJoin
{
    LeftInputType &left;
    RightInputType &right;
    LeftKeyFuncType left_key_func;
    RightKeyFuncType right_key_func;
    using key_type = std::invoke_result_t<LeftKeyFuncType, typename LeftInputType::type>;
    static_assert(std::is_same_v<std::invoke_result_t<RightKeyFuncType, typename RightInputType::type>, key_type>);
    const bool ordered;

    std::vector<std::pair<key_type, typename LeftInputType::type>> store;
    //std::unordered_map<key_type, typename LeftInputType::type, utils::hash<key_type>> store;

public:
    using type = utils::tuple_cat_t<typename LeftInputType::type, typename RightInputType::type>;

    RightOuterSortJoin(LeftInputType &_left, RightInputType &_right, const LeftKeyFuncType &_left_key_func, const RightKeyFuncType &_right_key_func, bool _ordered = false) 
        :left(_left), right(_right), left_key_func(_left_key_func), right_key_func(_right_key_func), ordered(_ordered), store()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto left_consume = [&](const auto &tuple) INLINE {
            auto key = left_key_func(tuple);
            store.emplace_back(key, tuple);
        };
        auto right_consume = [&, parent_consume](const auto &tuple) INLINE {
            auto key = right_key_func(tuple);
            auto begin = std::lower_bound(store.begin(), store.end(), key, [&](const auto &a, const auto &b) INLINE {
                return a.first < b;
            });
            bool flag = false;
            for(auto iter = begin; iter != store.end(); iter++)
            {
                if((*iter).first == key)
                {
                    flag = true;
                    parent_consume(std::tuple_cat((*iter).second, tuple));
                }
                else
                {
                    break;
                }
            }
            if(!flag)
            {
                parent_consume(std::tuple_cat(typename LeftInputType::type(), tuple));
            }
        };

        auto left_plan = left.gen_plan(left_consume);
        auto right_plan = right.gen_plan(right_consume);

        return [&, left_plan, right_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
            if(!ordered) std::sort(store.begin(), store.end(), [&](const auto &a, const auto &b) INLINE {
                return a.first < b.first;
            });
            right_plan(tuple...); 
            store.clear();
        };
    }
};

template <typename InputType, typename CacheFuncType>
class CacheProperties
{
    InputType &input;
    CacheFuncType cache_func;

public:
    using type = std::invoke_result_t<CacheFuncType, typename InputType::type>;

    CacheProperties(InputType &_input, const CacheFuncType &_cache_func) 
        : input(_input), cache_func(_cache_func) 
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            parent_consume(cache_func(tuple));
        };

        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename LeftInputType, typename RightInputType> 
class Union
{
    LeftInputType &left;
    RightInputType &right;

public:
    static_assert(std::is_same_v<typename LeftInputType::type, typename RightInputType::type>);
    using type = typename LeftInputType::type;

    Union(LeftInputType &_left, RightInputType &_right) 
        : left(_left), right(_right)
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE { 
            parent_consume(tuple); 
        };
        auto left_plan = left.gen_plan(consume);
        auto right_plan = right.gen_plan(consume);
        return [&, left_plan, right_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
            right_plan(tuple...); 
        };
    }
};

template <typename LeftInputType, typename ArgumentFuncType>
class Argument
{
    ArgumentFuncType arg_func;

public:
    using type = std::invoke_result_t<ArgumentFuncType, typename LeftInputType::type>;

    Argument(LeftInputType &, const ArgumentFuncType &_arg_func)
        : arg_func(_arg_func) 
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE { 
            parent_consume(tuple); 
        };

        return [&, consume](const auto &... tuple) INLINE { 
            consume(arg_func(tuple...)); 
        };
    }
};

template <typename LeftInputType, typename RightInputType, typename CombineFuncType> 
class Apply
{
    LeftInputType &left;
    RightInputType &right;
    CombineFuncType combine_func;
    typename LeftInputType::type current;

public:
    using type = std::invoke_result_t<CombineFuncType, typename LeftInputType::type, typename RightInputType::type>;

    Apply(LeftInputType &_left, RightInputType &_right, const CombineFuncType &_combine_func) 
        : left(_left), right(_right), combine_func(_combine_func), current()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto right_consume = [&, parent_consume](const auto &tuple) INLINE { 
            parent_consume(combine_func(current, tuple)); 
        };
        auto right_plan = right.gen_plan(right_consume);
        auto left_consume = [&, right_plan](const auto &tuple) INLINE {
            current = tuple;
            right_plan(tuple);
        };
        auto left_plan = left.gen_plan(left_consume);
        return [&, left_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
        };
    }
};

template <typename LeftInputType, typename RightInputType> 
class SemiApply
{
    LeftInputType &left;
    RightInputType &right;
    bool exist;

public:
    using type = typename LeftInputType::type;

    SemiApply(LeftInputType &_left, RightInputType &_right) 
        : left(_left), right(_right), exist()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto right_consume = [&](const auto &tuple) INLINE { 
            if(!exist) 
                exist = true;
        };
        auto right_plan = right.gen_plan(right_consume);
        auto left_consume = [&, parent_consume, right_plan](const auto &tuple) INLINE {
            exist = false;
            right_plan(tuple);
            if(exist)
                parent_consume(tuple);
        };
        auto left_plan = left.gen_plan(left_consume);
        return [&, left_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
        };
    }
};

template <typename LeftInputType, typename RightInputType> 
class AntiSemiApply
{
    LeftInputType &left;
    RightInputType &right;
    bool exist;

public:
    using type = typename LeftInputType::type;

    AntiSemiApply(LeftInputType &_left, RightInputType &_right) 
        : left(_left), right(_right), exist()
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto right_consume = [&](const auto &tuple) INLINE { 
            if(!exist) 
                exist = true;
        };
        auto right_plan = right.gen_plan(right_consume);
        auto left_consume = [&, parent_consume, right_plan](const auto &tuple) INLINE {
            exist = false;
            right_plan(tuple);
            if(!exist)
                parent_consume(tuple);
        };
        auto left_plan = left.gen_plan(left_consume);
        return [&, left_plan](const auto &... tuple) INLINE { 
            left_plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc> 
class ExpandAll
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    const std::vector<label_t> etypes;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<path_type, uint64_t>>;

    ExpandAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const std::vector<label_t> &_etypes)
        : input(_input), select_func(_select_func), txn(_txn), etypes(_etypes)
    {
    }
    ExpandAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const label_t &_etype)
        : input(_input), select_func(_select_func), txn(_txn), etypes({_etype})
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto src = select_func(tuple);
            for(const auto &etype:etypes)
            {
                auto nbrs = txn.get_edges(src, etype);
                while(nbrs.valid())
                {
                    parent_consume(std::tuple_cat(tuple, std::make_tuple(std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data()), nbrs.dst_id())));
                    nbrs.next();
                }
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc> 
class VarLengthExpandAll
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    const std::vector<label_t> etypes;
    
    const uint64_t min_hops, max_hops;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<uint64_t, uint64_t>>;

    VarLengthExpandAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const std::vector<label_t> &_etypes, const uint64_t &_min_hops, const uint64_t &_max_hops)
        : input(_input), select_func(_select_func), txn(_txn), etypes(_etypes), min_hops(_min_hops), max_hops(_max_hops)
    {
    }
    VarLengthExpandAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const label_t &_etype, const uint64_t &_min_hops, const uint64_t &_max_hops)
        : input(_input), select_func(_select_func), txn(_txn), etypes({_etype}), min_hops(_min_hops), max_hops(_max_hops)
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto src = select_func(tuple);
            uint64_t path = 0;
            //std::vector<path_type> path;
            auto dfs = [&](uint64_t src, uint64_t hops, const auto &dfs) -> void
            {
                if(hops > max_hops) return;
                //path.emplace_back();
                for(const auto &etype:etypes)
                {
                    for(auto nbrs = txn.get_edges(src, etype);nbrs.valid();nbrs.next())
                    {
                        const auto cur_edge = std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data());
                        //bool exist = false;
                        //for(size_t i=0;i<path.size()-1;i++)
                        //{
                        //    if(path[i] == cur_edge)
                        //    {
                        //        exist = true;
                        //        break;
                        //    }
                        //}
                        //if(exist) continue;
                        //path.back() = cur_edge;
                        if(hops >= min_hops && hops <= max_hops)
                        {
                            parent_consume(std::tuple_cat(tuple, std::make_tuple(path, nbrs.dst_id())));
                        }
                        dfs(nbrs.dst_id(), hops+1, dfs);
                    }
                }
                //path.pop_back();
            };
            if(min_hops == 0)
            {
                parent_consume(std::tuple_cat(tuple, std::make_tuple(path, src)));
            }
            dfs(src, 1, dfs);
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc> 
class VarLengthExpandPruning
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    const std::vector<label_t> etypes;
    
    const uint64_t min_hops, max_hops;

    //std::unordered_set<uint64_t> store;
    std::vector<uint64_t> store;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<uint64_t, uint64_t>>;

    VarLengthExpandPruning(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const std::vector<label_t> &_etypes, const uint64_t &_min_hops, const uint64_t &_max_hops)
        : input(_input), select_func(_select_func), txn(_txn), etypes(_etypes), min_hops(_min_hops), max_hops(_max_hops), store()
    {
    }
    VarLengthExpandPruning(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const label_t &_etype, const uint64_t &_min_hops, const uint64_t &_max_hops)
        : input(_input), select_func(_select_func), txn(_txn), etypes({_etype}), min_hops(_min_hops), max_hops(_max_hops), store()
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto src = select_func(tuple);
            uint64_t path = 0;
            //std::vector<path_type> path;
            auto dfs = [&](uint64_t src, uint64_t hops, const auto &dfs)
            {
                if(hops > max_hops) return;
                //path.emplace_back();
                for(const auto &etype:etypes)
                {
                    for(auto nbrs = txn.get_edges(src, etype);nbrs.valid();nbrs.next())
                    {
                        const auto cur_edge = std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data());
                        //bool exist = false;
                        //for(size_t i=0;i<path.size()-1;i++)
                        //{
                        //    if(path[i] == cur_edge)
                        //    {
                        //        exist = true;
                        //        break;
                        //    }
                        //}
                        //if(exist) continue;
                        //path.back() = cur_edge;
                        if(hops >= min_hops && hops <= max_hops)
                        {
                            store.emplace_back(nbrs.dst_id());
                            //auto it = store.find(nbrs.dst_id());
                            //if(it == store.end())
                            //{
                            //    parent_consume(std::tuple_cat(tuple, std::make_tuple(path, nbrs.dst_id())));
                            //    store.emplace_hint(it, nbrs.dst_id());
                            //}
                        }
                        dfs(nbrs.dst_id(), hops+1, dfs);
                    }
                }
                //path.pop_back();
            };
            auto bfs = [&](uint64_t src, uint64_t hops) INLINE
            {
                std::vector<size_t> frontier = {src};
                std::vector<size_t> next_frontier;
                for (uint64_t k=1;k<=hops;k++)
                {
                    next_frontier.clear();
                    for (auto vid : frontier)
                    {
                        for(const auto &etype:etypes)
                        {
                            auto nbrs = txn.get_edges(vid, etype);
                            while (nbrs.valid())
                            {
                                next_frontier.push_back(nbrs.dst_id());
                                if(k >= min_hops && k <= max_hops)
                                {
                                    store.emplace_back(nbrs.dst_id());
                                }
                                nbrs.next();
                            }
                        }
                    }
                    frontier.swap(next_frontier);
                }
            };
            //bfs(src, max_hops);
            if(min_hops == 0)
            {
                parent_consume(std::tuple_cat(tuple, std::make_tuple(path, src)));
            }
            dfs(src, 1, dfs);
            std::sort(store.begin(), store.end());
            auto last = std::unique(store.begin(), store.end());
            for(auto iter=store.begin();iter!=last;iter++)
            {
                parent_consume(std::tuple_cat(tuple, std::make_tuple(path, *iter)));
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
            store.clear();
        };
    }
};

template <typename InputType, typename SelectFunc> 
class ExpandInto
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    const std::vector<label_t> etypes;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, std::tuple<uint64_t, uint64_t>>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<path_type>>;

    ExpandInto(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const std::vector<label_t> &_etypes)
        : input(_input), select_func(_select_func), txn(_txn), etypes(_etypes)
    {
    }
    ExpandInto(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const label_t &_etype)
        : input(_input), select_func(_select_func), txn(_txn), etypes({_etype})
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            const auto [src, dst] = select_func(tuple);
            for(const auto &etype:etypes)
            {
                //TODO: change to get_edge
                auto nbrs = txn.get_edges(src, etype);
                while(nbrs.valid())
                {
                    if(nbrs.dst_id() == dst)
                    {
                        parent_consume(std::tuple_cat(tuple, std::make_tuple(std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data()))));
                    }
                    nbrs.next();
                }
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc> 
class OptionalExpandAll
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    const std::vector<label_t> etypes;

    bool exist;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<path_type, uint64_t>>;

    OptionalExpandAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const std::vector<label_t> &_etypes)
        : input(_input), select_func(_select_func), txn(_txn), etypes(_etypes), exist(false)
    {
    }
    OptionalExpandAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, const label_t &_etype)
        : input(_input), select_func(_select_func), txn(_txn), etypes({_etype}), exist(false)
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            exist = false;
            auto src = select_func(tuple);
            for(const auto &etype:etypes)
            {
                auto nbrs = txn.get_edges(src, etype);
                while(nbrs.valid())
                {
                    exist = true;
                    parent_consume(std::tuple_cat(tuple, std::make_tuple(std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data()), nbrs.dst_id())));
                    nbrs.next();
                }
            }
            if(!exist)
                parent_consume(std::tuple_cat(tuple, std::make_tuple(path_type(), 0)));
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc> 
class ShortestPath
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    label_t etype;
    uint64_t max_hops;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, std::tuple<uint64_t, uint64_t>>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<int64_t>>;

    ShortestPath(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, label_t _etype, uint64_t _max_hops=std::numeric_limits<uint64_t>::max())
        : input(_input), select_func(_select_func), txn(_txn), etype(_etype), max_hops(_max_hops)
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto pairwiseShortestPath = [&](uint64_t vid_from, uint64_t vid_to) -> int64_t 
        {
            if(vid_from == vid_to) return 0;
            std::unordered_map<uint64_t, std::pair<uint64_t, std::string_view>> parent, child;
            std::vector<uint64_t> forward_q, backward_q;
            parent[vid_from] = {vid_from, ""};
            child[vid_to] = {vid_to, ""};
            forward_q.push_back(vid_from);
            backward_q.push_back(vid_to);
            uint64_t hops = 0;
            auto get_path = [&](uint64_t src, uint64_t dst, auto &mid_edge) INLINE -> std::tuple<std::vector<uint64_t>, std::vector<path_type>>
            {
                std::vector<uint64_t> path_v;
                std::vector<path_type> path_e;
                for(auto it=parent.find(src);it!=parent.end();it=parent.find(it->second.first))
                {
                    path_v.emplace_back(it->first);
                    if(it->first == vid_from) break;
                    path_e.emplace_back(it->second.first, it->first, it->second.second);
                }
                std::reverse(path_v.begin(), path_v.end());
                std::reverse(path_e.begin(), path_e.end());
                path_e.emplace_back(std::make_tuple(src, dst, mid_edge.edge_data()));
                for(auto it=child.find(dst);it!=child.end();it=child.find(it->second.first))
                {
                    path_v.emplace_back(it->first);
                    if(it->first == vid_to) break;
                    path_e.emplace_back(it->first, it->second.first, it->second.second);
                }
                return {path_v, path_e};

            };
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
                                parent.emplace_hint(it, dst, std::make_pair(vid, out_edges.edge_data()));
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
                                child.emplace_hint(it, src, std::make_pair(vid, in_edges.edge_data()));
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
        };
        auto consume = [&, parent_consume, pairwiseShortestPath](const auto &tuple) INLINE {
            auto path = std::apply(pairwiseShortestPath, select_func(tuple));
            parent_consume(std::tuple_cat(tuple, std::make_tuple(path)));
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc> 
class ShortestPathAll
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    label_t etype;
    uint64_t max_hops;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, std::tuple<uint64_t, uint64_t>>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<std::vector<uint64_t>, std::vector<path_type>>>;

    ShortestPathAll(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, label_t _etype, uint64_t _max_hops=std::numeric_limits<uint64_t>::max())
        : input(_input), select_func(_select_func), txn(_txn), etype(_etype), max_hops(_max_hops)
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto pairwiseShortestPathAll = [&](uint64_t vid_from, uint64_t vid_to) -> std::vector<std::tuple<std::vector<uint64_t>, std::vector<path_type>>>
        {
            if(vid_from == vid_to) return {{{vid_from}, {}}};
            std::unordered_map<uint64_t, uint64_t> parent, child;
            std::vector<uint64_t> forward_q, backward_q;
            parent[vid_from] = 0;
            child[vid_to] = 0;
            forward_q.push_back(vid_from);
            backward_q.push_back(vid_to);
            uint64_t hops = 0, psp = max_hops, fhops = 0, bhops = 0;
            std::map<std::pair<uint64_t, uint64_t>, std::string_view> hits;
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
                            if (child.find(dst) != child.end()) 
                            {
                                hits.emplace(std::make_pair(vid, dst), out_edges.edge_data());
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
                            if (parent.find(src) != parent.end()) 
                            {
                                hits.emplace(std::make_pair(src, vid), in_edges.edge_data());
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
            
            std::vector<std::tuple<std::vector<uint64_t>, std::vector<path_type>>> paths;
            for(auto p : hits)
            {
                std::vector<uint64_t> path_v;
                std::vector<path_type> path_e;
                std::vector<std::tuple<std::vector<uint64_t>, std::vector<path_type>>> fpaths, bpaths;
                auto fdfs = [&](uint64_t vid, uint64_t deep, const auto &fdfs) -> void
                {
                    path_v.emplace_back(vid);
                    if(deep > 0)
                    {
                        auto out_edges = txn.get_edges(vid, etype);
                        while (out_edges.valid()) 
                        {
                            uint64_t dst = out_edges.dst_id();
                            auto iter = parent.find(dst);
                            if(iter != parent.end() && iter->second == deep-1)
                            {
                                path_e.emplace_back(out_edges.dst_id(), vid, out_edges.edge_data());
                                fdfs(dst, deep-1, fdfs);
                                path_e.pop_back();
                            }
                            out_edges.next();
                        }
                    }
                    else
                    {
                        fpaths.emplace_back();
                        std::reverse_copy(path_v.begin(), path_v.end(), std::back_inserter(std::get<0>(fpaths.back())));
                        std::reverse_copy(path_e.begin(), path_e.end(), std::back_inserter(std::get<1>(fpaths.back())));
                    }
                    path_v.pop_back();
                };

                auto bdfs = [&](uint64_t vid, uint64_t deep, const auto &bdfs) -> void
                {
                    path_v.emplace_back(vid);
                    if(deep > 0)
                    {
                        auto out_edges = txn.get_edges(vid, etype);
                        while (out_edges.valid()) 
                        {
                            uint64_t dst = out_edges.dst_id();
                            auto iter = child.find(dst);
                            if(iter != child.end() && iter->second == deep-1)
                            {
                                path_e.emplace_back(vid, out_edges.dst_id(), out_edges.edge_data());
                                bdfs(dst, deep-1, bdfs);
                                path_e.pop_back();
                            }
                            out_edges.next();
                        }
                    }
                    else
                    {
                        bpaths.emplace_back(path_v, path_e);
                    }
                    path_v.pop_back();
                };

                fdfs(p.first.first, parent[p.first.first], fdfs);
                bdfs(p.first.second, child[p.first.second], bdfs);
                for(auto &f: fpaths)
                {
                    for(auto &b: bpaths)
                    {
                        paths.emplace_back(f);
                        std::get<1>(paths.back()).emplace_back(p.first.first, p.first.second, p.second);
                        std::copy(std::get<0>(b).begin(), std::get<0>(b).end(), std::back_inserter(std::get<0>(paths.back())));
                        std::copy(std::get<1>(b).begin(), std::get<1>(b).end(), std::back_inserter(std::get<1>(paths.back())));
                    }
                }
            }
            return paths;
        };
        auto consume = [&, parent_consume, pairwiseShortestPathAll](const auto &tuple) INLINE {
            auto paths = std::apply(pairwiseShortestPathAll, select_func(tuple));
            for(const auto &path : paths)
            {
                parent_consume(std::tuple_cat(tuple, std::move(path)));
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename Tuple> 
class Empty
{
public:
    using type = Tuple;
};

template <typename T> 
class TestScan
{
    std::vector<T> data;

public:
    using type = std::tuple<T>;

    TestScan(const std::vector<T> &_data)
        : data(_data)
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE { 
            parent_consume(tuple); 
        };

        return [&, consume](const auto &... tuple) INLINE { 
            for(const auto &t : data) 
                consume(std::make_tuple(t));
        };
    }
};

template <typename InputType, typename SelectFunc, typename FilterFuncType>
class ExpandAllFilterLooseLimit
{
    InputType &input;
    SelectFunc select_func;
    FilterFuncType filter_func;

    Transaction & txn;
    const std::vector<label_t> etypes;

    const uint64_t max_size;
    uint64_t count;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<path_type, uint64_t>>;

    ExpandAllFilterLooseLimit(InputType &_input, const SelectFunc &_select_func, const FilterFuncType &_filter_func, Transaction &_txn, const std::vector<label_t> &_etypes, const uint64_t &_max_size)
        : input(_input), select_func(_select_func), filter_func(_filter_func), txn(_txn), etypes(_etypes), max_size(_max_size), count(0)
    {
    }

    ExpandAllFilterLooseLimit(InputType &_input, const SelectFunc &_select_func, const FilterFuncType &_filter_func, Transaction &_txn, const label_t &_etype, const uint64_t &_max_size)
        : input(_input), select_func(_select_func), filter_func(_filter_func), txn(_txn), etypes({_etype}), max_size(_max_size), count(0)
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto src = select_func(tuple);
            for(const auto &etype:etypes)
            {
                auto nbrs = txn.get_edges(src, etype);
                auto cur = nbrs.edge_data();
                while(nbrs.valid() && (count < max_size || cur == nbrs.edge_data()))
                {
                    if(filter_func(nbrs.edge_data()))
                    {
                        parent_consume(std::tuple_cat(tuple, std::make_tuple(std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data()), nbrs.dst_id())));
                        cur = nbrs.edge_data();
                        count++;
                    }
                    nbrs.next();
                }
                count = 0;
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename SelectFunc, typename RangeLeftFuncType, typename RangeRightFuncType>
class ExpandAllRange
{
    InputType &input;
    SelectFunc select_func;
    RangeLeftFuncType range_left_func;
    RangeRightFuncType range_right_func;

    Transaction & txn;
    const std::vector<label_t> etypes;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<path_type, uint64_t>>;

    ExpandAllRange(InputType &_input, const SelectFunc &_select_func, const RangeLeftFuncType &_range_left_func, const RangeRightFuncType &_range_right_func, Transaction &_txn, const std::vector<label_t> &_etypes)
        : input(_input), select_func(_select_func), range_left_func(_range_left_func), range_right_func(_range_right_func), txn(_txn), etypes(_etypes)
    {
    }
    ExpandAllRange(InputType &_input, const SelectFunc &_select_func, const RangeLeftFuncType &_range_left_func, const RangeRightFuncType &_range_right_func, Transaction &_txn, const label_t &_etype)
        : input(_input), select_func(_select_func), range_left_func(_range_left_func), range_right_func(_range_right_func), txn(_txn), etypes({_etype})
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            auto src = select_func(tuple);
            for(const auto &etype:etypes)
            {
                auto nbrs = txn.get_edges(src, etype);
                while(nbrs.valid() && range_left_func(nbrs.edge_data()))
                {
                    if(range_right_func(nbrs.edge_data()))
                    {
                        parent_consume(std::tuple_cat(tuple, std::make_tuple(std::make_tuple(src, nbrs.dst_id(), nbrs.edge_data()), nbrs.dst_id())));
                    }
                    nbrs.next();
                }
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};

template <typename InputType, typename KeyFuncType, typename CompareFuncType>
class LooseTop
{
    InputType &input;
    KeyFuncType key_func;
    CompareFuncType compare_func;
    const uint64_t max_size;
    using key_type = std::invoke_result_t<KeyFuncType, typename InputType::type>;
    std::multimap<key_type, typename InputType::type, CompareFuncType> store;

public:
    using type = typename InputType::type;

    LooseTop(InputType &_input, const KeyFuncType &_key_func, const CompareFuncType &_compare_func, const uint64_t &_max_size) 
        : input(_input), key_func(_key_func), compare_func(_compare_func), max_size(_max_size), store(compare_func)
    {
    }

    template <typename ParentConsumeType> INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&](const auto &tuple) INLINE {
            auto key = key_func(tuple);
            if(store.size() < max_size || !compare_func(store.rbegin()->first, key))
            {
                store.emplace(key_func(tuple), tuple);
                auto begin = store.lower_bound(std::prev(store.end())->first);
                uint64_t sum = 0;
                for(auto it=begin;it!=store.end();++it) ++sum;
                if(store.size() >= max_size+sum)
                {
                    for(uint64_t i=0;i<sum;i++)
                    {
                        store.erase(std::prev(store.end()));
                    }
                }
            }
        };

        auto plan = input.gen_plan(consume);
        return [&, plan, parent_consume](const auto &... tuple) INLINE { 
            plan(tuple...); 
            for(const auto &pair : store)
            {
                parent_consume(pair.second);
            }
            store.clear();
        };
    }
};

template <typename InputType, typename SelectFunc> 
class SingleSourceShortestPath
{
    InputType &input;
    SelectFunc select_func;

    Transaction & txn;
    label_t etype;
    uint64_t max_hops;

    static_assert(std::is_same_v<std::invoke_result_t<SelectFunc, typename InputType::type>, uint64_t>);


public:
    using type = utils::tuple_cat_t<typename InputType::type, std::tuple<uint64_t>, std::tuple<uint64_t>>;

    SingleSourceShortestPath(InputType &_input, const SelectFunc &_select_func, Transaction &_txn, label_t _etype, uint64_t _max_hops=std::numeric_limits<uint64_t>::max())
        : input(_input), select_func(_select_func), txn(_txn), etype(_etype), max_hops(_max_hops)
    {
    }

    template <typename ParentConsumeType>
    INLINE auto gen_plan(const ParentConsumeType &parent_consume)
    {
        auto consume = [&, parent_consume](const auto &tuple) INLINE {
            uint64_t vid_from = select_func(tuple);
            std::unordered_map<uint64_t, std::pair<uint64_t, std::string_view>> parent;
            std::vector<uint64_t> forward_q;
            parent[vid_from] = {vid_from, ""};
            forward_q.push_back(vid_from);
            uint64_t hops = 0;
            auto get_path = [&](uint64_t src) INLINE -> std::tuple<std::vector<uint64_t>, std::vector<path_type>>
            {
                std::vector<uint64_t> path_v;
                std::vector<path_type> path_e;
                for(auto it=parent.find(src);it!=parent.end();it=parent.find(it->second.first))
                {
                    path_v.emplace_back(it->first);
                    if(it->first == vid_from) break;
                    path_e.emplace_back(it->second.first, it->first, it->second.second);
                }
                std::reverse(path_v.begin(), path_v.end());
                std::reverse(path_e.begin(), path_e.end());
                return {path_v, path_e};

            };
            while (hops++ < max_hops) 
            {
                std::vector<uint64_t> new_front;
                for (uint64_t vid : forward_q) 
                {
                    auto out_edges = txn.get_edges(vid, etype);
                    while (out_edges.valid()) 
                    {
                        uint64_t dst = out_edges.dst_id();
                        auto it = parent.find(dst);
                        if (it == parent.end()) 
                        {
                            parent.emplace_hint(it, dst, std::make_pair(vid, out_edges.edge_data()));
                            new_front.push_back(dst);
                            parent_consume(std::tuple_cat(tuple, std::make_tuple(dst), std::make_tuple(hops)));
                        }
                        out_edges.next();
                    }
                }
                if (new_front.empty()) break;
                forward_q.swap(new_front);
            }
        };
        auto plan = input.gen_plan(consume);
        return [&, plan](const auto &... tuple) INLINE { 
            plan(tuple...); 
        };
    }
};
