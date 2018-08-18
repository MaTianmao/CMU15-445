#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb
{

/*
 * constructor
 * array_size: fixed array size for each bucket
 */


template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
{
    bucket_size = size;
    global_depth = 0;
    bucket.reserve((1 << global_depth));
    std::vector<std::pair<K, V> > kv;
    bucket.push_back(Bucket<K,V>{0, kv, 0});
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key)
{
    return (size_t)key & ((1 << global_depth) - 1);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const
{
    return global_depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const
{
    return bucket[bucket_id].local_depth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const
{
    return (1<<global_depth);
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value)
{
    size_t hashkey = HashKey(key);
    for (size_t i = 0; i < bucket[hashkey].size; i++)
    {
        if (bucket[hashkey].kv[i].first == key)
        {
            value = bucket[hashkey].kv[i].second;
            return true;
        }
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key)
{   
    mtx.lock();
    size_t hashkey = HashKey(key);
    for (size_t i = 0; i < bucket[hashkey].size; i++)
    {
        if (bucket[hashkey].kv[i].first == key)
        {
            bucket[hashkey].kv.erase(bucket[hashkey].kv.begin() + i);
            bucket[hashkey].size--;
            mtx.unlock();
            return true;
        }
    }
    mtx.unlock();
    return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
{
    mtx.lock();
    size_t hashkey = HashKey(key);
    if(bucket[hashkey].size < bucket_size){
        bucket[hashkey].kv.push_back(std::make_pair(key, value));
        bucket[hashkey].size++;
        mtx.unlock();
        return;
    }
    if (bucket[hashkey].size == bucket_size && bucket[hashkey].local_depth == global_depth)
    {
        bucket.reserve((1 << (global_depth + 1)));
        std::copy(bucket.begin(), bucket.begin() + (1 << global_depth), back_inserter(bucket));
        global_depth++;
    }
    if (bucket[hashkey].size == bucket_size && bucket[hashkey].local_depth < global_depth)
    {
        std::vector<std::pair<K, V> > kv1, kv2;
        Bucket<K,V> b1{bucket[hashkey].local_depth + 1, kv1, 0}, b2{bucket[hashkey].local_depth + 1, kv2, 0};
        for (size_t i = 0; i < bucket[hashkey].size; i++)
        {
            size_t h = HashKey(bucket[hashkey].kv[i].first);
            if (((h >> bucket[hashkey].local_depth) & 1) == 1)
            {
                b1.kv.push_back(bucket[hashkey].kv[i]);
                b1.size++;
            }
            else
            {
                b2.kv.push_back(bucket[hashkey].kv[i]);
                b2.size++;
            }
        }
        for (int i = 0; i < (1 << global_depth); i++)
        {
            if (bucket[i] == bucket[hashkey])
            {
                if (((i >> bucket[hashkey].local_depth) & 1) == 1)
                {
                    bucket[i] = b1;
                }
                else
                {
                    bucket[i] = b2;
                }
            }
        }
    }
    mtx.unlock();
    Insert(key, value);
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
