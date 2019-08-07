#include <list>
#include <cassert>


#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size): globalDepth(0), bucketSizeLimit(size) {
        bucketDirectory.push_back(std::make_shared<Bucket>(0));
    }

/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        return std::hash<K>{}(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        return globalDepth;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        return bucketDirectory[bucket_id]->localDepth;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        return static_cast<int>(bucketDirectory.size());
    }

/*
 * lookup function to find value associate with input key
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

        std::lock_guard<std::mutex> guard(mtx);
        std::shared_ptr<Bucket> local_bucket = getBucket(key);
        
        if(local_bucket->contents.find(key) != local_bucket->contents.end()){
            value = local_bucket->contents[key];
            return true;
        }else
            return false;
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {

        std::lock_guard<std::mutex> guard(mtx);
        std::shared_ptr<Bucket> local_bucket = getBucket(key);
        
        if(local_bucket->contents.find(key) != local_bucket->contents.end()){
            local_bucket->contents.erase(key);
            return true;
        }else
            return false;
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        std::lock_guard<std::mutex> guard(mtx);

        int index = getBucketIndex(HashKey(key));

        std::shared_ptr<Bucket> local_bucket = getBucket(key);
        while(local_bucket->contents.size() == bucketSizeLimit){
            if(local_bucket->localDepth == globalDepth){
                int len = bucketDirectory.size();
                for(int i = 0; i < len; i++){
                    bucketDirectory.push_back(bucketDirectory[i]);
                }
                ++globalDepth;
            }

            auto split_1 = std::make_shared<Bucket>(local_bucket->localDepth + 1);
            auto split_2 = std::make_shared<Bucket>(local_bucket->localDepth + 1);

            int mask = (1 << local_bucket->localDepth);
            for(auto item : local_bucket->contents){
                size_t newHash = HashKey(item.first);
                if(newHash & mask)
                    split_2->contents.insert(item);
                else
                    split_1->contents.insert(item);
            }

            for(size_t i = 0; i < bucketDirectory.size(); i++){
                if(bucketDirectory[i] == local_bucket){
                    if(i & mask)
                        bucketDirectory[i] = split_2;
                    else
                        bucketDirectory[i] = split_1;
                }
            }

            local_bucket = getBucket(key);
            index = getBucketIndex(HashKey(key));
        }

        bucketDirectory[index]->contents[key] = value;
    }    
    
    template<typename K, typename V>
    int ExtendibleHash<K, V>::getBucketIndex(size_t hashKey) const {
        return static_cast<int> (hashKey & ((1 << globalDepth) - 1)); //hashKey % 2^globalDepth
    }

    template<typename K, typename V>
    std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> ExtendibleHash<K, V>::getBucket(const K &key) {
        size_t hash_key = HashKey(key);
        size_t bucket_id = getBucketIndex(hash_key);
        return bucketDirectory[bucket_id];
    };

    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace cmudb
