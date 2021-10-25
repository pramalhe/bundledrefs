/* 
 * File:   dummy_list.h
 */

#pragma once
#include "errors.h"

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
    // define BEFORE including rq_provider.h
    #define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif
//#include "rq_provider.h"

template <typename K, typename V>
class node_t;
#define nodeptr node_t<K,V> *

template <typename K, typename V>
class node_t {
public:
    K key;
    std::atomic<V> val;
    std::atomic<node_t*> next;

};

#ifndef casword_t
#define casword_t intptr_t
#endif

template <typename K, typename V, class RecManager>
class dummylist {
private:
    RecManager * const recmgr;
    //RQProvider<K, V, node_t<K,V>, lflist<K,V,RecManager>, RecManager, true, true> * rqProvider;
#ifdef USE_DEBUGCOUNTERS
    debugCounters * const counters;
#endif
    nodeptr head;

    nodeptr new_node(const int tid, const K& key, const V& val, nodeptr next);
    long long debugKeySum(nodeptr head);

    V doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent);
    
    int init[MAX_TID_POW2] = {0,};

public:
    const K KEY_MIN;
    const K KEY_MAX;
    const V NO_VALUE;
    dummylist(int numProcesses, const K KEY_MIN, const K KEY_MAX, const V NO_VALUE);
    ~dummylist();
    bool contains(const int tid, const K& key);
    V insert(const int tid, const K& key, const V& value);
    V insertIfAbsent(const int tid, const K& key, const V& value);
    V erase(const int tid, const K& key);
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues);
    
    /**
     * This function must be called once by each thread that will
     * invoke any functions on this class.
     * 
     * It must be okay that we do this with the main thread and later with another thread!!!
     */
    void initThread(const int tid);
    void deinitThread(const int tid);
#ifdef USE_DEBUGCOUNTERS
    debugCounters * debugGetCounters() { return counters; }
    void clearCounters() { counters->clear(); }
#endif
    long long debugKeySum();
//    void validateRangeQueries(const long long prefillKeyChecksum) {
//        rqProvider->validateRQs(prefillKeyChecksum);
//    }
    bool validate(const long long keysum, const bool checkkeysum);
    long long getSize();
    long long getSizeInNodes();
    string getSizeString() {
        stringstream ss;
        ss<<getSizeInNodes()<<" nodes in data structure";
        return ss.str();
    }
    RecManager * debugGetRecMgr() {
        return recmgr;
    }
    
    inline int getKeys(const int tid, node_t<K,V> * node, K * const outputKeys, V * const outputValues){
        //ignore marked
        outputKeys[0] = node->key;
        outputValues[0] = node->val;
        return 1;
    }
    
    inline bool isInRange(const K& key, const K& lo, const K& hi) {
        return (lo <= key && key <= hi);
    }
    inline bool isLogicallyDeleted(const int tid, node_t<K,V> * node);
    
    inline bool isLogicallyInserted(const int tid, node_t<K,V> * node) {
        return true;
    }

    node_t<K,V> * debug_getEntryPoint() { return head; }
};

