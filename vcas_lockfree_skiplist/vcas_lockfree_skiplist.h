#ifndef VCAS_LOCKFREE_SKIPLIST_H
#define VCAS_LOCKFREE_SKIPLIST_H

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
// define BEFORE including rq_provider.h
#define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif
#include "rq_provider.h"
#include "random.h"
#include "plaf.h"

namespace vcas_lockfree_skiplist {
using namespace std;

/////////////////////////////////////////////////////////
// DEFINES
/////////////////////////////////////////////////////////
#define SKIP_LIST_MAX_LEVEL (20)

#define SKIP_LIST_MARK_MASK 0x1

/////////////////////////////////////////////////////////
// TYPES
/////////////////////////////////////////////////////////
template <typename K, typename V>
class node_t {
 public:
  union {
    struct {
      //volatile long lock;
      volatile K key;
      volatile V val;
      volatile int topLevel;

      //vcas_obj_t<long long>* volatile marked;
      volatile long long fullyLinked;  
      //vcas_obj_t<long long>* volatile fullyLinked;
      vcas_obj_t<node_t<K, V>*>* volatile p_next[SKIP_LIST_MAX_LEVEL];
    };
  };

};

#define nodeptr node_t<K, V>*

template <typename K, typename V, class RecManager>
class skiplist {
 private:
  volatile char padding0[PREFETCH_SIZE_BYTES];
  nodeptr volatile p_head;
  // volatile char padding1[PREFETCH_SIZE_BYTES];
  nodeptr volatile p_tail;
  volatile char padding2[PREFETCH_SIZE_BYTES];

  const int NUM_PROCESSES;
  RecManager* const recmgr;
  Random* const
      threadRNGs;  // threadRNGs[tid * PREFETCH_SIZE_WORDS] = rng for thread tid
  RQProvider<K, V, node_t<K, V>, skiplist<K, V, RecManager>, RecManager, true,
             false>* rqProvider;
#ifdef USE_DEBUGCOUNTERS
  debugCounters* const counters;
#endif

  nodeptr allocateNode(const int tid);

  void initNode(const int tid, nodeptr p_node, K key, V value, int height);
  int find_impl(const int tid, K key, nodeptr* p_preds, nodeptr* p_succs,
                nodeptr* p_found);
  V doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent);

  int init[MAX_TID_POW2] = {
      0,
  };

 public:
  const K KEY_MIN;
  const K KEY_MAX;
  const V NO_VALUE;
  volatile char padding3[PREFETCH_SIZE_BYTES];

  skiplist(const int numProcesses, const K _KEY_MIN, const K _KEY_MAX,
           const V NO_VALUE, Random* const threadRNGs);
  ~skiplist();
  
  bool mark_skiplist_node(const int tid, nodeptr ptr);

  bool contains(const int tid, K key);
  const pair<V, bool> find(const int tid, const K& key);
  V insert(const int tid, const K& key, const V& value) {
    return doInsert(tid, key, value, false);
  }
  V insertIfAbsent(const int tid, const K& key, const V& value) {
    return doInsert(tid, key, value, true);
  }
  V erase(const int tid, const K& key);
  int rangeQuery(const int tid, const K& lo, const K& hi, K* const resultKeys,
                 V* const resultValues);

  void initThread(const int tid);
  void deinitThread(const int tid);
#ifdef USE_DEBUGCOUNTERS
  debugCounters* debugGetCounters() { return counters; }
  void clearCounters() { counters->clear(); }
#endif

  bool isMarked(nodeptr ptr);
  nodeptr getUnmarked(nodeptr ptr);
  nodeptr getMarked(nodeptr ptr);
  
  long long getSizeInNodes() {
    long long size = 0;
    const int dummyTid = 0;
    for (nodeptr curr = getUnmarked(rqProvider->read_vcas_unsafe(dummyTid, p_head->p_next[0]));
         curr->key != KEY_MAX;
         curr = getUnmarked(rqProvider->read_vcas_unsafe(dummyTid, curr->p_next[0]))) {
      ++size;
    }
    return size;
  }
  // warning: this can only be used when there are no other threads accessing
  // the data structure
  long long getSize() {
    long long size = 0;
    const int dummyTid = 0;
    for (nodeptr curr = getUnmarked(rqProvider->read_vcas_unsafe(dummyTid, p_head->p_next[0]));
         curr->key != KEY_MAX;
         curr = getUnmarked(rqProvider->read_vcas_unsafe(dummyTid, curr->p_next[0]))) {
      //if (isMarked(rqProvider->read_vcas_unsafe(dummyTid, curr->p_next[0])) == false)
        size ++;
    }
    return size;
  }
  
  string getSizeString() {
    stringstream ss;
    ss << getSizeInNodes() << " nodes in data structure";
    return ss.str();
  }

  RecManager* const debugGetRecMgr() { return recmgr; }

  inline int getKeys(const int tid, nodeptr node, K* const outputKeys,
                     V* const outputValues, const int ts) {
    outputKeys[0] = node->key;
    outputValues[0] = node->val;
    return 1;
    return 0;
  }

  bool isInRange(const K& key, const K& lo, const K& hi) {
    return (lo <= key && key <= hi);
  }
  inline bool isLogicallyDeleted(const int tid, nodeptr node) {
    return (isMarked(rqProvider->read_vcas_unsafe(0, node->p_next[0])));
  }

  inline bool isLogicallyInserted(const int tid, nodeptr node) {
    return (node->fullyLinked == 1);
  }
  
 private:
  // warning: this can only be used when there are no other threads accessing
  // the data structure
  long long debugKeySum(nodeptr head) {
    long long result = 0;
    const int dummyTid = 0;
    // traverse lowest level
    nodeptr curr = getUnmarked(rqProvider->read_vcas_unsafe(dummyTid, p_head->p_next[0]));
    while (curr->key < KEY_MAX) {
      //if (isMarked(rqProvider->read_vcas_unsafe(dummyTid, curr->p_next[0])) == false) {
        result += curr->key;
      //}
      curr = getUnmarked(rqProvider->read_vcas_unsafe(dummyTid, curr->p_next[0]));
    }
    return result;
  }

 public:

  bool validate(const long long keysum, const bool checkkeysum) { 
    if (checkkeysum) 
      return (debugKeySum(p_head) == keysum);
  }

  node_t<K, V>* debug_getEntryPoint() { return p_head; }


  long long debugKeySum() { return debugKeySum(p_head); }
};
}  // namespace vcas_skiplist_lock
#endif  // SKIPLIST_H