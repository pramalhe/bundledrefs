

#ifndef LOCKFREE_LIST_IMPL_H
#define LOCKFREE_LIST_IMPL_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utility>

#include "vcas_lockfree_list.h"

// #define CAS __sync_val_compare_and_swap
#define BOOL_CAS __sync_bool_compare_and_swap
#define likely
#define unlikely
#define CPU_RELAX

namespace vcas_lockfree_list {
using namespace std;

template <typename K, typename V, class RecordMgr>
bool list<K, V, RecordMgr>::isMarked(nodeptr ptr){
    return ((intptr_t) ptr & LIST_MARK_MASK);
}

template <typename K, typename V, class RecordMgr>
nodeptr list<K, V, RecordMgr>::getUnmarked(nodeptr ptr) {
    return (nodeptr)((intptr_t) ptr & ~LIST_MARK_MASK);
}

template <typename K, typename V, class RecordMgr>
nodeptr list<K, V, RecordMgr>::getMarked(nodeptr ptr) {
    return (nodeptr)((intptr_t)ptr | LIST_MARK_MASK);
}

template <typename K, typename V, class RecordMgr>
bool list<K, V, RecordMgr>::mark_list_node(const int tid, nodeptr ptr){
    nodeptr before = NULL;
    nodeptr after = NULL;
    bool result = false;      
    
    do {
          before = rqProvider->read_vcas(tid, ptr->p_next);
          if (isMarked(before)) {
              result = false;
              break;
          }
          after = getMarked(before);
          result = rqProvider->cas_vcas(tid, &ptr->p_next, before, after);
    } while (!result); 
    
    return result;
}

template <typename K, typename V, class RecordMgr>
void list<K, V, RecordMgr>::initNode(const int tid, nodeptr p_node, K key, V value) {
  rqProvider->init_node(tid, p_node);
  p_node->key = key;
  p_node->val = value;
  p_node->p_next = new vcas_obj_t<nodeptr>(nullptr, nullptr);
}

template <typename K, typename V, class RecordMgr>
nodeptr list<K, V, RecordMgr>::allocateNode(const int tid) {
  nodeptr nnode = recmgr->template allocate<node_t<K, V> >(tid);
  if (nnode == NULL) {
    cout << "ERROR: out of memory" << endl;
    exit(-1);
  }
  return nnode;
}

template <typename K, typename V, class RecordMgr>
bool list<K, V, RecordMgr>::find_impl(const int tid, K key, nodeptr* p_pred, nodeptr* p_succ) {


    nodeptr pred = NULL;
    nodeptr curr = NULL;
    nodeptr pred_next = NULL;
    nodeptr curr_next = NULL;

retry: 
  
    pred = p_head;

    pred_next = rqProvider->read_vcas(tid, pred->p_next);
    if (isMarked(pred_next))
      goto retry;
    curr = pred_next;
      
    while (true)
    {
        curr_next = rqProvider->read_vcas(tid, curr->p_next);
        while (isMarked(curr_next)) {
          curr = getUnmarked(curr_next);
          curr_next = rqProvider->read_vcas(tid, curr->p_next);
        }
        if (curr->key >= key)
            break;
        pred = curr;
        pred_next = curr_next;
        curr = pred_next;
    } 
    
    if (curr != pred_next && rqProvider->cas_vcas(tid, &pred->p_next, pred_next, curr) == false)
      goto retry;        

    *p_pred = pred;
    *p_succ = curr;

    return (curr->key == key);
}

template <typename K, typename V, class RecManager>
list<K, V, RecManager>::list(const int numProcesses, const K _KEY_MIN, const K _KEY_MAX, const V NO_VALUE, Random* const threadRNGs)
    : NUM_PROCESSES(numProcesses),
      recmgr(new RecManager(numProcesses, 0)),
      threadRNGs(threadRNGs)
#ifdef USE_DEBUGCOUNTERS
      ,
      counters(new debugCounters(numProcesses))
#endif
      ,
      KEY_MIN(_KEY_MIN),
      KEY_MAX(_KEY_MAX),
      NO_VALUE(NO_VALUE) {
  rqProvider =
      new RQProvider<K, V, node_t<K, V>, list<K, V, RecManager>, RecManager,
                     true, false>(numProcesses, this, recmgr);

  // note: initThread calls rqProvider->initThread

  int i;
  const int dummyTid = 0;
  recmgr->initThread(dummyTid);

  p_head = allocateNode(dummyTid);
  initNode(dummyTid, p_head, KEY_MIN, NO_VALUE);

  nodeptr p_tail = allocateNode(dummyTid);
  initNode(dummyTid, p_tail, KEY_MAX, NO_VALUE);

  // Initialize next pointers.
  rqProvider->write_vcas(dummyTid, p_head->p_next, p_tail);
  rqProvider->write_vcas(dummyTid, p_tail->p_next, (nodeptr) nullptr);

}

template <typename K, typename V, class RecManager>
list<K, V, RecManager>::~list() {
  const int dummyTid = 0;
  nodeptr curr = p_head;
  while (curr->key < KEY_MAX) {
    auto tmp = curr;
    curr = getUnmarked(rqProvider->read_vcas(dummyTid, curr->p_next));
    recmgr->retire(dummyTid, tmp);
  }
  recmgr->retire(dummyTid, curr);
  delete rqProvider;
  recmgr->printStatus();
  delete recmgr;
#ifdef USE_DEBUGCOUNTERS
  delete counters;
#endif
}

template <typename K, typename V, class RecManager>
void list<K, V, RecManager>::initThread(const int tid) {
  if (init[tid])
    return;
  else
    init[tid] = !init[tid];

  recmgr->initThread(tid);
  rqProvider->initThread(tid);
}

template <typename K, typename V, class RecManager>
void list<K, V, RecManager>::deinitThread(const int tid) {
  if (!init[tid])
    return;
  else
    init[tid] = !init[tid];

  recmgr->deinitThread(tid);
  rqProvider->deinitThread(tid);
}

template <typename K, typename V, class RecManager>
bool list<K, V, RecManager>::contains(const int tid, K key) {
    nodeptr p_pred;
    nodeptr p_succ;
    nodeptr p_found = NULL;

    bool res;
    recmgr->leaveQuiescentState(tid, true);
    res = find_impl(tid, key, &p_pred, &p_succ) && !isMarked(rqProvider->read_vcas(tid, p_succ->p_next));
    recmgr->enterQuiescentState(tid);
    return res;
}

template <typename K, typename V, class RecManager>
const pair<V, bool> list<K, V, RecManager>::find(const int tid, const K& key) {
    nodeptr p_pred;
    nodeptr p_succ;
    V value = NO_VALUE;
    bool res;
    recmgr->leaveQuiescentState(tid, true);
    res = (find_impl(tid, key, &p_pred, &p_succ) && !isMarked(rqProvider->read_vcas(tid, p_succ->p_next)));
    if (res)
      value = p_succ->val;
    recmgr->enterQuiescentState(tid);
    return pair<V,bool>(value, res);
}

template <typename K, typename V, class RecManager>
V list<K, V, RecManager>::doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent) {
    nodeptr p_pred;
    nodeptr p_succ;
    nodeptr p_new_node = NULL;
    V ret = NO_VALUE;

    
    while (true) {    
      recmgr->leaveQuiescentState(tid);
      if (find_impl(tid, key, &p_pred, &p_succ)) {
        //if (allocated == true) {
          //recmgr->retire(tid, p_new_node);
        //}
        ret = p_succ->val;
        recmgr->enterQuiescentState(tid);
        return ret;
      }
  
      //if (allocated == false) {
        p_new_node = allocateNode(tid); 
//#ifdef __HANDLE_STATS
//        GSTATS_APPEND(tid, node_allocated_addresses, ((long long) p_new_node)%(1<<12));
//#endif
        initNode(tid, p_new_node, key, value);
        //allocated = true;
      //}
      
      rqProvider->write_vcas(tid, p_new_node->p_next, p_succ);
      if (!rqProvider->cas_vcas(tid, &(p_pred->p_next), p_succ, p_new_node)) {
        recmgr->retire(tid, p_new_node);
        recmgr->enterQuiescentState(tid);
        continue;
      } 
      
      recmgr->enterQuiescentState(tid); 
      return ret;    
        
    }

}

template <typename K, typename V, class RecManager>
V list<K, V, RecManager>::erase(const int tid, const K& key) {
    nodeptr p_pred;
    nodeptr p_succ;
    nodeptr victim;

    V ret = NO_VALUE;
    
    
    recmgr->leaveQuiescentState(tid);
    if (!find_impl(tid, key, &p_pred, &p_succ)) {
        recmgr->enterQuiescentState(tid);
        return ret;
    }
    victim = p_succ;
    bool result = mark_list_node(tid, victim);
    if (result) {
      ret = victim->val;
      find_impl(tid, key, &p_pred, &p_succ);
      recmgr->retire(tid, victim);
    }
    
    recmgr->enterQuiescentState(tid);
    return ret;
}

template <typename K, typename V, class RecManager>
int list<K, V, RecManager>::rangeQuery(const int tid, const K& lo,
                                           const K& hi, K* const resultKeys,
                                           V* const resultValues) {
  recmgr->leaveQuiescentState(tid, true);

  int ts = rqProvider->traversal_start(tid);
  int cnt = 0;
  nodeptr pred = p_head;

  nodeptr curr = rqProvider->read_vcas(tid, p_head->p_next, ts);
 
  while (curr->key < lo) {
    pred = curr;
    curr = getUnmarked(rqProvider->read_vcas(tid, pred->p_next, ts)); 

  }

  while (curr->key <= hi) {
    
    rqProvider->traversal_try_add(tid, curr, resultKeys, resultValues, &cnt, lo, hi, ts);
    pred = curr;
    curr = getUnmarked(rqProvider->read_vcas(tid, pred->p_next, ts));
  }


  rqProvider->traversal_end(tid, resultKeys, resultValues, &cnt, lo, hi);
  recmgr->enterQuiescentState(tid);
  return cnt;
}
}  
#endif 
