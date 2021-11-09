/*
 * File:   skiplist_lock_impl.h
 * Author: Trevor Brown and Maya Arbel-Raviv
 *
 * This is a heavily modified version of the skip-list packaged with StackTrack
 * (by Alistarh et al.)
 *
 * Created on August 6, 2017, 5:25 PM
 */

#ifndef LOCKFREE_SKIPLIST_IMPL_H
#define LOCKFREE_SKIPLIST_IMPL_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utility>

#include "vcas_lockfree_skiplist.h"

// #define CAS __sync_val_compare_and_swap
#define BOOL_CAS __sync_bool_compare_and_swap
#define likely
#define unlikely
#define CPU_RELAX

namespace vcas_lockfree_skiplist {
using namespace std;

template <typename K, typename V, class RecordMgr>
bool skiplist<K, V, RecordMgr>::isMarked(nodeptr ptr){
    return ((intptr_t) ptr & SKIP_LIST_MARK_MASK);
}

template <typename K, typename V, class RecordMgr>
nodeptr skiplist<K, V, RecordMgr>::getUnmarked(nodeptr ptr) {
    return (nodeptr)((intptr_t) ptr & ~SKIP_LIST_MARK_MASK);
}

template <typename K, typename V, class RecordMgr>
nodeptr skiplist<K, V, RecordMgr>::getMarked(nodeptr ptr) {
    return (nodeptr)((intptr_t)ptr | SKIP_LIST_MARK_MASK);
}

template <typename K, typename V, class RecordMgr>
bool skiplist<K, V, RecordMgr>::mark_skiplist_node(const int tid, nodeptr ptr){
    nodeptr before = NULL;
    nodeptr after = NULL;
    bool result = false;      
    
    for (int i = ptr->topLevel; i >= 0; i--) {
      do {
            before = rqProvider->read_vcas(tid, ptr->p_next[i]);
            if (isMarked(before)) {
                result = false;
                break;
            }
            after = getMarked(before);
            //result = BOOL_CAS(&(ptr->p_next[i]), (intptr_t)before, (intptr_t)after);
            result = rqProvider->cas_vcas(tid, &ptr->p_next[i], before, after);
      } while (!result);        
    }
    
    return result;
}


static int sl_randomLevel(const int tid, Random* const threadRNGs) {
  unsigned int v =
      threadRNGs[tid * PREFETCH_SIZE_WORDS]
          .nextNatural();  // 32-bit word input to count zero bits on right
  unsigned int c = 32;     // c will be the number of zero bits on the right
  v &= -signed(v);
  if (v) c--;
  if (v & 0x0000FFFF) c -= 16;
  if (v & 0x00FF00FF) c -= 8;
  if (v & 0x0F0F0F0F) c -= 4;
  if (v & 0x33333333) c -= 2;
  if (v & 0x55555555) c -= 1;
  return (c < SKIP_LIST_MAX_LEVEL) ? c : SKIP_LIST_MAX_LEVEL - 1;
}

template <typename K, typename V, class RecordMgr>
void skiplist<K, V, RecordMgr>::initNode(const int tid, nodeptr p_node, K key,
                                         V value, int height) {
  rqProvider->init_node(tid, p_node);
  p_node->key = key;
  p_node->val = value;
  p_node->topLevel = height;
  //p_node->lock = 0;

  // Allocate initial vcas objects, but leave as TBD
  //p_node->marked = new vcas_obj_t<long long>(0, nullptr);
  //rqProvider->write_vcas(tid, p_node->marked, 0ll);
  p_node->fullyLinked = 0;
  //rqProvider->write_vcas(tid, p_node->fullyLinked, 0ll);
  for (int level = 0; level <= height; ++level) {
    p_node->p_next[level] = new vcas_obj_t<nodeptr>(nullptr, nullptr);
  }
}

template <typename K, typename V, class RecordMgr>
nodeptr skiplist<K, V, RecordMgr>::allocateNode(const int tid) {
  nodeptr nnode = recmgr->template allocate<node_t<K, V> >(tid);
  if (nnode == NULL) {
    cout << "ERROR: out of memory" << endl;
    exit(-1);
  }
  return nnode;
}

template <typename K, typename V, class RecordMgr>
int skiplist<K, V, RecordMgr>::find_impl(const int tid, K key, nodeptr* p_preds, nodeptr* p_succs, nodeptr* p_found) {
    int level;
    int l_found = -1;
    nodeptr p_pred = NULL;
    nodeptr p_curr = NULL;
    nodeptr p_pred_next = NULL;
    nodeptr p_curr_next = NULL;

retry: 
    l_found = -1;   
    p_pred = p_head;

    for (level = SKIP_LIST_MAX_LEVEL - 1; level >= 0; level--) {
        p_pred_next = rqProvider->read_vcas(tid, p_pred->p_next[level]);
        if (isMarked(p_pred_next))
          goto retry;
          
        for (p_curr = p_pred_next;; p_curr = p_curr_next)
        {
            p_curr_next = rqProvider->read_vcas(tid, p_curr->p_next[level]);
            while (isMarked(p_curr_next)) {
              p_curr = getUnmarked(p_curr_next);
              p_curr_next = rqProvider->read_vcas(tid, p_curr->p_next[level]);
            }
            if (p_curr->key >= key)
                break;
            p_pred = p_curr;
            p_pred_next = p_curr_next;
        } 
        
        //if (p_curr != p_pred_next && BOOL_CAS(&(p_pred->p_next[level]), (intptr_t)p_pred_next, (intptr_t)p_curr) == false) 
        if (p_curr != p_pred_next && rqProvider->cas_vcas(tid, &p_pred->p_next[level], p_pred_next, p_curr) == false)
          goto retry;        

        if (l_found == -1 && key == p_curr->key) {
            l_found = level;
        }
        
        p_preds[level] = p_pred;
        p_succs[level] = p_curr;
    }
    if (p_found) *p_found = p_curr;
    return l_found;
}

template <typename K, typename V, class RecManager>
skiplist<K, V, RecManager>::skiplist(const int numProcesses, const K _KEY_MIN,
                                     const K _KEY_MAX, const V NO_VALUE,
                                     Random* const threadRNGs)
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
      new RQProvider<K, V, node_t<K, V>, skiplist<K, V, RecManager>, RecManager,
                     true, false>(numProcesses, this, recmgr);

  // note: initThread calls rqProvider->initThread

  int i;
  const int dummyTid = 0;
  recmgr->initThread(dummyTid);

  p_head = allocateNode(dummyTid);
  initNode(dummyTid, p_head, KEY_MIN, NO_VALUE, SKIP_LIST_MAX_LEVEL - 1);

  p_tail = allocateNode(dummyTid);
  initNode(dummyTid, p_tail, KEY_MAX, NO_VALUE, SKIP_LIST_MAX_LEVEL - 1);

  // Initialize next pointers.
  for (i = 0; i < SKIP_LIST_MAX_LEVEL; i++) {
    rqProvider->write_vcas(dummyTid, p_head->p_next[i], p_tail);
    rqProvider->write_vcas(dummyTid, p_tail->p_next[i], (nodeptr) nullptr);
  }
  p_tail->fullyLinked = 1;
  p_head->fullyLinked = 1;
}

template <typename K, typename V, class RecManager>
skiplist<K, V, RecManager>::~skiplist() {
  const int dummyTid = 0;
  nodeptr curr = p_head;
  while (curr->key < KEY_MAX) {
    auto tmp = curr;
    curr = getUnmarked(rqProvider->read_vcas(dummyTid, curr->p_next[0]));
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
void skiplist<K, V, RecManager>::initThread(const int tid) {
  if (init[tid])
    return;
  else
    init[tid] = !init[tid];

  recmgr->initThread(tid);
  rqProvider->initThread(tid);
}

template <typename K, typename V, class RecManager>
void skiplist<K, V, RecManager>::deinitThread(const int tid) {
  if (!init[tid])
    return;
  else
    init[tid] = !init[tid];

  recmgr->deinitThread(tid);
  rqProvider->deinitThread(tid);
}

template <typename K, typename V, class RecManager>
bool skiplist<K, V, RecManager>::contains(const int tid, K key) {
    nodeptr p_preds[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_found = NULL;
    int lFound;
    bool res;
    recmgr->leaveQuiescentState(tid, true);
    lFound = find_impl(tid, key, p_preds, p_succs, &p_found);
    res = (lFound != -1) && !isMarked(rqProvider->read_vcas(tid, p_succs[lFound]->p_next[0]));
    recmgr->enterQuiescentState(tid);
    return res;
}

template <typename K, typename V, class RecManager>
const pair<V, bool> skiplist<K, V, RecManager>::find(const int tid, const K& key) {
    nodeptr p_preds[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_found = NULL;
    int lFound;
    bool res;
    recmgr->leaveQuiescentState(tid, true);
    lFound = find_impl(tid, key, p_preds, p_succs, &p_found);
    res = (lFound != -1) && !isMarked(rqProvider->read_vcas(tid, p_succs[lFound]->p_next[0]));
    recmgr->enterQuiescentState(tid);
    if (res) {
        return pair<V,bool>(p_found->val, true);
    } else {
        return pair<V,bool>(NO_VALUE, false);
    }
}

template <typename K, typename V, class RecManager>
V skiplist<K, V, RecManager>::doInsert(const int tid, const K& key,
                                       const V& value, bool onlyIfAbsent) {
    nodeptr p_preds[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_node_found = NULL;
    nodeptr p_pred = NULL;
    nodeptr p_succ = NULL;
    nodeptr p_new_node = NULL;
    nodeptr p_new_node_next = NULL;
    V ret = NO_VALUE;
    int level;
    int topLevel = -1;
    int lFound = -1;
    int done = 0;
    bool allocated = false;

    topLevel = sl_randomLevel(tid, threadRNGs);
    while (!done) {
        recmgr->leaveQuiescentState(tid);
        lFound = find_impl(tid, key, p_preds, p_succs, NULL);
        
        if (lFound != -1) {
        
            if (allocated == true) {
              recmgr->retire(tid, p_new_node);
            }

            if (onlyIfAbsent) {
                ret = p_succs[lFound]->val; 
                recmgr->enterQuiescentState(tid);
                return ret;
            } else {
               cout<<"ERROR: insert-replace functionality not implemented for lockfree_skiplist_impl"<<endl;
               exit(-1);
            }
        }
        
        if (allocated == false) {
          p_new_node = allocateNode(tid); 
#ifdef __HANDLE_STATS
          GSTATS_APPEND(tid, node_allocated_addresses, ((long long) p_new_node)%(1<<12));
#endif
          initNode(tid, p_new_node, key, value, topLevel);
          allocated = true;
        }
        
        for (level = 0; level <= topLevel; level++) {
            //p_new_node->p_next[level] = p_succs[level];
            rqProvider->write_vcas(tid, p_new_node->p_next[level], p_succs[level]);
        }
        
        //if (BOOL_CAS(&(p_preds[0]->p_next[0]), (intptr_t)p_succs[0], (intptr_t)p_new_node) == false) {
        if (!rqProvider->cas_vcas(tid, &(p_preds[0]->p_next[0]), p_succs[0], p_new_node)) {
          recmgr->enterQuiescentState(tid);
          continue;
        }
        
        // inserting the higher levels
        for (int i = 1; i <= topLevel; i++)
        {
            while (true)
            {
                p_pred = p_preds[i];
                p_succ = p_succs[i];
                p_new_node_next = rqProvider->read_vcas(tid, p_new_node->p_next[i]);
                if (isMarked(p_new_node_next))
                  break;                  

                if (p_succ != p_new_node_next && !rqProvider->cas_vcas(tid, &(p_new_node->p_next[i]), p_new_node_next, p_succ))
                  break;

                if (rqProvider->cas_vcas(tid, &(p_pred->p_next[i]), p_succ, p_new_node))
                  break;

                find_impl(tid, key, p_preds, p_succs, NULL);
            }
        }
        

        
        if (!BOOL_CAS(&(p_new_node->fullyLinked), 0, 1)) {
          find_impl(tid, key, p_preds, p_succs, NULL);
          recmgr->retire(tid, p_new_node);
        }
        
#ifdef __HANDLE_STATS
            GSTATS_ADD_IX(tid, skiplist_inserted_on_level, 1, topLevel);
#endif

        done = 1;
        recmgr->enterQuiescentState(tid);
    }
    return ret;
}

template <typename K, typename V, class RecManager>
V skiplist<K, V, RecManager>::erase(const int tid, const K& key) {
    nodeptr p_preds[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIP_LIST_MAX_LEVEL] = {0,};
    nodeptr p_victim = NULL;
    int lFound = -1;
    V ret = NO_VALUE;
    
    
    recmgr->leaveQuiescentState(tid);
    lFound = find_impl(tid, key, p_preds, p_succs, NULL);
    if (lFound == -1) {
        recmgr->enterQuiescentState(tid);
        return ret;
    }
    p_victim = p_succs[lFound];
    bool result = mark_skiplist_node(tid, p_victim);
    if (result) {
      ret = p_victim->val;
      if (BOOL_CAS(&(p_victim->fullyLinked), 0, 1) == false) {
        find_impl(tid, key, p_preds, p_succs, NULL);
        recmgr->retire(tid, p_victim);
      }
    }
    
    recmgr->enterQuiescentState(tid);
    return ret;
}

template <typename K, typename V, class RecManager>
int skiplist<K, V, RecManager>::rangeQuery(const int tid, const K& lo,
                                           const K& hi, K* const resultKeys,
                                           V* const resultValues) {

  recmgr->leaveQuiescentState(tid, true);
  int ts = rqProvider->traversal_start(tid);
  int cnt = 0;
  nodeptr pred = p_head;
  nodeptr curr = NULL;
  for (int level = SKIP_LIST_MAX_LEVEL - 1; level >= 0; level--) {
    curr = getUnmarked(rqProvider->read_vcas(tid, pred->p_next[level], ts));
    while (curr->key < lo) {
      pred = curr;
      curr = getUnmarked(rqProvider->read_vcas(tid, pred->p_next[level], ts));
      //            nodesSkipped++;
    }
  }
  // continue until we pass the high key
  while (curr->key <= hi) {
    rqProvider->traversal_try_add(tid, curr, resultKeys, resultValues, &cnt, lo,
                                  hi, ts);
    curr = getUnmarked(rqProvider->read_vcas(tid, curr->p_next[0]));
  }
  rqProvider->traversal_end(tid, resultKeys, resultValues, &cnt, lo, hi);
#ifdef SNAPCOLLECTOR_PRINT_RQS
  cout << "rqSize=" << cnt << endl;
#endif

  recmgr->enterQuiescentState(tid);
  return cnt;
}
}  
#endif 
