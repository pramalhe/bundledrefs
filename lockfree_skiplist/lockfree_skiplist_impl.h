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

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

#include "lockfree_skiplist.h"


#define BOOL_CAS __sync_bool_compare_and_swap
#define CAS __sync_val_compare_and_swap
#define likely 
#define unlikely 
#define CPU_RELAX

template<typename K, typename V>
inline bool isMarked(nodeptr ptr){
    return ((intptr_t) ptr & SKIP_LIST_MARK_MASK);
}
template<typename K, typename V>
inline nodeptr getUnmarked(nodeptr ptr) {
    return (nodeptr)((intptr_t) ptr & ~SKIP_LIST_MARK_MASK);
}
template<typename K, typename V>
inline nodeptr getMarked(nodeptr ptr) {
    return (nodeptr)((intptr_t)ptr | SKIP_LIST_MARK_MASK);
}

template<typename K, typename V>
inline bool mark_sl_node(nodeptr ptr){
    nodeptr before = NULL;
    nodeptr after = NULL;
    bool result = false;      
    
    for (int i = ptr->topLevel; i >= 0; i--) {
      do {
            before = ptr->p_next[i];
            if (isMarked(before)) {
                result = false;
                break;
            }
            after = getMarked(before);
            result = BOOL_CAS(&(ptr->p_next[i]), (intptr_t)before, (intptr_t)after);
      } while (!result);        
    }
    
    return result;
}

static int sl_randomLevel(const int tid, Random * const threadRNGs) {
    unsigned int v = threadRNGs[tid*PREFETCH_SIZE_WORDS].nextNatural();         // 32-bit word input to count zero bits on right
    unsigned int c = 32;                                                        // c will be the number of zero bits on the right
    v &= -signed(v);
    if (v) c--;
    if (v & 0x0000FFFF) c -= 16;
    if (v & 0x00FF00FF) c -= 8;
    if (v & 0x0F0F0F0F) c -= 4;
    if (v & 0x33333333) c -= 2;
    if (v & 0x55555555) c -= 1;
    return (c < SKIPLIST_MAX_LEVEL) ? c : SKIPLIST_MAX_LEVEL-1;
}

template <typename K, typename V, class RecordMgr>
void skiplist<K,V,RecordMgr>::initNode(const int tid, nodeptr p_node, K key, V value, int height) {
    rqProvider->init_node(tid, p_node);
    p_node->key = key;
    p_node->val = value;
    p_node->topLevel = height;
    rqProvider->write_addr(tid, &p_node->marked, (long long) 0);
    rqProvider->write_addr(tid, &p_node->fullyLinked, (long long) 0);
    rqProvider->write_addr(tid, (long long *) p_node->p_next, (long long) 0);
}

template <typename K, typename V, class RecordMgr>
nodeptr skiplist<K,V,RecordMgr>::allocateNode(const int tid) {
    nodeptr nnode = recmgr->template allocate<node_t<K,V> >(tid);
    if (nnode == NULL) {
        cout<<"ERROR: out of memory"<<endl;
        exit(-1);
    }
    return nnode;
}

template <typename K, typename V, class RecordMgr>
int skiplist<K,V,RecordMgr>::find_impl(const int tid, K key, nodeptr* p_preds, nodeptr* p_succs, nodeptr* p_found) {
    int level;
    int l_found = -1;
    nodeptr p_pred = NULL;
    nodeptr p_curr = NULL;
    nodeptr p_pred_next = NULL;
    nodeptr p_curr_next = NULL;

retry: 
    l_found = -1;   
    p_pred = p_head;

    for (level = SKIPLIST_MAX_LEVEL - 1; level >= 0; level--) {
        p_pred_next = p_pred->p_next[level];
        if (isMarked(p_pred_next))
          goto retry;
          
        for (p_curr = p_pred_next;; p_curr = p_curr_next)
        {
            p_curr_next = p_curr->p_next[level];
            while (isMarked(p_curr_next)) {
              p_curr = getUnmarked(p_curr_next);
              p_curr_next = p_curr->p_next[level];
            }
            if (p_curr->key >= key)
                break;
            p_pred = p_curr;
            p_pred_next = p_curr_next;
        } 
        
        if (p_curr != p_pred_next && BOOL_CAS(&(p_pred->p_next[level]), (intptr_t)p_pred_next, (intptr_t)p_curr) == false) 
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
skiplist<K,V,RecManager>::skiplist(const int numProcesses,  const K _KEY_MIN, const K _KEY_MAX, const V NO_VALUE, Random * const threadRNGs)
: NUM_PROCESSES(numProcesses)
, recmgr(new RecManager(numProcesses, 0))
, threadRNGs(threadRNGs)
#ifdef USE_DEBUGCOUNTERS
, counters(new debugCounters(numProcesses))
#endif
, KEY_MIN(_KEY_MIN)
, KEY_MAX(_KEY_MAX)
, NO_VALUE(NO_VALUE) {
    rqProvider = new RQProvider<K, V, node_t<K,V>, skiplist<K,V,RecManager>, RecManager, true, false>(numProcesses, this, recmgr);
    
    // note: initThread calls rqProvider->initThread
    
    int i;
    const int dummyTid = 0;
    recmgr->initThread(dummyTid);

    p_head = allocateNode(dummyTid);
    initNode(dummyTid, p_head, KEY_MIN, NO_VALUE, SKIPLIST_MAX_LEVEL - 1);

    p_tail = allocateNode(dummyTid);
    initNode(dummyTid, p_tail, KEY_MAX, NO_VALUE, SKIPLIST_MAX_LEVEL - 1);

    for (i = 0; i < SKIPLIST_MAX_LEVEL; i++) {
        p_head->p_next[i] = p_tail;
    }
}

template <typename K, typename V, class RecManager>
skiplist<K,V,RecManager>::~skiplist() {
    const int dummyTid = 0;
    nodeptr curr = p_head;
    while (curr->key < KEY_MAX) {
        auto tmp = curr;
        curr = getUnmarked(curr->p_next[0]);
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
void skiplist<K,V,RecManager>::initThread(const int tid) {
    if (init[tid]) return; else init[tid] = !init[tid];

    recmgr->initThread(tid);
    rqProvider->initThread(tid);
}

template <typename K, typename V, class RecManager>
void skiplist<K,V,RecManager>::deinitThread(const int tid) {
    if (!init[tid]) return; else init[tid] = !init[tid];

    recmgr->deinitThread(tid);
    rqProvider->deinitThread(tid);
}

template <typename K, typename V, class RecManager>
bool skiplist<K,V,RecManager>::contains(const int tid, K key) {
    nodeptr p_preds[SKIPLIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIPLIST_MAX_LEVEL] = {0,};
    nodeptr p_found = NULL;
    int lFound;
    bool res;
    recmgr->leaveQuiescentState(tid, true);
    lFound = find_impl(tid, key, p_preds, p_succs, &p_found);
    res = (lFound != -1) && !isMarked(p_succs[lFound]->p_next[0]);
    recmgr->enterQuiescentState(tid);
    return res;
}

template <typename K, typename V, class RecManager>
const pair<V,bool> skiplist<K,V,RecManager>::find(const int tid, const K& key) {
    nodeptr p_preds[SKIPLIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIPLIST_MAX_LEVEL] = {0,};
    nodeptr p_found = NULL;
    int lFound;
    bool res;
    recmgr->leaveQuiescentState(tid, true);
    lFound = find_impl(tid, key, p_preds, p_succs, &p_found);
    res = (lFound != -1) && !isMarked(p_succs[lFound]->p_next[0]);
    recmgr->enterQuiescentState(tid);
    if (res) {
        return pair<V,bool>(p_found->val, true);
    } else {
        return pair<V,bool>(NO_VALUE, false);
    }
}

template <typename K, typename V, class RecManager>
V skiplist<K,V,RecManager>::doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent) {
    nodeptr p_preds[SKIPLIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIPLIST_MAX_LEVEL] = {0,};
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
            p_new_node->p_next[level] = p_succs[level];
        }
        
        if (BOOL_CAS(&(p_preds[0]->p_next[0]), (intptr_t)p_succs[0], (intptr_t)p_new_node) == false) {
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
                p_new_node_next = p_new_node->p_next[i];
                if (isMarked(p_new_node_next))
                  break;                  

                if (p_succ != p_new_node_next && BOOL_CAS(&(p_new_node->p_next[i]), (intptr_t)p_new_node_next, (intptr_t)p_succ) == false)
                  break;

                if (BOOL_CAS(&(p_pred->p_next[i]), (intptr_t)p_succ, (intptr_t)p_new_node) == true)
                  break;

                find_impl(tid, key, p_preds, p_succs, NULL);
            }
        }
        

        
        if (BOOL_CAS(&(p_new_node->fullyLinked), 0, 1) == false) {
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
V skiplist<K,V,RecManager>::erase(const int tid, const K& key) {
    nodeptr p_preds[SKIPLIST_MAX_LEVEL] = {0,};
    nodeptr p_succs[SKIPLIST_MAX_LEVEL] = {0,};
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
    bool result = mark_sl_node(p_victim);
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
int skiplist<K,V,RecManager>::rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
    recmgr->leaveQuiescentState(tid, true);
    rqProvider->traversal_start(tid);
    int cnt = 0;
    nodeptr pred = p_head;
    nodeptr curr = NULL;
    for (int level = SKIPLIST_MAX_LEVEL - 1; level >= 0; level--) {
        curr = getUnmarked(pred->p_next[level]);
        while (curr->key < lo) {
            pred = curr;
            curr = getUnmarked(pred->p_next[level]);
        }
    }
    
    // continue until we pass the high key
    while (curr->key <= hi) {
        rqProvider->traversal_try_add(tid, curr, resultKeys, resultValues, &cnt, lo, hi);
        curr = getUnmarked(curr->p_next[0]);
    }
    rqProvider->traversal_end(tid, resultKeys, resultValues, &cnt, lo, hi);
    
    recmgr->enterQuiescentState(tid);
    return cnt;
}

#endif /* LOCKFREE_SKIPLIST_IMPL_H */

