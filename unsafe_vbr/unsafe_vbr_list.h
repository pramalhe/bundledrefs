#ifndef UNSAFE_VBR_LIST_H_
#define UNSAFE_VBR_LIST_H_

#pragma once
#include "errors.h"
#include <atomic>
#include <iostream>


#include "LocalAllocator.h"
#include "UnsafeVBRNode.h"

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
    // define BEFORE including rq_provider.h
    #define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif


__thread LocalAllocator *localVBRAllocator = nullptr;
__thread uint64_t currVBREpoch = 0;




template <typename K, typename V, class RecManager>
class unsafe_vbr_list {


private:
    RecManager * const recmgr = nullptr;
    

    UnsafeVBRNode<K,V> *head;
    Allocator *globalAllocator;
    
    
    void init(UnsafeVBRNode<K,V> *ptr, K newKey, V newValue, UnsafeVBRNode<K,V> *newNext) {
      readGlobalReclamationEpoch();
      ptr->birthEpoch.store(currVBREpoch);
      ptr->key = newKey;
      ptr->value = newValue; 
      ptr->next.store(integrateEpochIntoPointer(currVBREpoch, newNext));
    }
    

    //nodeptr new_node(const int tid, const K& key, const V& val, nodeptr next);
    
    // warning: this can only be used when there are no other threads accessing the data structure
    long long debugKeySum(UnsafeVBRNode<K,V> *head) {
        long long result = 0;
        UnsafeVBRNode<K,V> *curr = getUnsafeVBRNode(head->next); 
        while (curr->key < KEY_MAX) {
            result += curr->key;
            curr = getUnsafeVBRNode(curr->next); 
        }
        return result;      
    }

    //V doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent);
    
    //int init[MAX_TID_POW2] = {0,};
    
    inline uint64_t getReclamationEpoch(){
      return localVBRAllocator->getEpoch();
	  }
    
    inline UnsafeVBRNode<K,V> *integrateEpochIntoPointer(uint64_t epoch, UnsafeVBRNode<K,V> *ptr) {
      uint64_t version = epoch;
      uint64_t shiftedVersion = version << POINTER_BITS;
      UnsafeVBRNode<K,V> *integrated = (UnsafeVBRNode<K,V> *) ((uintptr_t)(ptr) | shiftedVersion);
      return integrated;
    }
    
    inline UnsafeVBRNode<K,V> *getUnsafeVBRNode(UnsafeVBRNode<K,V> *ptr) {
      UnsafeVBRNode<K,V> *tmp = ptr;
      return (UnsafeVBRNode<K,V> *) ((uintptr_t)(tmp) & ~STATE_MASK);
    }
    
    inline uint64_t getVersion(UnsafeVBRNode<K,V> *ptr) {
      UnsafeVBRNode<K,V> *tmp = ptr;
      uint64_t shiftedVersion = (uintptr_t)tmp & EPOCH_MASK;
      return (shiftedVersion  >> POINTER_BITS);
    }

    inline bool isMarked(UnsafeVBRNode<K,V> *ptr) {
      UnsafeVBRNode<K,V> *tmp = ptr;
      return (bool)((uintptr_t)(tmp) & MARK_BIT);
    }

    inline bool isValidBirthEpoch(uint64_t birthEpoch) {
      return (birthEpoch <= currVBREpoch);
    }
    
    inline bool isValidVersion(uint64_t version) {
      return (version <= currVBREpoch);
    }
 
    inline void readGlobalReclamationEpoch() {   
      currVBREpoch = getReclamationEpoch();
    }
    
    inline bool mark(UnsafeVBRNode<K,V> *victim) {
      UnsafeVBRNode<K,V> *succ = victim->next;
      if (isValidBirthEpoch(victim->birthEpoch) == false) return false;
      if (isMarked(succ)) return false;
      UnsafeVBRNode<K,V> *markedSucc = (UnsafeVBRNode<K,V> *)((uintptr_t)(succ) | MARK_BIT);
      
      if(victim->next.compare_exchange_strong(succ, markedSucc) == true) {
        return true;
      } else {
        return false;
      }
    }

public:
    const K KEY_MIN;
    const K KEY_MAX;
    const V NO_VALUE;

    unsafe_vbr_list(int numProcesses, const K key_min, const K key_max, const V no_value) : KEY_MIN{key_min}, KEY_MAX{key_max}, NO_VALUE{no_value} {
        head = nullptr;
        
        globalAllocator = new Allocator(sizeof(UnsafeVBRNode<K,V>), 2, numProcesses);
        initThread(0);
        
        UnsafeVBRNode<K,V> *tail = (UnsafeVBRNode<K,V> *)localVBRAllocator->alloc();
        init(tail, KEY_MAX, NO_VALUE, nullptr);        
                       
        head = (UnsafeVBRNode<K,V> *)localVBRAllocator->alloc();
        init(head, KEY_MIN, NO_VALUE, tail);
    }

    ~unsafe_vbr_list() {
      cout<<"UnsafeVBRNode size = " << sizeof(UnsafeVBRNode<K,V>) << ". Reclamation epoch = " << getReclamationEpoch() <<endl;
    }
    
  private:

    bool trim(UnsafeVBRNode<K,V> *pred, UnsafeVBRNode<K,V> *curr) {
    
      UnsafeVBRNode<K,V> *predNext = pred->next;
      uint64_t predNextVersion = getVersion(predNext);
      UnsafeVBRNode<K,V> *currNext = curr->next;
      if (isMarked(predNext) || !isMarked(currNext) || getUnsafeVBRNode(predNext) != curr || !isValidVersion(predNextVersion))
        return false;
      if (pred->updateNext(integrateEpochIntoPointer(predNextVersion, curr), integrateEpochIntoPointer(currVBREpoch, getUnsafeVBRNode(currNext)))) {
        localVBRAllocator->retire(curr);
        return true;
      }
      return false;

    }

    UnsafeVBRNode<K,V> *find(K key, UnsafeVBRNode<K,V> **predPtr, uint64_t *predVersion, K *currKey) {
      UnsafeVBRNode<K,V> *pred, *predNext, *predNextRaw, *curr, *currNext;
      uint64_t version, tmpEpoch;
      K tmpKey;
      UnsafeVBRNode<K,V> *tmp;
        
      try_again:

        readGlobalReclamationEpoch();

        pred = head;
        predNextRaw = pred->next;
        predNext = getUnsafeVBRNode(predNextRaw);
        version = getVersion(predNextRaw);
        curr = predNext;
        
        while (true) {
          while (isMarked(curr->next)) {
            currNext = getUnsafeVBRNode(curr->next);
            if (isValidBirthEpoch(curr->birthEpoch) == false) {
              goto try_again;
            }
            curr = currNext;
          }
          
          tmpKey = curr->key;
          if (isValidBirthEpoch(curr->birthEpoch) == false) {
            goto try_again;
          }
          
          if (tmpKey >= key) {
            break;
          }
          
          pred = curr;
          predNextRaw = pred->next;
          predNext = getUnsafeVBRNode(predNextRaw);
          version = getVersion(predNextRaw);
          if (isValidVersion(version) == false || isMarked(predNextRaw)) {
            goto try_again;
          }
          curr = predNext;
        }
        
        if (predNext != curr) {
          if (!trim(pred, predNext)) {
            goto try_again;
          }
          predNextRaw = pred->next;
          version = getVersion(predNextRaw);
          curr = getUnsafeVBRNode(predNextRaw);
          if (isValidVersion(version) == false || isMarked(predNextRaw) || isMarked(curr->next))
            goto try_again;
        }
        
        *predPtr = pred;
        *predVersion = version;
        *currKey = tmpKey;
        return curr;   
    } 

  public:

    V insert(const int tid, const K& key, const V& value) {
      
      UnsafeVBRNode<K,V> *pred, *curr, *next, *origNextV, *optimizedNextV;
      uint64_t predVersion, newNodeTS, initEpoch;
      K currKey;
      
      
      while (true) {
        curr = find(key, &pred, &predVersion, &currKey);
        if (currKey == key) {
          V result = curr->value;
          if (isValidBirthEpoch(curr->birthEpoch) == false)
            continue;
          return result;
        } 
        
        UnsafeVBRNode<K,V> *newNode = (UnsafeVBRNode<K,V> *)localVBRAllocator->alloc();
        init(newNode, key, value, curr);
                 
        if (pred->updateNext(integrateEpochIntoPointer(predVersion, curr), integrateEpochIntoPointer(currVBREpoch, newNode))) {
          return NO_VALUE;
        } else {
          localVBRAllocator->returnAlloc(newNode);
        }
  
          
      }

    }
    
    V insertIfAbsent(const int tid, const K& key, const V& value) {
      return insert(tid, key, value);
    }

    V erase(const int tid, const K& key) {

      UnsafeVBRNode<K,V> *pred, *curr;
      uint64_t predVersion;
      K currKey;
      V result;

      
      while (true) {
        result = NO_VALUE;
        curr = find(key, &pred, &predVersion, &currKey);
        if (currKey != key) return result;  
        result = curr->value; 
        if (mark(curr) == false) continue; // sets the remover identity
        find(key, &pred, &predVersion, &currKey); // linearization point = trim
        return result;     
        
      }
    }
    
    bool contains(const int tid, const K& key) {
      uint64_t predVersion;
      K currKey;
	    UnsafeVBRNode<K,V> *pred, *curr;
         
      curr = find(key, &pred, &predVersion, &currKey);
      return (currKey == key);

    }
    
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {	
    //intptr_t rangeQuery(intptr_t low, intptr_t high, int tid) {      
     
      uint64_t predVersion, rangeQueryEpoch, minEpoch, predTS, currTS, outputExpTS;
      int count;
      K predKey, tmpKey, prevKey;
      V currValue;
 	    UnsafeVBRNode<K,V> *pred, *curr;
          
      while (true) {
          
        count = 0;
       
        curr = find(lo, &pred, &predVersion, &tmpKey);
        if (currVBREpoch < getReclamationEpoch())
          continue;
        
        while (curr->key < lo) {
          curr = getUnsafeVBRNode(curr->next);
          if (currVBREpoch < getReclamationEpoch())
            continue;
        }
        
        while (curr->key <= hi) {
          resultKeys[count] = curr->key;
          resultValues[count] = curr->value;
          count++;
          
          curr = getUnsafeVBRNode(curr->next);
          if (currVBREpoch < getReclamationEpoch())
            continue;

        }
        
        if (currVBREpoch < getReclamationEpoch())
          continue;
       
        return count; 
      }
      
    } 
    
    /**
     * This function must be called once by each thread that will
     * invoke any functions on this class.
     * 
     * It must be okay that we do this with the main thread and later with another thread!!!
     */
    void initThread(const int tid) {
      if (localVBRAllocator == nullptr)
        localVBRAllocator = new LocalAllocator(globalAllocator, tid);
      currVBREpoch = 0;

    }

    void deinitThread(const int tid) {

    }

#ifdef USE_DEBUGCOUNTERS
    debugCounters * debugGetCounters() { return counters; }
    void clearCounters() { counters->clear(); }
#endif
    long long debugKeySum() {
        return debugKeySum(head);
    }

//    void validateRangeQueries(const long long prefillKeyChecksum) {
//        rqProvider->validateRQs(prefillKeyChecksum);
//    }
    bool validate(const long long keysum, const bool checkkeysum) {
        return true;
    }

    long long getSize() {
        long long result = 0;
        UnsafeVBRNode<K,V> *curr = getUnsafeVBRNode(head->next); 
        while (curr->key < KEY_MAX) {
            result ++;
            curr = getUnsafeVBRNode(curr->next); 
        }
        return result; 
    }

    long long getSizeInNodes() {return getSize();}
    string getSizeString() {
        stringstream ss;
        ss<<getSizeInNodes()<<" nodes in data structure";
        return ss.str();
    }
    RecManager * debugGetRecMgr() {
        return recmgr;
    }
    
    inline int getKeys(const int tid, UnsafeVBRNode<K,V> * node, K * const outputKeys, V * const outputValues){
        //ignore marked
        outputKeys[0] = node->key;
        outputValues[0] = node->value;
        return 1;
    }
    
    inline bool isInRange(const K& key, const K& lo, const K& hi) {
        return (lo <= key && key <= hi);
    }
    inline bool isLogicallyDeleted(const int tid, UnsafeVBRNode<K,V> * node) {
      return false;
    }
    
    inline bool isLogicallyInserted(const int tid, UnsafeVBRNode<K,V> * node) {
        return true;
    }

    UnsafeVBRNode<K,V> * debug_getEntryPoint() { return head; }

};

#endif
