#ifndef MVCCVBR_SKIPLIST_H_
#define MVCCVBR_SKIPLIST_H_

#pragma once
#include "errors.h"
#include "Index.h"

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
    // define BEFORE including rq_provider.h
    #define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif


__thread LocalAllocator *localAllocator = nullptr;
__thread uint64_t currEpoch = 0;

#define PADDING_BYTES 192
#define PENDING_TS 1
#define TS_CYCLE 2


#define PENDING_MASK 0x1

template <typename K, typename V, class RecManager>
class mvccvbr_skiplist {
private:
    RecManager * const recmgr = nullptr;
    //RQProvider<K, V, node_t<K,V>, lflist<K,V,RecManager>, RecManager, true, true> * rqProvider;
#ifdef USE_DEBUGCOUNTERS
    debugCounters * const counters;
#endif
    //nodeptr head;
    DirectCTSNode<K,V> *head;
    Index<K,V> *index;
    Allocator *globalAllocator;
    volatile char padding0[PADDING_BYTES];
    std::atomic<uint64_t> tsEpoch;
    volatile char padding1[PADDING_BYTES];
    

    //nodeptr new_node(const int tid, const K& key, const V& val, nodeptr next);
    
    // warning: this can only be used when there are no other threads accessing the data structure
    long long debugKeySum(DirectCTSNode<K,V> *head) {
        long long result = 0;
        DirectCTSNode<K,V> *curr = getDirectCTSNode(head->next); 
        while (curr->key < KEY_MAX) {
            result += curr->key;
            curr = getDirectCTSNode(curr->next); 
        }
        return result;      
    }

    //V doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent);
    
    //int init[MAX_TID_POW2] = {0,};
    
    inline uint64_t getReclamationEpoch(){
      return localAllocator->getEpoch();
	  }
     
    inline uint64_t getTsEpoch(){
      return tsEpoch.load();
	  }
     
    uint64_t incrementTsEpoch(uint64_t expEpoch){
      uint64_t tmp = expEpoch;
      uint64_t newEpoch = expEpoch + TS_CYCLE;
      if (tsEpoch.compare_exchange_strong(tmp, newEpoch, std::memory_order_acq_rel) == true)
        return newEpoch;
      return tmp; 
    }
    
    inline DirectCTSNode<K,V> *integrateEpochIntoPointer(uint64_t epoch, DirectCTSNode<K,V> *ptr) {
      uint64_t version = epoch;
      uint64_t shiftedVersion = version << POINTER_BITS;
      DirectCTSNode<K,V> *integrated = (DirectCTSNode<K,V> *) ((uintptr_t)(ptr) | shiftedVersion);
      return integrated;
    }
    
    inline DirectCTSNode<K,V> *getDirectCTSNode(DirectCTSNode<K,V> *ptr) {
      DirectCTSNode<K,V> *tmp = ptr;
      return (DirectCTSNode<K,V> *) ((uintptr_t)(tmp) & ~STATE_MASK);
    }
    
    inline uint64_t getVersion(DirectCTSNode<K,V> *ptr) {
      DirectCTSNode<K,V> *tmp = ptr;
      uint64_t shiftedVersion = (uintptr_t)tmp & EPOCH_MASK;
      return (shiftedVersion  >> POINTER_BITS);
    }
    
    inline bool isPending(uint64_t ts) {
      return (ts & PENDING_MASK);
    }
    
    inline uint64_t getSnapshotTs(uint64_t ts) {
      return (ts & TS_MASK);
    }

    inline bool isMarked(DirectCTSNode<K,V> *ptr) {
      DirectCTSNode<K,V> *tmp = ptr;
      return (bool)((uintptr_t)(tmp) & MARK_MASK);
    }
    
    inline bool isFlagged(DirectCTSNode<K,V> *ptr) {
      DirectCTSNode<K,V> *tmp = ptr;
      return (bool)((uintptr_t)(tmp) & FLAG_MASK);
    }
    
    inline bool isValidTS(uint64_t ts) {
      uint64_t shiftedEpoch = (ts & ~TS_MASK);
      uint64_t birthEpoch = (shiftedEpoch >> SNAPSHOT_EPOCH_BITS);
      return (birthEpoch <= currEpoch);
    }
    
    inline uint64_t updateTS(DirectCTSNode<K,V> *ptr) {
      uint64_t oldTS = ptr->ts;
      if (isValidTS(oldTS) == false || isPending(oldTS) == false) return oldTS;
      uint64_t newTS = (oldTS & ~TS_MASK);
      uint64_t currTsEpoch = getTsEpoch();
      newTS = (newTS | currTsEpoch);
      if (ptr->ts.compare_exchange_strong(oldTS, newTS) == true)
        return newTS;
      return oldTS;
        
    }
    

    
    inline bool isValidVersion(uint64_t version) {
      return (version <= currEpoch);
    }
 
    inline void readGlobalReclamationEpoch() {   
      currEpoch = getReclamationEpoch();
    }
    
    inline bool mark(DirectCTSNode<K,V> *victim) {
      DirectCTSNode<K,V> *succ = victim->next;
      if (isValidTS(victim->ts) == false) return false;
      if (isMarked(succ) || isFlagged(succ)) return false;
      DirectCTSNode<K,V> *markedSucc = (DirectCTSNode<K,V> *)((uintptr_t)(succ) | MARK_MASK);
      
      if(victim->next.compare_exchange_strong(succ, markedSucc) == true) {
        return true;
      } else {
        return false;
      }
    }
    
    inline void initDirectCTSNode(DirectCTSNode<K,V> *node, K key, V value, DirectCTSNode<K,V> *next, DirectCTSNode<K,V> *nextV) {
      readGlobalReclamationEpoch();
      uint64_t shiftedReclamationEpoch = (currEpoch << SNAPSHOT_EPOCH_BITS);
      uint64_t initTS = (shiftedReclamationEpoch | PENDING_MASK);
      node->ts.store(initTS);
      node->key = key;
      node->value = value;
      node->nextV.store(integrateEpochIntoPointer(currEpoch, nextV)); 
      node->next.store(integrateEpochIntoPointer(currEpoch, next));

    }

public:
    const K KEY_MIN;
    const K KEY_MAX;
    const V NO_VALUE;

    mvccvbr_skiplist(int numProcesses, const K key_min, const K key_max, const V no_value) : KEY_MIN{key_min}, KEY_MAX{key_max}, NO_VALUE{no_value} {
        head = nullptr;
        
        globalAllocator = new Allocator(sizeof(DirectCTSNode<K,V>), 2, numProcesses);
        index = new Index<K,V>(numProcesses);
        initThread(0);
        
        tsEpoch = 2;
        
        DirectCTSNode<K,V> *tail = (DirectCTSNode<K,V> *)localAllocator->alloc();
        initDirectCTSNode(tail, KEY_MAX, NO_VALUE, nullptr, nullptr);
        updateTS(tail);
        
                       
        head = (DirectCTSNode<K,V> *)localAllocator->alloc();
        initDirectCTSNode(head, KEY_MIN, NO_VALUE, tail, nullptr);
        updateTS(head);
        
        index->init(head, tail);
        
        incrementTsEpoch(2);
    }

    ~mvccvbr_skiplist() {
      cout<<"DirectCTSNode size = " << sizeof(DirectCTSNode<K,V>) << ". TS epoch = " << getTsEpoch() << ". Reclamation epoch = " << getReclamationEpoch() <<endl;
    }
    
  private:
  
    DirectCTSNode<K,V> *readVersion(DirectCTSNode<K,V> *ptr, uint64_t snapshotTS) {
      DirectCTSNode<K,V> *curr, *currNextV;
      uint64_t currNextVVersion, currTS, currNextVTS;
      curr = ptr;
      currTS = curr->getTS();
      if (isValidTS(currTS) == false)
        return nullptr;
      
      while (getSnapshotTs(currTS) > snapshotTS) {
        currNextV = curr->getNextV();
        currNextVTS = currNextV->getTS();
        currNextVVersion = curr->getNextVVersion();
        if (isValidVersion(currNextVVersion) == false || isValidTS(currNextVTS) == false)
          return nullptr;
        curr = currNextV;
        currTS = currNextVTS;
      }
      return curr;
    }
    
    DirectCTSNode<K,V> *getNextV(DirectCTSNode<K,V> *ptr, uint64_t snapshotTS) {
      
      DirectCTSNode<K,V> *pred, *curr, *currNextV;
      uint64_t predVersion, currTS, tmp;
      
      pred = ptr;
      curr = pred->getNext();
      currTS = curr->getTS();
      if (isPending(currTS)) {
        if (isValidTS(updateTS(curr)) == false) return nullptr;
      }
      return readVersion(curr, snapshotTS);
    }
    
    // all pointers are assumed to be unmarked and unflagged
    // curr is assumed to be marked
    DirectCTSNode<K,V> *trim(DirectCTSNode<K,V> *pred, DirectCTSNode<K,V> *curr, uint64_t predVersion) {

      DirectCTSNode<K,V> *deleted, *deletedNext, *output, *origNextV, *optimizedNextV, *succNext, *currNextV;
      uint64_t succTS, succTagTS, currTS, updateEpoch, tmp;

      if (!isMarked(curr->next)) 
        return nullptr;
      
      DirectCTSNode<K,V> *succ = curr->getNext();
      succNext = succ->next; 
      succTS = succ->getTS();
      if (isValidTS(succTS) == false) 
        return nullptr;     
      
      while (isMarked(succNext)) {
        succ = getDirectCTSNode(succNext);
        succNext = succ->next;
        succTS = succ->getTS();
        if (isValidTS(succTS) == false) 
          return nullptr;
      }     

      if (isPending(succTS)) {
        //readGlobalReclamationEpoch();
        succTS = updateTS(succ);
        if (pred->next != integrateEpochIntoPointer(predVersion, curr))
          return nullptr;
      }     
      
      if (succ->flag(succTS) == false && (succ->isFlagged() == false || succ->getTS() != succTS)) 
        return nullptr;
      
      // preparing the new node
      // to be inserted instead of the flagged one
      succNext = succ->getNext();
      DirectCTSNode<K,V> *succTag = (DirectCTSNode<K,V> *)localAllocator->alloc();
      initDirectCTSNode(succTag, succ->key, succ->value, succNext, curr);
      origNextV = succTag->nextV;
      
      if (pred->updateNext(integrateEpochIntoPointer(predVersion, curr), integrateEpochIntoPointer(currEpoch, succTag))) {
        updateEpoch = currEpoch;        
        succTagTS = updateTS(succTag);
        
        currNextV = curr->getNextV();
        if (getSnapshotTs(curr->getTS()) == getSnapshotTs(succTagTS)) {
          optimizedNextV = integrateEpochIntoPointer(currEpoch, currNextV);
          succTag->nextV.compare_exchange_strong(origNextV, optimizedNextV);
        }
        
        if (isPending(succTagTS) == false) 
          index->insert(succTag, succTagTS);
        
        deleted = curr;
        do {
          deletedNext = deleted->getNext();
          index->remove(deleted->key);
          localAllocator->retire(deleted);
          deleted = deletedNext;
        } while (deleted != succ);
        
        localAllocator->retire(succ);
        
        output = integrateEpochIntoPointer(updateEpoch, succTag);
        return output;
      } else {
        localAllocator->returnAlloc(succTag);
        return nullptr;
      }

    }

    DirectCTSNode<K,V> *find(K key, DirectCTSNode<K,V> **predPtr, uint64_t *predVersion, K *currKey) {
      DirectCTSNode<K,V> *pred, *predNext, *predNextRaw, *curr, *currNext;
      uint64_t version, tmpEpoch;
      K tmpKey;
      DirectCTSNode<K,V> *tmp;
        
      try_again:
        tmpKey = key;
        readGlobalReclamationEpoch();

        do {
          pred = index->findPred(tmpKey);
          tmpKey = pred->key;
          predNextRaw = pred->next;
          if (isValidTS(pred->getTS()) == false) {
            goto try_again;
          }
        } while (isMarked(predNextRaw) || isFlagged(predNextRaw));
       
        predNext = getDirectCTSNode(predNextRaw);
        version = getVersion(predNextRaw);
        curr = predNext;
        
        while (true) {
          while (isMarked(curr->next)) {
            currNext = curr->getNext();
            if (isValidTS(curr->getTS()) == false) {
              goto try_again;
            }
            curr = currNext;
          }
          
          tmpKey = curr->key;
          if (isValidTS(curr->getTS()) == false) {
            goto try_again;
          }
          
          if (tmpKey >= key) {
            if (isPending(pred->getTS())) {
              if (isValidTS(updateTS(pred)) == false){
                goto try_again;
              }
            }
            break;
          } else if (isFlagged(curr->next)) {
            currNext = curr->getNext();
            if (isValidTS(curr->getTS()) == false) {
              goto try_again;
            }
            curr = currNext;
            continue;
          }
          
          pred = curr;
          predNextRaw = pred->next;
          predNext = getDirectCTSNode(predNextRaw);
          version = getVersion(predNextRaw);
          if (isValidVersion(version) == false || isMarked(predNextRaw) || isFlagged(predNextRaw)) {
            goto try_again;
          }
          curr = predNext;
        }
        
        if (predNext != curr) {
          tmp = trim(pred, predNext, version);
          if (tmp == nullptr) {
            goto try_again;
          }
          curr = getDirectCTSNode(tmp);
          version = getVersion(tmp);
          tmpKey = curr->key;
          if (pred->next != tmp || tmpKey < key) {
            goto try_again;
          }
        } else {
          if (isPending(curr->getTS())) {
            if (isValidTS(updateTS(curr)) == false) {
              goto try_again;
            } 
          }         
        }
        
        *predPtr = pred;
        *predVersion = version;
        *currKey = tmpKey;
        return curr;   
    } 
    
    inline void backoff(int amount) {
        if(amount == 0) return;
        volatile long long sum = 0;
        int limit = amount;
        for(int i = 0; i < limit; i++)
            sum += i; 
    } 

  public:

    V insert(const int tid, const K& key, const V& value) {
      
      DirectCTSNode<K,V> *pred, *curr, *next, *origNextV, *optimizedNextV;
      uint64_t predVersion, newNodeTS, initEpoch;
      K currKey;
      
      
      while (true) {
        curr = find(key, &pred, &predVersion, &currKey);
        if (currKey == key) {
          V result = curr->value;
          if (isValidTS(curr->getTS()) == false)
            continue;
          return result;
        } 
        
        DirectCTSNode<K,V> *newNode = (DirectCTSNode<K,V> *)localAllocator->alloc();
        initDirectCTSNode(newNode, key, value, curr, curr);
        initEpoch = newNode->ts;
        origNextV = newNode->nextV;
                 
        if (pred->updateNext(integrateEpochIntoPointer(predVersion, curr), integrateEpochIntoPointer(currEpoch, newNode))) {
          newNodeTS = updateTS(newNode);
          if (isValidTS(newNodeTS) == true) {
            if (getSnapshotTs(curr->getTS()) == getSnapshotTs(newNodeTS)) {
              optimizedNextV = integrateEpochIntoPointer(currEpoch, curr->getNextV());
              newNode->nextV.compare_exchange_strong(origNextV, optimizedNextV);
            }
            index->insert(newNode, newNodeTS);
          }

            
          return NO_VALUE;
        } else {
          localAllocator->returnAlloc(newNode);
        }
  
          
      }

    }
    
    V insertIfAbsent(const int tid, const K& key, const V& value) {
      return insert(tid, key, value);
    }

    V erase(const int tid, const K& key) {

      DirectCTSNode<K,V> *pred, *curr;
      uint64_t predVersion;
      K currKey;
      V result;

      
      while (true) {
        result = NO_VALUE;
        curr = find(key, &pred, &predVersion, &currKey);
        //if (pred->next != integrateEpochIntoPointer(predVersion, curr)) continue;
        if (currKey != key) return result;  
        result = curr->value; 
        if (mark(curr) == false) continue; // sets the remover identity
        if (trim(pred, curr, predVersion) == nullptr) 
          find(key, &pred, &predVersion, &currKey); // linearization point = trim
        return result;     
        
      }
    }
    
    bool contains(const int tid, const K& key) {
      uint64_t predVersion;
      K currKey;
	    DirectCTSNode<K,V> *pred, *curr;
         
      curr = find(key, &pred, &predVersion, &currKey);
      return (currKey == key);

    }
    
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {	
    //intptr_t rangeQuery(intptr_t low, intptr_t high, int tid) {      
     
      uint64_t predVersion, rangeQueryEpoch, minEpoch, predTS;
      int count;
      K predKey, currKey, tmpKey;
      V currValue;
 	    DirectCTSNode<K,V> *pred, *curr;
  
      minEpoch = getTsEpoch();
      backoff(1000);

      currKey = lo;
          
      while (true) {
          
        count = 0;
        
        rangeQueryEpoch = getTsEpoch() - 2;
        if (rangeQueryEpoch < minEpoch) {
          incrementTsEpoch(minEpoch);
          currKey = lo;
          continue;
        }
        
        
        curr = find(currKey, &pred, &predVersion, &tmpKey);
        
        predKey = pred->key;
        
        pred = readVersion(pred, rangeQueryEpoch);
        
        if (pred == nullptr) {

          currKey = lo;
          continue;
        }
        
        if (pred->key >= lo) {

          currKey = predKey;
          continue;
        }
        
        do {
          curr = getNextV(pred, rangeQueryEpoch);
          if (curr == nullptr) {

            currKey = lo;
            break;            
          }
          currKey = curr->key;  
          currValue = curr->value;
          if (isValidTS(curr->getTS()) == false) {

            currKey = lo;
            break; 
          }        
          pred = curr;
        } while (currKey < lo);
        
        if (curr == nullptr || isValidTS(curr->getTS()) == false) {

          currKey = lo;
          continue;
        }
        
        
        while (currKey <= hi) {
          resultKeys[count] = currKey;
          resultValues[count] = currValue;
          count++;
          curr = getNextV(curr, rangeQueryEpoch);
          if (curr == nullptr) {

            currKey = lo;
            break;            
          }
          currKey = curr->key; 
          currValue = curr->value;
          if (isValidTS(curr->getTS()) == false) {

            currKey = lo;
            break; 
          } 
        }
        
        if (curr == nullptr || isValidTS(curr->getTS()) == false) {

          currKey = lo;
          continue;
        }
       
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
      if (localAllocator == nullptr)
        localAllocator = new LocalAllocator(globalAllocator, tid);
      currEpoch = 0;
      index->initThread(tid);
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
        DirectCTSNode<K,V> *curr = getDirectCTSNode(head->next); 
        while (curr->key < KEY_MAX) {
            result ++;
            curr = getDirectCTSNode(curr->next); 
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
    
    inline int getKeys(const int tid, DirectCTSNode<K,V> * node, K * const outputKeys, V * const outputValues){
        //ignore marked
        outputKeys[0] = node->key;
        outputValues[0] = node->value;
        return 1;
    }
    
    inline bool isInRange(const K& key, const K& lo, const K& hi) {
        return (lo <= key && key <= hi);
    }
    inline bool isLogicallyDeleted(const int tid, DirectCTSNode<K,V> * node) {
      return false;
    }
    
    inline bool isLogicallyInserted(const int tid, DirectCTSNode<K,V> * node) {
        return true;
    }

    DirectCTSNode<K,V> * debug_getEntryPoint() { return head; }

};

#endif
