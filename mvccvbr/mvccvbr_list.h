#ifndef MVCCVBR_LIST_H_
#define MVCCVBR_LIST_H_

#pragma once
#include "errors.h"
#include "DirectCTSNode.h"
#include "LocalAllocator.h"

#if defined(MVCC_VBR_SKIPLIST)
#include "Index.h"
#elif defined(MVCC_VBR_TREE)
#include "TreeIndex.h"
#endif

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
    // define BEFORE including rq_provider.h
    #define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif


__thread LocalAllocator *localAllocator = nullptr;
__thread uint64_t currEpoch = 0;
__thread uint64_t currTreeEpoch = 0;

#define PADDING_BYTES 192
#define PENDING_TS 1
#define TS_CYCLE 2
#define MAX_INDEX_ATTEMPTS 5
#define PENDING_MASK 0x1

template <typename K, typename V, class RecManager>
class mvccvbr_list {
private:
    RecManager * const recmgr = nullptr;
    DirectCTSNode<K,V> *head;
#if defined(MVCC_VBR_SKIPLIST)
    Index<K,V> *index;
#elif defined(MVCC_VBR_TREE)
    TreeIndex<K,V> *treeIndex;
#endif
    Allocator *globalAllocator;
    volatile char padding0[PADDING_BYTES];
    std::atomic<uint64_t> tsEpoch;
    volatile char padding1[PADDING_BYTES];
    

    //nodeptr new_node(const int tid, const K& key, const V& val, nodeptr next);
    
    // warning: this can only be used when there are no other threads accessing the data structure
    long long debugKeySum(DirectCTSNode<K,V> *head) {
        long long result = 0;
        K prevKey = head->key;
        DirectCTSNode<K,V> *curr = getDirectCTSNode(head->next); 
        while (curr->key < KEY_MAX) {
            if (curr->key >= prevKey)
              result += curr->key;
            prevKey = curr->key;
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
    
    inline uint64_t getReclamationEpochFromTs(uint64_t ts) {
      uint64_t shiftedEpoch = (ts & ~TS_MASK);
      uint64_t birthEpoch = (shiftedEpoch >> SNAPSHOT_EPOCH_BITS);
      return birthEpoch;
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
      node->ts.store(initTS, std::memory_order_acq_rel);
      node->next.store(integrateEpochIntoPointer(currEpoch, next), std::memory_order_acq_rel);
      node->nextV.store(integrateEpochIntoPointer(currEpoch, nextV), std::memory_order_acq_rel);       
      node->key.store(key, std::memory_order_acq_rel);
      node->value.store(value, std::memory_order_acq_rel);


    }

public:
    const K KEY_MIN;
    const K KEY_MAX;
    const V NO_VALUE;

    mvccvbr_list(int numProcesses, const K key_min, const K key_max, const V no_value) : KEY_MIN{key_min}, KEY_MAX{key_max}, NO_VALUE{no_value} {
        head = nullptr;
        
        globalAllocator = new Allocator(sizeof(DirectCTSNode<K,V>), 2, numProcesses);
#if defined(MVCC_VBR_SKIPLIST) 
        index = new Index<K,V>(numProcesses);
#elif defined(MVCC_VBR_TREE)
        treeIndex = new TreeIndex<K,V>(numProcesses);
#endif
        initThread(0);
        
        tsEpoch = 2;
        
        DirectCTSNode<K,V> *tail = (DirectCTSNode<K,V> *)localAllocator->alloc();
        initDirectCTSNode(tail, KEY_MAX, NO_VALUE, nullptr, nullptr);
        updateTS(tail);
        
                       
        head = (DirectCTSNode<K,V> *)localAllocator->alloc();
        initDirectCTSNode(head, KEY_MIN, NO_VALUE, tail, nullptr);
        updateTS(head);
#if defined(MVCC_VBR_SKIPLIST)        
        index->init(head, tail);
#elif defined(MVCC_VBR_TREE)
        treeIndex->init(head, KEY_MAX);
#endif
        
        incrementTsEpoch(2);
    }

    ~mvccvbr_list() {
      cout << "DirectCTSNode size = " << sizeof(DirectCTSNode<K,V>) << endl;
      cout << "TS epoch = " << getTsEpoch() << endl;
      cout << "Reclamation epoch = " << getReclamationEpoch() << endl;
      cout << "Num caches = " << globalAllocator->getNumCaches() << endl;
      
#if defined(MVCC_VBR_SKIPLIST) 
      cout << "IndexNode size = " << index->getNodeSize() << endl;
      cout << "Index reclamation epoch = " << index->getIndexEpoch() << endl;
      cout << "Index num caches = " << index->getNumCaches() << endl;
      delete index;
#elif defined(MVCC_VBR_TREE)
      cout << "IndexNode size = " << treeIndex->getTreeIndexNodeSize() << endl;
      cout << "Index reclamation epoch = " << treeIndex->getTreeIndexEpoch() << endl;
      cout << "Index num caches = " << treeIndex->getNumCaches() << endl;
      delete treeIndex;
#endif

      delete globalAllocator;
      
    }
    
  private:
  
    DirectCTSNode<K,V> *readVersion(DirectCTSNode<K,V> *ptr, uint64_t snapshotTS, uint64_t expTS, uint64_t *outputExpTS) {
      DirectCTSNode<K,V> *curr, *currNextV, *currNextVRaw, *pred, *predNextVRaw;
      uint64_t predNextVVersion, currTS, currNextVTS, predTS;
      K predKey, currKey;
      
      if (ptr == nullptr)
        return nullptr;
      
      if (getSnapshotTs(expTS) <= snapshotTS) {
        *outputExpTS = expTS;
        return ptr;
      }
      
      pred = ptr;
      predKey = pred->getKey();
      predNextVRaw = pred->nextV;
      predNextVVersion = getVersion(predNextVRaw);
      curr = getDirectCTSNode(predNextVRaw);
      predTS = pred->getTS();
      if (predTS != expTS || curr == nullptr || predNextVVersion != getReclamationEpochFromTs(predTS))
        return nullptr;
       
      currKey = curr->getKey();  
      currTS = curr->getTS();
      if (isPending(currTS) || getReclamationEpochFromTs(currTS) > predNextVVersion) {
        return nullptr;
      }
    
        
      while (getSnapshotTs(currTS) > snapshotTS) {
        pred = curr;
        predNextVRaw = pred->nextV;
        predNextVVersion = getVersion(predNextVRaw);        
        curr = getDirectCTSNode(predNextVRaw);      
        predTS = pred->getTS();
        if (predTS != currTS || curr == nullptr || predNextVVersion != getReclamationEpochFromTs(predTS))
          return nullptr;        
        
        currKey = curr->getKey(); 
        currTS = curr->getTS();
        if (isPending(currTS) || getReclamationEpochFromTs(currTS) > predNextVVersion)
          return nullptr;        
   
      }
      *outputExpTS = currTS;
      return curr;
    }
    
    DirectCTSNode<K,V> *getNextV(DirectCTSNode<K,V> *pred, uint64_t snapshotTS, uint64_t expPredTS, uint64_t *outputExpTS) {
      
      DirectCTSNode<K,V> *curr, *currNextV, *predNextRaw;
      uint64_t predVersion, currTS, tmp, predTS, expCurrTS;
      K predKey = pred->getKey(), currKey;

      predNextRaw = pred->next;
      predVersion = getVersion(predNextRaw);
      curr = getDirectCTSNode(predNextRaw);
      predTS = pred->getTS();
      if (curr == nullptr || predTS != expPredTS)
        return nullptr;
      currKey = curr->getKey();

      currTS = curr->getTS();
      if (getReclamationEpochFromTs(currTS) > predVersion)
        return nullptr;
      
      if (isPending(currTS)) {
        currTS = updateTS(curr);
        return nullptr;
      }
      
      if (getSnapshotTs(currTS) <= snapshotTS) {
        *outputExpTS = currTS;
        return curr;
      }
      curr = readVersion(curr, snapshotTS, currTS, &expCurrTS);
      if (curr == nullptr)
        return nullptr;
      currKey = curr->getKey();
      currTS = curr->getTS();
      if (currTS != expCurrTS /*|| currKey <= predKey*/)
        return nullptr;
      
      *outputExpTS = expCurrTS;
      return curr;
    }
    
    // all pointers are assumed to be unmarked and unflagged
    // curr is assumed to be marked
    DirectCTSNode<K,V> *trim(DirectCTSNode<K,V> *pred, DirectCTSNode<K,V> *curr, uint64_t predVersion) {

      DirectCTSNode<K,V> *deleted, *deletedNext, *output, *origNextV, *optimizedNextV, *succNext, *currNextV, *predNext, *currNext;
      uint64_t succTS, succTagTS, succNextTS, currTS, updateEpoch, tmp;
      K succKey, predKey;
      
      predKey = pred->key;
      predNext = pred->next;
      
      if (isValidTS(pred->getTS()) == false || getDirectCTSNode(predNext) != curr || getVersion(predNext) != predVersion || isMarked(predNext) || isFlagged(predNext))
        return nullptr;

      currNext = curr->next;
      if (!isMarked(currNext) || isValidTS(curr->getTS()) == false) 
        return nullptr;
      
      DirectCTSNode<K,V> *succ = getDirectCTSNode(currNext);
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
        if (isValidTS(succTS) == false || pred->next != predNext)
          return nullptr;
      }     
      
      if (succ->flag(succTS) == false && (succ->isFlagged() == false || succ->getTS() != succTS)) 
        return nullptr;
      
      // preparing the new node
      // to be inserted instead of the flagged one
      succNext = succ->getNext();
      succKey = succ->getKey();
      if (succNext != nullptr) {
        succNextTS = succNext->getTS();        
        if (isPending(succNextTS)) {
          succNextTS = updateTS(succNext);
          if (isValidTS(succNextTS) == false)
            return nullptr;
        } 
      }       
      
      DirectCTSNode<K,V> *succTag = (DirectCTSNode<K,V> *)localAllocator->alloc();
      initDirectCTSNode(succTag, succKey, succ->value, succNext, curr);
      
      if (pred->updateNext(predNext, integrateEpochIntoPointer(currEpoch, succTag))) {

        updateEpoch = currEpoch;        
        succTagTS = updateTS(succTag);
        
        
#if defined(MVCC_VBR_SKIPLIST)
          if (isPending(succTagTS) == false) 
            index->insert(succTag, succTagTS);
#elif defined(MVCC_VBR_TREE)
          if (isPending(succTagTS) == false) 
            treeIndex->insert(succTag, succTagTS);
#endif
        
        deleted = curr;
        do {
          deletedNext = deleted->getNext();
          
#if defined(MVCC_VBR_SKIPLIST)
          index->remove(deleted->getKey());
#elif defined(MVCC_VBR_TREE)
          treeIndex->remove(deleted->getKey());
#endif
          
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

    DirectCTSNode<K,V> *find(K key, DirectCTSNode<K,V> **predPtr, uint64_t *predVersion, K *outputCurrKey) {
      DirectCTSNode<K,V> *pred, *predNext, *predNextRaw, *curr, *currNext, *currNextRaw;
      uint64_t version, tmpEpoch;
      K tmpKey = key, predKey, currKey, lessKey;
      DirectCTSNode<K,V> *tmp;
      int attemptsCounter;

        
      try_again:
        readGlobalReclamationEpoch();
        attemptsCounter = 0;
        tmpKey = key;

#if defined(MVCC_VBR_LIST)
        pred = head;
        predNextRaw = pred->next;
#elif defined(MVCC_VBR_SKIPLIST)

        
        while (true) {
          if (attemptsCounter == MAX_INDEX_ATTEMPTS) {
            pred = head;
            predNextRaw = pred->next;
            predKey = pred->getKey();
            break;
          }
          pred = index->findPred(tmpKey);
          predKey = pred->getKey();
          predNextRaw = pred->next;
          attemptsCounter++;
          if (isValidTS(pred->getTS()) == false) {
            goto try_again;
          }
          if (predKey >= key) {
            continue;
          }
          tmpKey = predKey;
          if (isMarked(predNextRaw) || isFlagged(predNextRaw)) {
            continue;
          }

          break;
        }
        
#elif defined(MVCC_VBR_TREE)

        while (true) {
          if (attemptsCounter == MAX_INDEX_ATTEMPTS) {
            pred = head;
            predNextRaw = pred->next;
            break;
          }
          pred = treeIndex->findPred(tmpKey, &lessKey);
          assert(pred != nullptr);
          tmpKey = pred->key;
          assert(tmpKey < key || lessKey < tmpKey);
          predNextRaw = pred->next;
          attemptsCounter++;
          if (isValidTS(pred->getTS()) == false) {
            goto try_again;
          }
          if (tmpKey >= key || isMarked(predNextRaw) || isFlagged(predNextRaw)) {
            tmpKey = lessKey;
            continue;
          }
          break;
        }

#endif
       
        predNext = getDirectCTSNode(predNextRaw);
        version = getVersion(predNextRaw);
        curr = predNext;
        
        while (true) {

          while (isMarked(curr->next) || isFlagged(curr->next)) {
            currNext = curr->getNext();
            if (isValidTS(curr->getTS()) == false) {
              goto try_again;
            }
            curr = currNext;
          }
          
          currKey = curr->getKey();
          if (isValidTS(curr->getTS()) == false) {
            goto try_again;
          }
          
          if (currKey >= key) {
            if (isPending(pred->getTS())) {
              if (isValidTS(updateTS(pred)) == false){
                goto try_again;
              }
            }
            break;
          //} else if (isFlagged(curr->next)) {
            //currNext = curr->getNext();
            //if (isValidTS(curr->getTS()) == false) {
              //goto try_again;
            //}
            //curr = currNext;
            //continue;
          } else {
          
            pred = curr;
            predKey = pred->getKey();
            predNextRaw = pred->next;
            predNext = getDirectCTSNode(predNextRaw);
            version = getVersion(predNextRaw);
            if (isValidTS(pred->getTS()) == false || isValidVersion(version) == false || isMarked(predNextRaw) || isFlagged(predNextRaw)) {
              goto try_again;
            }
            curr = predNext;
          }
        }
        
        
        if (predNext != curr) {
          tmp = trim(pred, predNext, version);
          if (tmp == nullptr) {
            goto try_again;
          }
          curr = getDirectCTSNode(tmp);
          version = getVersion(tmp);
          currKey = curr->getKey();
          if (pred->next != tmp || currKey < key || isValidTS(curr->getTS()) == false) {
            goto try_again;
          }
        }
        
        if (isPending(curr->getTS())) {
          if (isValidTS(updateTS(curr)) == false) {
            goto try_again;
          } 
        }         

        
        *predPtr = pred;
        *predVersion = version;
        *outputCurrKey = currKey;
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

    V insert(const int tid, const K& key, const V& value) { //galiinsert
      
      DirectCTSNode<K,V> *pred, *curr, *next, *predNext;
      uint64_t predVersion, newNodeTS, initEpoch;
      K currKey, predKey;
      
      
      while (true) {
        curr = find(key, &pred, &predVersion, &currKey);
        if (currKey == key) {
          V result = curr->value;
          if (isValidTS(curr->getTS()) == false)
            continue;
          return result;
        } 
        
        predNext = pred->next;
        predKey = pred->getKey();
        if (isValidTS(pred->getTS()) == false || getVersion(predNext) != predVersion || getDirectCTSNode(predNext) != curr || isMarked(predNext) || isFlagged(predNext))
          continue;
        
        DirectCTSNode<K,V> *newNode = (DirectCTSNode<K,V> *)localAllocator->alloc();
        initDirectCTSNode(newNode, key, value, curr, curr);
         
                 
        if (pred->updateNext(predNext, integrateEpochIntoPointer(currEpoch, newNode))) {
          newNodeTS = updateTS(newNode);
#if defined(MVCC_VBR_SKIPLIST)
          if (isValidTS(newNodeTS) == true) 
            index->insert(newNode, newNodeTS);
#elif defined(MVCC_VBR_TREE)
          if (isValidTS(newNodeTS) == true) 
            treeIndex->insert(newNode, newNodeTS);
#endif


            
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
     
      uint64_t predVersion, rangeQueryEpoch, minEpoch, predTS, currTS, outputExpTS;
      int count;
      K predKey, currKey, tmpKey, prevKey;
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
        
        predKey = pred->getKey();
        predTS = pred->getTS();
        
        if (isValidTS(predTS) == false) {
          currKey = lo;
          continue;
        }
        
        
        if (getSnapshotTs(predTS) <= rangeQueryEpoch) {
          curr = pred;
          outputExpTS = predTS;
        } else {
          curr = readVersion(pred, rangeQueryEpoch, predTS, &outputExpTS);
        }
        
        if (curr == nullptr) {
          currKey = lo;
          continue;
        } 
        
        currTS = curr->getTS();
        
        if (pred->getTS() != predTS || currTS != outputExpTS) {
          currKey = lo;
          continue;
        }
        
        pred = curr;
        predTS = currTS;
        
        if (pred->getKey() >= lo) {

          currKey = predKey;
          continue;
        }
        
        if (predTS != pred->getTS()) {
          currKey = lo;
          continue;
        }
        
        
        
        do {
          curr = getNextV(pred, rangeQueryEpoch, predTS, &outputExpTS);
          if (curr == nullptr) {
            currKey = lo;
            break;
          }

          currKey = curr->getKey();  
          currValue = curr->value;
          currTS = curr->getTS();
          
          
          if (pred->getTS() != predTS || currTS != outputExpTS) {
            currKey = lo;
            break;
          }         

          pred = curr;
          predTS = currTS;

          
        } while (currKey < lo);
        
        
        
        if (curr == nullptr || pred->getTS() != predTS || currTS != outputExpTS) {

          currKey = lo;
          continue;
        }
        
        
        while (currKey <= hi) {
          resultKeys[count] = currKey;
          resultValues[count] = currValue;
          count++;
          
          curr = getNextV(pred, rangeQueryEpoch, predTS, &outputExpTS);
          if (curr == nullptr) {
            currKey = lo;
            break;
          }

          currKey = curr->getKey(); 
          currValue = curr->value;          
          currTS = curr->getTS();
          
          if (pred->getTS() != predTS || currTS != outputExpTS) {
            currKey = lo;
            break;
          } 
          
          pred = curr;
          predTS = currTS;

 
        }
        
        
        
        if (curr == nullptr || pred->getTS() != predTS || currTS != outputExpTS) {

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
      if (localAllocator == nullptr || localAllocator->getGlobalAllocator() != globalAllocator)
        localAllocator = new LocalAllocator(globalAllocator, tid);
      currEpoch = 0;
#if defined(MVCC_VBR_SKIPLIST)
      index->initThread(tid);
#elif defined(MVCC_VBR_TREE)
      treeIndex->initThread(tid);
#endif
      
    }

    void deinitThread(const int tid) {
      localAllocator->returnAllocCaches();
      
#if defined(MVCC_VBR_SKIPLIST)
      index->deinitThread(tid);
#elif defined(MVCC_VBR_TREE)
      treeIndex->deinitThread(tid);
#endif
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
