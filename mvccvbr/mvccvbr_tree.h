#ifndef MVCCVBR_TREE_H_
#define MVCCVBR_TREE_H_


#pragma once
#include "errors.h"
#include "TreeIndex.h"

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
    // define BEFORE including rq_provider.h
    #define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif

__thread LocalAllocator *localTreeAllocator = nullptr;
__thread uint64_t currTreeEpoch;

#define PADDING_BYTES 192
#define PENDING_TS 1
#define TS_CYCLE 2
#define PENDING_MASK 0x1
#define MAX_INDEX_ATTEMPTS 5

template <typename K, typename V, class RecManager>
class mvccvbr_tree {


  private:
    RecManager * const recmgr = nullptr;
    volatile char padding0[PADDING_BYTES];
    std::atomic<uint64_t> tsEpoch;
    volatile char padding1[PADDING_BYTES];
    Allocator *globalAllocator;
    volatile char padding2[PADDING_BYTES];
    TreeIndex<K,V> *treeIndex;
    
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
  
    
    inline uint64_t getReclamationEpoch(){
      return localTreeAllocator->getEpoch();
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
  
    DirectCTSNode<K,V> *head;
    
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
      return (birthEpoch <= currTreeEpoch);
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
      return (version <= currTreeEpoch);
    }
 
    inline void readGlobalReclamationEpoch() {   
      currTreeEpoch = getReclamationEpoch();
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
      uint64_t shiftedReclamationEpoch = (currTreeEpoch << SNAPSHOT_EPOCH_BITS);
      uint64_t initTS = (shiftedReclamationEpoch | PENDING_MASK);
      node->ts.store(initTS);
      node->key = key;
      node->value = value;
      node->nextV.store(integrateEpochIntoPointer(currTreeEpoch, nextV)); 
      node->next.store(integrateEpochIntoPointer(currTreeEpoch, next));

    }

  public:
  
    void initThread(int tid) {
      if (localTreeAllocator == nullptr)
        localTreeAllocator = new LocalAllocator(globalAllocator, tid);
      currTreeEpoch = 0;
      treeIndex->initThread(tid);
    }
    
    void deinitThread(const int tid) {
      localTreeAllocator->returnAllocCaches();
      treeIndex->deinitThread(tid);
    }
    
    long long debugKeySum() {
        return debugKeySum(head);
    }

    const K KEY_MIN;
    const K KEY_MAX;
    const V NO_VALUE;
    mvccvbr_tree(int numProcesses, const K KEY_MIN, const K KEY_MAX, const V NO_VALUE) : KEY_MIN(KEY_MIN), KEY_MAX(KEY_MAX), NO_VALUE(NO_VALUE) {
    
        globalAllocator = new Allocator(getNodeSize(), 2, numProcesses);        
        treeIndex = new TreeIndex<K,V>(numProcesses);
        initThread(0);
        
        tsEpoch = 2;
        
        DirectCTSNode<K,V> *tail = (DirectCTSNode<K,V> *)localTreeAllocator->alloc();
        initDirectCTSNode(tail, KEY_MAX, NO_VALUE, nullptr, nullptr);
        updateTS(tail);
        
                       
        head = (DirectCTSNode<K,V> *)localTreeAllocator->alloc();
        initDirectCTSNode(head, KEY_MIN, NO_VALUE, tail, nullptr);
        updateTS(head);
        
        incrementTsEpoch(2);
        treeIndex->init(head, KEY_MAX);
    }
    
    ~mvccvbr_tree() {
      cout << "DirectCTSNode size = " << sizeof(DirectCTSNode<K,V>) << endl;
      cout << "IndexNode size = " << treeIndex->getTreeIndexNodeSize() << endl;
      cout << "TS epoch = " << getTsEpoch() << endl;
      cout << "Reclamation epoch = " << getReclamationEpoch() << endl;
      cout << "Num caches = " << globalAllocator->getNumCaches() << endl;
      cout << "Index reclamation epoch = " << treeIndex->getTreeIndexEpoch() << endl;
      cout << "Index num caches = " << treeIndex->getNumCaches() << endl;
      delete globalAllocator;
      delete treeIndex;
    }

  private:
  
    DirectCTSNode<K,V> *readVersion(DirectCTSNode<K,V> *ptr, uint64_t snapshotTS, uint64_t expTS, uint64_t *outputExpTS) {
      DirectCTSNode<K,V> *curr, *currNextV, *currNextVRaw, *pred, *predNextVRaw;
      uint64_t predNextVVersion, currTS, currNextVTS, predTS;
      
      if (ptr == nullptr)
        return nullptr;
      
      pred = ptr;
      predNextVRaw = pred->nextV;
      predNextVVersion = getVersion(predNextVRaw);
      curr = getDirectCTSNode(predNextVRaw);
      predTS = pred->getTS();
      if (predTS != expTS || curr == nullptr)
        return nullptr;
        
      currTS = curr->getTS();
      if (isPending(currTS) || getReclamationEpochFromTs(currTS) > predNextVVersion)
        return nullptr;
        
      while (getSnapshotTs(currTS) > snapshotTS) {
        pred = curr;
        predNextVRaw = pred->nextV;
        predNextVVersion = getVersion(predNextVRaw);        
        curr = getDirectCTSNode(predNextVRaw);      
        predTS = pred->getTS();
        if (predTS != currTS || curr == nullptr)
          return nullptr;        
        
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
      K predKey = pred->key, currKey;

      predNextRaw = pred->next;
      predVersion = getVersion(predNextRaw);
      curr = getDirectCTSNode(predNextRaw);
      predTS = pred->getTS();
      if (curr == nullptr || predTS != expPredTS)
        return nullptr;

      currTS = curr->getTS();
      if (getReclamationEpochFromTs(currTS) > predVersion)
        return nullptr;
      if (isPending(currTS)) {
        currTS = updateTS(curr);
        return nullptr;
      }
      
      if (getSnapshotTs(currTS) <= snapshotTS)
        return curr;
      curr = readVersion(curr, snapshotTS, currTS, &expCurrTS);
      if (curr == nullptr)
        return nullptr;
      currKey = curr->key;
      currTS = curr->getTS();
      if (currTS != expCurrTS /*|| currKey <= predKey*/)
        return nullptr;
      *outputExpTS = expCurrTS;
      return curr;
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
        readGlobalReclamationEpoch();
        succTS = updateTS(succ);
        if (pred->next != integrateEpochIntoPointer(predVersion, curr))
          return nullptr;
      }     
      
      if (succ->flag(succTS) == false && (succ->isFlagged() == false || succ->getTS() != succTS)) 
        return nullptr;
      
      // preparing the new node
      // to be inserted instead of the flagged one
      succNext = succ->getNext();
      DirectCTSNode<K,V> *succTag = (DirectCTSNode<K,V> *)localTreeAllocator->alloc();
      initDirectCTSNode(succTag, succ->key, succ->value, succNext, curr);
      origNextV = succTag->nextV;
      
      if (pred->updateNext(integrateEpochIntoPointer(predVersion, curr), integrateEpochIntoPointer(currTreeEpoch, succTag))) {
        updateEpoch = currTreeEpoch;        
        succTagTS = updateTS(succTag);
        
        //currNextV = curr->getNextV();
        //if (getSnapshotTs(curr->getTS()) == getSnapshotTs(succTagTS)) {
          //optimizedNextV = integrateEpochIntoPointer(currTreeEpoch, currNextV);
          //succTag->nextV.compare_exchange_strong(origNextV, optimizedNextV);
        //}
        
        if (isPending(succTagTS) == false)
          treeIndex->insert(succTag, succTagTS);
        
        deleted = curr;
        do {
          deletedNext = deleted->getNext();
          treeIndex->remove(deleted->key);
          localTreeAllocator->retire(deleted);
          deleted = deletedNext;
        } while (deleted != succ);
        
        localTreeAllocator->retire(succ);
        
        output = integrateEpochIntoPointer(updateEpoch, succTag);
        return output;
      } else {
        localTreeAllocator->returnAlloc(succTag);
        return nullptr;
      }

    }

    DirectCTSNode<K,V> *find(K key, DirectCTSNode<K,V> **predPtr, uint64_t *predVersion, K *currKey) {
      DirectCTSNode<K,V> *pred, *predNext, *predNextRaw, *curr, *currNext;
      uint64_t version, tmpEpoch;
      DirectCTSNode<K,V> *tmp;
      K tmpKey, lessKey;
      int attemptsCounter;
        
      try_again:
        tmpKey = key;
        readGlobalReclamationEpoch();
        attemptsCounter = 0;

        
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
              readGlobalReclamationEpoch();goto try_again;
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
          if (isValidTS(curr->getTS()) == false) {
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
        
        DirectCTSNode<K,V> *newNode = (DirectCTSNode<K,V> *)localTreeAllocator->alloc();
        initDirectCTSNode(newNode, key, value, curr, curr);
        initEpoch = newNode->ts;
        origNextV = newNode->nextV;
                 
        if (pred->updateNext(integrateEpochIntoPointer(predVersion, curr), integrateEpochIntoPointer(currTreeEpoch, newNode))) {
          newNodeTS = updateTS(newNode);
          if (isValidTS(newNodeTS) == true) {
            //if (getSnapshotTs(curr->getTS()) == getSnapshotTs(newNodeTS)) {
              //optimizedNextV = integrateEpochIntoPointer(currTreeEpoch, curr->getNextV());
              //newNode->nextV.compare_exchange_strong(origNextV, optimizedNextV);
            //}
            treeIndex->insert(newNode, newNodeTS);
          }

          
            
          return NO_VALUE;
        } else {
          localTreeAllocator->returnAlloc(newNode);
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
    
    inline void backoff(int amount) {
        if(amount == 0) return;
        volatile long long sum = 0;
        int limit = amount;
        for(int i = 0; i < limit; i++)
            sum += i; 
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
        
        predKey = pred->key;
        predTS = pred->getTS();
        
        if (isValidTS(predTS) == false) {
          currKey = lo;
          continue;
        }
        
        
        if (getSnapshotTs(predTS) <= rangeQueryEpoch) {
          curr = pred;
          outputExpTS = predTS;
        } else
          curr = readVersion(pred, rangeQueryEpoch, predTS, &outputExpTS);
        
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
        
        if (pred->key >= lo) {

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

          currKey = curr->key;  
          currValue = curr->value;
          currTS = curr->getTS();
          
          
          if (pred->getTS() != predTS || currTS != outputExpTS) {
            currKey = lo;
            break;
          }         

          pred = curr;
          predTS = currTS;

          
        } while (currKey < lo);
        
        
        
        if (curr == nullptr || pred->getTS() != predTS || isPending(currTS) || getSnapshotTs(currTS) > rangeQueryEpoch) {

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

          currKey = curr->key; 
          currValue = curr->value;          
          currTS = curr->getTS();
          
          if (pred->getTS() != predTS || currTS != outputExpTS) {
            currKey = lo;
            break;
          } 
          
          pred = curr;
          predTS = currTS;

 
        }
        
        
        
        if (curr == nullptr || pred->getTS() != predTS || isPending(currTS) || getSnapshotTs(currTS) > rangeQueryEpoch) {

          currKey = lo;
          continue;
        }
       
        return count; 
      }
      
    } 

    std::string myName()
    {
        return "DirectCTSTree";
    }
    
    std::string getInfo() {
        return "epoch: " + to_string(getReclamationEpoch()) + ", ts epoch: " + to_string(getTsEpoch());
    }

    size_t getNodeSize() {
      return sizeof(DirectCTSNode<K,V>);
    }
    
    size_t getIndexNodeSize() {
      return treeIndex->getNodeSize();
    }
    
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
