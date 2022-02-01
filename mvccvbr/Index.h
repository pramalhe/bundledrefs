#ifndef _INDEX_H_
#define _INDEX_H_

#include <atomic>
#include <iostream>

#include "DirectCTSNode.h"
//#include "Atomic.h"
#include "LocalAllocator.h"

#define INDEX_MAX_LEVEL (18)
#define INDEX_FREQ 2

__thread LocalAllocator *localIndexAllocator = nullptr;
__thread unsigned int indexLevelSeed;
__thread uint64_t currIndexEpoch = 0;

template <typename K, typename V> 
class Index
{

  private:
  
    class IndexNode {  
      public:
  
        std::atomic<uint64_t> birthEpoch;
        K key;
        int topLevel;
        std::atomic<DirectCTSNode<K,V> *> node;
        std::atomic<uint64_t> fullyLinked; 
        std::atomic<IndexNode *> next[INDEX_MAX_LEVEL];
       
        IndexNode(uint64_t birthEpoch, DirectCTSNode<K,V> *node, int topLevel) : birthEpoch(birthEpoch), node(node), topLevel(topLevel) {
          this->key = node->key;
          this->fullyLinked = 0;
        }
        
        void init(uint64_t birthEpoch, int topLevel, DirectCTSNode<K,V> *n, uint64_t nTS) {
          this->birthEpoch = birthEpoch;
    
          uint64_t nodeVersion = (nTS & ~TS_MASK);
          this->node = (DirectCTSNode<K,V> *) ((uintptr_t)(n) | nodeVersion);
    
          this->key = n->key;
          this->topLevel = topLevel;     
          this->fullyLinked = 0;
        }
  
    } __attribute__((aligned((64))));



    
    inline uint64_t getVersion(IndexNode *ptr) {
      IndexNode *tmp = ptr;
      uint64_t shiftedVersion = (uintptr_t)tmp & EPOCH_MASK;
      return (shiftedVersion  >> POINTER_BITS);
    }
    
    inline DirectCTSNode<K,V> *getDirectCTSNode(IndexNode *indexNode) {
      DirectCTSNode<K,V> *tmp = indexNode->node;
      return (DirectCTSNode<K,V> *) ((uintptr_t)(tmp) & ~EPOCH_MASK); 
    }
    
    inline bool isMarked(IndexNode *ptr) {
        return ((intptr_t) ptr & MARK_MASK);
    }
    
    inline IndexNode *getMarked(IndexNode *ptr) {
        return (IndexNode *)((intptr_t)ptr | MARK_MASK);
    }
    
    inline IndexNode *getRef(IndexNode *ptr) {
        return (IndexNode *)((intptr_t)ptr & ~STATE_MASK);
    }
    
    inline bool shouldRetire(IndexNode *indexNode) {
      uint64_t ZERO = 0;
      uint64_t ONE = 1;
      return (!indexNode->fullyLinked.compare_exchange_strong(ZERO, ONE));
    }
    
    inline IndexNode *integrate(IndexNode *ptr, uint64_t version) {
      uintptr_t shiftedVersion = version << POINTER_BITS;
      IndexNode *tmp = (IndexNode *) ((uintptr_t)(ptr) | shiftedVersion);
      return tmp;
    }
    
    inline bool updateNext(IndexNode *indexNode, int level, IndexNode *expNext, IndexNode *newNext) {
      if (isMarked(expNext) == true) return false;
      IndexNode *tmp = expNext;
      return indexNode->next[level].compare_exchange_strong(tmp, newNext);
    }
    
    inline bool updateNode(IndexNode *indexNode, DirectCTSNode<K,V> *newNode, uint64_t newNodeTS) {
      DirectCTSNode<K,V> *exp = indexNode->node;
      uint64_t expVersion = ((uint64_t)exp & ~TS_MASK);
      uint64_t newVersion = (newNodeTS & ~TS_MASK);
      if (newVersion < expVersion) return false;
      DirectCTSNode<K,V> *tmp = (DirectCTSNode<K,V> *) ((uintptr_t)(newNode) | newVersion);
      return indexNode->node.compare_exchange_strong(exp, tmp);
    }

    inline bool markIndexNode(IndexNode *victim) {
        IndexNode *before, *after;
        bool result = false;      
        
        for (int i = victim->topLevel; i >= 0; i--) {
          do {
                before = victim->next[i];
                if (isMarked(before) || currIndexEpoch < victim->birthEpoch) {
                    result = false;
                    break;
                }
                after = getMarked(before);
                result = updateNext(victim, i, before, after);
          } while (!result);         
        }
        
        return result;
    }
    
    volatile char padding0[PADDING_BYTES];
    IndexNode *indexHead;
    volatile char padding1[PADDING_BYTES];
    Allocator *globalIndexAllocator;
    volatile char padding2[PADDING_BYTES];
    
    bool find(K key, IndexNode **preds, IndexNode **succs, int minLevel)
    {
        IndexNode *pred, *predNext, *succ, *succNext;
        K succKey;

    retry:
        currIndexEpoch = getIndexEpoch();
        pred = indexHead;
        for (int i = INDEX_MAX_LEVEL - 1; i >= minLevel; i--)
        {
            predNext = pred->next[i]; 
            if (isMarked(predNext) || pred->birthEpoch > currIndexEpoch) 
              goto retry;             
                
            succ = getRef(predNext);
            succNext = succ->next[i];
            if (succ->birthEpoch > currIndexEpoch) 
              goto retry;
            
            while (true) {
              while (isMarked(succNext)) {
                succ = getRef(succNext);
                succNext = succ->next[i];
                if (succ->birthEpoch > currIndexEpoch) 
                  goto retry;
              }
              
              succKey = succ->key;
              if (succ->birthEpoch > currIndexEpoch) 
                goto retry;
              
              if (succKey >= key)
                  break;
                  
              pred = succ;
              predNext = succNext;
              
              succ = getRef(predNext);
              succNext = succ->next[i];
              if (succ->birthEpoch > currIndexEpoch) 
                goto retry;
            } 
            

            if ((getRef(predNext) != succ) && updateNext(pred, i, predNext, integrate(succ, currIndexEpoch)) == false)
                goto retry;

            if (preds != nullptr)
            {
                preds[i] = pred;
                succs[i] = succ;
            }

        }

        return succKey == key;
    }
    
    DirectCTSNode<K,V> *findIndexPred(K key) {
        IndexNode *pred, *predNext, *succ, *succNext;
        K succKey;

    retry:
        currIndexEpoch = getIndexEpoch();
        pred = indexHead;
        for (int i = INDEX_MAX_LEVEL - 1; i >= 0; i--)
        {
            predNext = pred->next[i]; 
            if (isMarked(predNext) || pred->birthEpoch > currIndexEpoch) 
              goto retry;             
                
            succ = getRef(predNext);
            succNext = succ->next[i];
            if (succ->birthEpoch > currIndexEpoch) 
              goto retry;
            
            while (true) {
              while (isMarked(succNext)) {
                succ = getRef(succNext);
                succNext = succ->next[i];
                if (succ->birthEpoch > currIndexEpoch) 
                  goto retry;
              }
              
              succKey = succ->key;
              if (succ->birthEpoch > currIndexEpoch) 
                goto retry;
              
              if (succKey >= key)
                  break;
                  
              pred = succ;
              predNext = succNext;
              
              succ = getRef(predNext);
              succNext = succ->next[i];
              if (succ->birthEpoch > currIndexEpoch) 
                goto retry;
            } 

        }

        
        return getDirectCTSNode(pred);
     
    }
    
    int get_random_level() {
        int i;
        int level = 1;
    
        for (i = 0; i < INDEX_MAX_LEVEL - 1; i++)
        {
            if ((rand_r_32(&indexLevelSeed) & 0xFF) < 128)
                level++;
            else
                break;
        }
        return (level % INDEX_MAX_LEVEL);
    }
    


  public:
  
    uint64_t getIndexEpoch(){
      return localIndexAllocator->getEpoch();
	  }
     
    int getNumCaches() {
      return globalIndexAllocator->getNumCaches();
    }
     

  
    Index(int numProcesses)
    {
        globalIndexAllocator = new Allocator(getNodeSize(), 2, numProcesses);
        //initThread(0);
    }
    
    ~Index() {
      delete globalIndexAllocator;
    }
    
    void initThread(int tid) {
      if (localIndexAllocator == nullptr || localIndexAllocator->getGlobalAllocator() != globalIndexAllocator)
        localIndexAllocator = new LocalAllocator(globalIndexAllocator, tid);
      indexLevelSeed = tid + 3;
      currIndexEpoch = 0;
    }
    
    void deinitThread(const int tid) {
      localIndexAllocator->returnAllocCaches();
    }
    
    void init(DirectCTSNode<K,V> *head, DirectCTSNode<K,V> *tail) {
        IndexNode *indexTail;
        
        currIndexEpoch = getIndexEpoch();
        
        indexHead = (IndexNode *) localIndexAllocator->alloc();
        indexHead->init(currIndexEpoch, INDEX_MAX_LEVEL - 1, head, head->getTS());
        indexTail = (IndexNode *) localIndexAllocator->alloc();
        indexTail->init(currIndexEpoch, INDEX_MAX_LEVEL - 1, tail, tail->getTS());
       
        for (int i = 0; i < INDEX_MAX_LEVEL; i++) {
          indexHead->next[i] = integrate(indexTail, currIndexEpoch);
          indexTail->next[i] = integrate(nullptr, currIndexEpoch);
        }
    }
       

    
    bool insert(DirectCTSNode<K,V> *n, uint64_t nTS)
    {
       
        if (n->key % INDEX_FREQ == 0) return false;
        IndexNode *newIndexNode;
        IndexNode *preds[INDEX_MAX_LEVEL], *succs[INDEX_MAX_LEVEL];
        IndexNode *expNext, *newNext;
        K key = n->key;
        
        // preparing the new index node
        int topLevel = get_random_level();
        newIndexNode = (IndexNode *) localIndexAllocator->alloc();
        currIndexEpoch = getIndexEpoch();
        newIndexNode->init(currIndexEpoch, topLevel, n, nTS);
        

    retry:
        if (n->getTS() != nTS) { 
          localIndexAllocator->returnAlloc(newIndexNode);
          return false;
        }
        
        if (find(key, preds, succs,  0) == true) {
          localIndexAllocator->returnAlloc(newIndexNode);        
          return updateNode(succs[0], n, nTS);
        } 
        
          
        
        if (n->getTS() != nTS) { 
          localIndexAllocator->returnAlloc(newIndexNode);
          return false;
        }
        
        for (int i = 0; i <= topLevel; i++) {
          newIndexNode->next[i] = integrate(succs[i], currIndexEpoch);
        }
        
                
        // insertion linearization
        expNext = preds[0]->next[0];
        newNext = integrate(newIndexNode, currIndexEpoch);
        if (getVersion(expNext) > currIndexEpoch || getRef(expNext) != succs[0] || updateNext(preds[0], 0, expNext, newNext) == false) {
            goto retry;
        } 
         
        
        // inserting the higher levels
        for (int i = 1; i <= topLevel; i++)
        {
            while (true)
            {
                expNext = newIndexNode->next[i];
                newNext = integrate(succs[i], currIndexEpoch);
                
                if (isMarked(expNext))
                  break;                  

                if (expNext != newNext && updateNext(newIndexNode, i, expNext, newNext) == false)
                  break;


                expNext = preds[i]->next[i];
                newNext = integrate(newIndexNode, currIndexEpoch);
                if (getVersion(expNext) <= currIndexEpoch && getRef(expNext) == succs[i] && updateNext(preds[i], i, expNext, newNext))
                  break;

                find(key, preds, succs, i);
            }
        }
        
        if (shouldRetire(newIndexNode)) {
          // in charge of retirement
          find(key, nullptr, nullptr, 0);
          currIndexEpoch = getIndexEpoch();
          localIndexAllocator->retire(newIndexNode);                   
        }


        return true;
    }
    
    bool remove(K key)
    {

        if (key % INDEX_FREQ == 0) return false;
        IndexNode *victim, *preds[INDEX_MAX_LEVEL], *succs[INDEX_MAX_LEVEL];

        if (find(key, preds, succs, 0) == false)
          return false;
        
        victim = succs[0];
        bool result = markIndexNode(victim);
        if (result) {
         
          if (shouldRetire(victim)) {
            // in charge of retirement
            find(key, nullptr, nullptr, 0);
            currIndexEpoch = getIndexEpoch();
            localIndexAllocator->retire(victim);               
          }
          return true;
        }
        
        return false;

    }

    
    DirectCTSNode<K,V> *findPred(K key) {

      return findIndexPred(key);
    }
    
    size_t getNodeSize() {
      return sizeof(IndexNode);
    }
    
} __attribute__((aligned((64))));

#endif
