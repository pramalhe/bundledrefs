#ifndef DIRECT_CTS_NODE_H_
#define DIRECT_CTS_NODE_H_

#include <atomic>
#include "rand_r_32.h"

#define MARK_MASK 0x1
#define FLAG_MASK 0x2
#define STATE_MASK 0xFFFF000000000003ull
#define EPOCH_MASK 0xFFFF000000000000ull
#define POINTER_BITS 48
#define LIFE_CYCLE 1000000
#define TS_MASK 0x0000FFFFFFFFFFFFull
#define SNAPSHOT_EPOCH_BITS 48

template <typename K, typename V>
class DirectCTSNode {  
  public:

    std::atomic<K> key;
    std::atomic<V> value;
    std::atomic<DirectCTSNode *> nextV;
    std::atomic<DirectCTSNode *> next;
    std::atomic<DirectCTSNode *> firstPred;
    std::atomic<uint64_t> ts;
    
    
    DirectCTSNode(K key, V value, DirectCTSNode *next, DirectCTSNode *nextV, uint64_t ts) : key(key), value(value), next(next), nextV(nextV), ts(ts) {}
    
    uint64_t getTS() {
      uint64_t tmp = ts.load(std::memory_order_acq_rel);
      return tmp;
    }
    
    K getKey() {
      K tmp = key.load(std::memory_order_acq_rel);
      return tmp;
    }
    
    void setTS(uint64_t newTs) {
      ts = newTs;
    }
            
    DirectCTSNode *getNext() {
      DirectCTSNode *tmp = next;
      return (DirectCTSNode *) ((uintptr_t)(tmp) & ~STATE_MASK);
    }
	
	  DirectCTSNode *getNextV() {
      DirectCTSNode *tmp = nextV;
      return (DirectCTSNode *) ((uintptr_t)(tmp) & ~STATE_MASK);
    }
    
    uint64_t getVersion() {
      DirectCTSNode *tmp = next;
      uint64_t shiftedVersion = (uintptr_t)tmp & EPOCH_MASK;
      return (shiftedVersion  >> POINTER_BITS);
    }
    
    uint64_t getNextVVersion() {
      DirectCTSNode *tmp = nextV;
      uint64_t shiftedVersion = (uintptr_t)tmp & EPOCH_MASK;
      return (shiftedVersion  >> POINTER_BITS);
    }


    bool updateNext(DirectCTSNode *expVal, DirectCTSNode *newVal) {
      return next.compare_exchange_strong(expVal, newVal);
    }
  

    
    bool isMarked() {
      DirectCTSNode *tmp = next;     
      return (bool)((uintptr_t)(tmp) & MARK_MASK);
    }
    
    bool isMarkedPtr(DirectCTSNode *ptr) {
      DirectCTSNode *tmp = ptr;     
      return (bool)((uintptr_t)(tmp) & MARK_MASK);
    }
    
    bool isFlagged() {
      DirectCTSNode *tmp = next;
      return (bool)((uintptr_t)(tmp) & FLAG_MASK);
    }
    
    bool isFlaggedPtr(DirectCTSNode *ptr) {
      DirectCTSNode *tmp = ptr;
      return (bool)((uintptr_t)(tmp) & FLAG_MASK);
    }
    
    bool flag(uint64_t expTS) {
      
      DirectCTSNode *succ = next;
      if (getTS() != expTS) return false;
      if (isMarkedPtr(succ) || isFlaggedPtr(succ)) return false;
      DirectCTSNode *flaggedSucc = (DirectCTSNode *)((uintptr_t)(succ) | FLAG_MASK);
      
      if(next.compare_exchange_strong(succ, flaggedSucc) == true) {
        return true;
      } else {
        return false;
      }
    }


} __attribute__((aligned((64))));

#endif
