#ifndef ALLOCATOR_H_
#define ALLOCATOR_H_

#include <atomic>
#include <assert.h>
#include "AllocCache.h"

#define PADDING_BYTES 192

#define HEAP_SIZE (32 * 32 * 1024 * 1024L)

class Allocator {  
  private:
  
    volatile char padding0[PADDING_BYTES];
    std::atomic<uint64_t> epoch;
    volatile char padding1[PADDING_BYTES];
    
    size_t objectSize;
    std::atomic<AllocCache *> head;
    std::atomic<uint64_t> numCaches;
    int entriesPerCache;
    
    
  public:

    
    int numThreads;
  
    Allocator(size_t objectSize, uint64_t initEpoch, int numThreads) : objectSize(objectSize), epoch(initEpoch), numThreads(numThreads) {
    
    
      AllocCache *currAllocCache;
      head = nullptr;
      
    
      void *heap = (void *)malloc(HEAP_SIZE);
      char *currHeap = (char *)heap;
      memset(currHeap, 0, HEAP_SIZE);
      numCaches = 0;
      while (currHeap + ENTRIES_PER_CACHE * this->objectSize <= ((char *)heap) + HEAP_SIZE) {
        currAllocCache = new AllocCache(head, this->objectSize);
        currAllocCache->allocEntries(currHeap);
        head = currAllocCache;
        numCaches++;
        currHeap += ENTRIES_PER_CACHE * this->objectSize;
      }

    }
    
    uint64_t getEpoch(){
      return epoch.load(std::memory_order_acq_rel);
	  }
     
    uint64_t incrementEpoch(uint64_t expEpoch, uint64_t newEpoch){
      uint64_t tmp = expEpoch;
      if (epoch.compare_exchange_strong(tmp, newEpoch, std::memory_order_acq_rel) == true)
        return newEpoch;
      return tmp;
    }
    
    int getEntriesPerCache() {
      return ENTRIES_PER_CACHE;
    }
    
    size_t getObjectSize() {
      return objectSize;
    }
    
    AllocCache *popAllocCache() {
      AllocCache *currHead = head, *nextHead;
      while (currHead != nullptr) {
        nextHead = currHead->getNext();
        if (head.compare_exchange_strong(currHead, nextHead) == true) {
          numCaches.fetch_add(-1);
          break;
        }
        currHead = head;
      }
      currHead->setNext(nullptr);
      return currHead;
    }
    
    int getNumCaches() {
      return numCaches;
    }



} __attribute__((aligned((64))));

#endif
