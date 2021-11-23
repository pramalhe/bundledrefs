#ifndef ALLOC_CACHE_H_
#define ALLOC_CACHE_H_

#include <iostream>

#define ENTRIES_PER_CACHE 64


class AllocCache {  

  private:
  
  AllocCache *next;
  int availEntries;
  void **entries;
  size_t objectSize;
  uint64_t maxEpoch;
    
  public:
  
    AllocCache(AllocCache *next, size_t objectSize) : next(next), objectSize(objectSize) {
      this->availEntries = ENTRIES_PER_CACHE;
      this->entries = (void**)malloc(ENTRIES_PER_CACHE * sizeof(void*));
      this->maxEpoch = 0;
    }
    
    ~AllocCache() {
      free(this->entries);
    }
    
    uint64_t getMaxEpoch() {
      return maxEpoch;
    }
    
    
    void allocEntries(void *mem) {
      for(int i = 0; i < ENTRIES_PER_CACHE; i++){
      	entries[i] = ((char*)mem + i * objectSize);
      }
      availEntries = 0;
    }
    
    bool isFull() { 
      return (availEntries == 0);
    }
    
    bool isEmpty() {
      return (availEntries == ENTRIES_PER_CACHE);
    }
    
    bool addEntry(void *obj, uint64_t retireEpoch) {
      if (isFull()) return false;
      
      availEntries--;
      entries[availEntries] = obj;
      if (retireEpoch > maxEpoch)
        maxEpoch = retireEpoch;
      return true;
    }
    
    bool addEntry(void *obj) {
      if (isFull()) return false;
      
      availEntries--;
      entries[availEntries] = obj;
      return true;
    }

    void *removeEntry() {
      if (isEmpty()) return nullptr;
      
      void *obj = entries[availEntries];
      availEntries++;
      return obj;
    }
    
    AllocCache *getNext() {
      return next;
    }
    
    void setNext(AllocCache *allocCache) {
      next = allocCache;
    }
    
    void initMaxEpoch() {
      maxEpoch = 0;
    }


} __attribute__((aligned((64))));

#endif
