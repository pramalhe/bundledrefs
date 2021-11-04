#ifndef LOCAL_ALLOCATOR_H_
#define LOCAL_ALLOCATOR_H_

#include <atomic>
#include <malloc.h>
#include "Allocator.h"


#define DEFAULT_ALLOC_CACHES 10
#define DEFAULT_FREE_CACHES 10

#define DEFAULT_LIFE_CYCLE 2


class LocalAllocator {  

  private:
  	Allocator *global;
    AllocCache *freeCaches;
  	AllocCache *allocCachesHead; 
    AllocCache *allocCachesTail;
    size_t objectSize;
  	int tid;
   
    

    
  public:
  
    uint64_t getEpoch(){
      return global->getEpoch();
	  }
     
    uint64_t incrementEpoch(uint64_t exp){
      return global->incrementEpoch(exp, exp + DEFAULT_LIFE_CYCLE); 
    }
  
    LocalAllocator(Allocator *global, int tid) : global(global), tid(tid) {
    
      AllocCache *tmp;
      
      objectSize = this->global->getObjectSize();

      
      // initializing the alloc caches
      allocCachesHead = nullptr;
      allocCachesTail = nullptr;
      for (int i = 0; i < DEFAULT_ALLOC_CACHES; i++) {
        tmp = this->global->popAllocCache(); 
        if (tmp == nullptr)
          break;
        tmp->setNext(allocCachesHead);
        if (allocCachesTail == nullptr)
          allocCachesTail = tmp;
        allocCachesHead = tmp;
      }
      
      // initializing the free caches
      freeCaches = nullptr;
      for (int i = 0; i < DEFAULT_FREE_CACHES; i++) {
        tmp = new AllocCache(freeCaches, objectSize); 
        freeCaches = tmp;
      }    
    }


    void *alloc() {
    
      void *ret;
      AllocCache *tmp;
      uint64_t incEpochAmount;
      
      
      while(true) {
      
        while (allocCachesHead != nullptr) {
          if (allocCachesHead->isEmpty()) {
            tmp = allocCachesHead->getNext();
          
            
            allocCachesHead = tmp;
            if (allocCachesHead != nullptr) {
              uint64_t maxEpoch = allocCachesHead->getMaxEpoch();
              uint64_t currGlobalEpoch = getEpoch();
              if (maxEpoch == currGlobalEpoch) {
                incrementEpoch(currGlobalEpoch); 
              }
            }   
          } else {
            ret = allocCachesHead->removeEntry();
            return ret;
          }
        }
        
        allocCachesTail = nullptr;
        for (int i = 0; i < DEFAULT_ALLOC_CACHES; i++) {
          tmp = this->global->popAllocCache(); 
          if (tmp == nullptr)
            break;
          tmp->setNext(allocCachesHead);
          if (allocCachesTail == nullptr)
            allocCachesTail = tmp;         
          allocCachesHead = tmp;
        }       
      }    
    }

    
    void retire(void *obj) {
      AllocCache *tmp;
      
      while (freeCaches != nullptr) {
        if (freeCaches->isFull()) {
          tmp = freeCaches->getNext();
          
          
          freeCaches->setNext(nullptr);
          allocCachesTail->setNext(freeCaches);
          allocCachesTail = allocCachesTail->getNext();
          
          freeCaches = tmp;
        } else {
          freeCaches->addEntry(obj, getEpoch());
          return;
        }
      }
      
      for (int i = 0; i < DEFAULT_FREE_CACHES; i++) {
        tmp = new AllocCache(freeCaches, objectSize); 
        freeCaches = tmp;
      }
    }
        
    void returnAlloc(void *obj) {
      if (allocCachesHead == nullptr || allocCachesHead->isFull())
        retire(obj);
      else
        allocCachesHead->addEntry(obj);    
    }



} __attribute__((aligned((64))));

#endif
