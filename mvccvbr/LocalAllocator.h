#ifndef LOCAL_ALLOCATOR_H_
#define LOCAL_ALLOCATOR_H_

#include <atomic>
#include <malloc.h>
#include "Allocator.h"


#define DEFAULT_ALLOC_CACHES 10
#define DEFAULT_FREE_CACHES 1

#define DEFAULT_LIFE_CYCLE 2


class LocalAllocator {  

  private:
  	Allocator *global;
    AllocCache *freeCachesHead;
    AllocCache *freeCachesTail;
  	AllocCache *allocCachesHead; 
    AllocCache *allocCachesTail;
    size_t objectSize;
  	int tid;
   
    

    
  public:
  
    Allocator *getGlobalAllocator() {
      return global;
    }
  
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
      freeCachesHead = nullptr;
      freeCachesTail = nullptr;
      for (int i = 0; i < DEFAULT_FREE_CACHES; i++) {
        tmp = new AllocCache(freeCachesHead, objectSize); 
        if (freeCachesTail == nullptr)
          freeCachesTail = tmp;
        freeCachesHead = tmp;
      }    
    }


    void *alloc() {
    
      void *ret;
      AllocCache *tmp;
      uint64_t incEpochAmount, maxEpoch, currGlobalEpoch;
      
      
      while(true) {
      
        while (allocCachesHead != nullptr) {
          if (allocCachesHead->isEmpty()) {
            tmp = allocCachesHead->getNext();

            allocCachesHead->setNext(nullptr);
            if (freeCachesHead == nullptr) {
              freeCachesHead = allocCachesHead;              
            } else {
              freeCachesTail->setNext(allocCachesHead);              
            }
            freeCachesTail = allocCachesHead;

            allocCachesHead = tmp;
            if (allocCachesHead != nullptr) {
              maxEpoch = allocCachesHead->getMaxEpoch();
              currGlobalEpoch = getEpoch();
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
          assert(tmp != nullptr);
          //if (tmp == nullptr)
            //break;
          tmp->setNext(allocCachesHead);
          if (allocCachesTail == nullptr)
            allocCachesTail = tmp;         
          allocCachesHead = tmp;
        } 
        
        maxEpoch = allocCachesHead->getMaxEpoch();
        currGlobalEpoch = getEpoch();
        if (maxEpoch == currGlobalEpoch) {
          incrementEpoch(currGlobalEpoch); 
        }      
      }    
    }

    
    void retire(void *obj) {
      AllocCache *tmp;
      
      while (freeCachesHead != nullptr) {
        if (freeCachesHead->isFull()) {
          tmp = freeCachesHead->getNext();
          
          global->pushAllocCache(freeCachesHead);
          
          freeCachesHead = tmp;
        } else {
          freeCachesHead->addEntry(obj, getEpoch());
          return;
        }
      }
      
      freeCachesTail = nullptr;
      for (int i = 0; i < DEFAULT_FREE_CACHES; i++) {
        tmp = new AllocCache(freeCachesHead, objectSize); 
        if (freeCachesTail == nullptr)
          freeCachesTail = tmp;
        freeCachesHead = tmp;
      }
      freeCachesHead->addEntry(obj, getEpoch());
    }
        
    void returnAlloc(void *obj) {
      if (allocCachesHead == nullptr || allocCachesHead->isFull())
        retire(obj);
      else
        allocCachesHead->addEntry(obj);    
    }
    
    void returnAllocCaches() {
      AllocCache *tmp;
      
      while (freeCachesHead != nullptr) {
        tmp = freeCachesHead->getNext();
        if (freeCachesHead->isEmpty() == false) {
          global->pushAllocCache(freeCachesHead);
        } else {
          delete freeCachesHead;
        }
        freeCachesHead = tmp;
      }

      while (allocCachesHead != nullptr) {
        tmp = allocCachesHead->getNext();
        if (allocCachesHead->isEmpty() == false) {
          global->pushAllocCache(allocCachesHead);
        }
        allocCachesHead = tmp;
      }      
      
    }



} __attribute__((aligned((64))));

#endif
