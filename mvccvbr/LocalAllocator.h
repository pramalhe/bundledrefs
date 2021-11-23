#ifndef LOCAL_ALLOCATOR_H_
#define LOCAL_ALLOCATOR_H_

#include <atomic>
#include <malloc.h>
#include "Allocator.h"


#define DEFAULT_ALLOC_CACHES 2
#define DEFAULT_FREE_CACHES 1

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
            delete allocCachesHead;
            
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
          
          global->pushAllocCache(freeCaches);
          //freeCaches->setNext(nullptr);
          //allocCachesTail->setNext(freeCaches);
          //allocCachesTail = allocCachesTail->getNext();
          
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
    
    void returnAllocCaches() {
      AllocCache *tmp;
      
      while (freeCaches != nullptr) {
        tmp = freeCaches->getNext();
        if (freeCaches->isEmpty() == false) {
          global->pushAllocCache(freeCaches);
        } else {
          delete freeCaches;
        }
        freeCaches = tmp;
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
