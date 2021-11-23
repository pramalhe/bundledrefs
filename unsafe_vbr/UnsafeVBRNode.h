#ifndef UNSAFE_VBR_NODE_H_
#define UNSAFE_VBR_NODE_H_

#include <atomic>

#define MARK_BIT 0x1
#define POINTER_BITS 48
#define STATE_MASK 0xFFFF000000000001ull
#define EPOCH_MASK 0xFFFF000000000000ull

template <typename K, typename V>
class UnsafeVBRNode {  
  public:

      std::atomic<uint64_t> birthEpoch;
      K key;
      V value;
      std::atomic<UnsafeVBRNode *> next;

      void init(K newKey, V newValue) {

        key = newKey;
        value = newValue; 
      }
      
      bool updateNext(UnsafeVBRNode *expVal, UnsafeVBRNode *newVal) {
        return next.compare_exchange_strong(expVal, newVal);
      }


} __attribute__((aligned((64))));

#endif
