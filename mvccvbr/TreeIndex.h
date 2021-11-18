#ifndef _TREE_INDEX_H_
#define _TREE_INDEX_H_

#include <atomic>
#include <iostream>

#include "DirectCTSNode.h"
#include "LocalAllocator.h"


__thread LocalAllocator *localTreeIndexAllocator = nullptr;
__thread uint64_t currTreeIndexEpoch;

template <typename K, typename V> 
class TreeIndex
{

  private:
  
    class TreeIndexNode {  
      public:
  
        std::atomic<uint64_t> birthEpoch;
        K key;
        std::atomic<DirectCTSNode<K,V> *> node; 
        std::atomic<TreeIndexNode *> left;
        std::atomic<TreeIndexNode *> right;
        
        void init(uint64_t inputBirthEpoch, DirectCTSNode<K,V> *n, uint64_t nTS) {
          this->birthEpoch = inputBirthEpoch;
    
          uint64_t nodeVersion = (nTS & ~TS_MASK);
          this->node = (DirectCTSNode<K,V> *) ((uintptr_t)(n) | nodeVersion);
    
          this->key = n->key;
        }
        
        void init(uint64_t inputBirthEpoch, K inputKey) {
          this->birthEpoch = inputBirthEpoch;
          this->key = inputKey;
        }
  
    } __attribute__((aligned((64))));


    
    inline uint64_t getVersion(TreeIndexNode *ptr) {
      TreeIndexNode *tmp = ptr;
      uint64_t shiftedVersion = (uintptr_t)tmp & EPOCH_MASK;
      return (shiftedVersion  >> POINTER_BITS);
    }
    
    inline DirectCTSNode<K,V> *getDirectCTSNode(TreeIndexNode *indexNode) {
      DirectCTSNode<K,V> *tmp = indexNode->node;
      return (DirectCTSNode<K,V> *) ((uintptr_t)(tmp) & ~EPOCH_MASK); 
    }
    
    inline bool isMarked(TreeIndexNode *ptr) {
        return ((intptr_t) ptr & MARK_MASK);
    }
    
    inline bool isFlagged(TreeIndexNode *ptr) {
        return ((intptr_t) ptr & FLAG_MASK);
    }
    
    inline TreeIndexNode *getMarked(TreeIndexNode *ptr) {
        return (TreeIndexNode *)((intptr_t)ptr | MARK_MASK);
    }
    
    inline TreeIndexNode *getFlagged(TreeIndexNode *ptr) {
        return (TreeIndexNode *)((intptr_t)ptr | FLAG_MASK);
    }
    
    inline TreeIndexNode *getUnflagged(TreeIndexNode *ptr) {
        return (TreeIndexNode *)((intptr_t)ptr & ~FLAG_MASK);
    }
    
    inline TreeIndexNode *getRef(TreeIndexNode *ptr) {
        return (TreeIndexNode *)((intptr_t)ptr & ~STATE_MASK);
    }
    
    inline TreeIndexNode *integrate(TreeIndexNode *ptr, uint64_t version) {
      uintptr_t shiftedVersion = version << POINTER_BITS;
      TreeIndexNode *ptrWithoutVersion = (TreeIndexNode *)((intptr_t)ptr & ~EPOCH_MASK);
      TreeIndexNode *tmp = (TreeIndexNode *) ((uintptr_t)(ptrWithoutVersion) | shiftedVersion);
      return tmp;
    }

    inline bool updateLeft(TreeIndexNode *indexNode, TreeIndexNode *expLeft, TreeIndexNode *newLeft) {

      TreeIndexNode *tmp = expLeft;
      return indexNode->left.compare_exchange_strong(tmp, newLeft);
    }
        
    inline bool updateRight(TreeIndexNode *indexNode, TreeIndexNode *expRight, TreeIndexNode *newRight) {

      TreeIndexNode *tmp = expRight;
      return indexNode->right.compare_exchange_strong(tmp, newRight);
    }
    
    inline bool updateNode(TreeIndexNode *indexNode, DirectCTSNode<K,V> *newNode, uint64_t newNodeTS) {
      DirectCTSNode<K,V> *exp = indexNode->node;
      uint64_t expVersion = ((uint64_t)exp & ~TS_MASK);
      uint64_t newVersion = (newNodeTS & ~TS_MASK);
      if (newVersion < expVersion) return false;
      DirectCTSNode<K,V> *tmp = (DirectCTSNode<K,V> *) ((uintptr_t)(newNode) | newVersion);
      return indexNode->node.compare_exchange_strong(exp, tmp);
    }
    
    //volatile char padding0[PREFETCH_SIZE_BYTES];
    Allocator *globalTreeIndexAllocator;
    TreeIndexNode *root;
	  TreeIndexNode *leftChild;

    bool cleanup(TreeIndexNode *ancestor, TreeIndexNode *successor, TreeIndexNode *parent, bool flagLeftChild) {
    
      TreeIndexNode *flagged, *parentLeft, *parentRight, *ancestorLeft, *ancestorRight;
      //bool tagLeftChild = ((key < parent->key) ? (isMarked(parent->left) == false) : (isMarked(parent->right) == true));
      bool updateLeftSuccessor = (getRef(ancestor->left) == successor);
      
      while (true) {
        if (flagLeftChild) {
          parentLeft = parent->left;
          if (currTreeIndexEpoch != getTreeIndexEpoch())
            return false;
          if (isFlagged(parentLeft)) {
            flagged = parentLeft;
            break;
          }
          flagged = getFlagged(parentLeft);  
          if (updateLeft(parent, parentLeft, integrate(flagged, currTreeIndexEpoch)))
            break;
        } else {
          parentRight = parent->right;
          if (currTreeIndexEpoch != getTreeIndexEpoch())
            return false;
          if (isFlagged(parentRight)) {
            flagged = parentRight;
            break;
          }
          flagged = getFlagged(parentRight);  
          if (updateRight(parent, parentRight, integrate(flagged, currTreeIndexEpoch)))
            break;
        }
      }
      
      if (updateLeftSuccessor) {
        ancestorLeft = ancestor->left;
        if (currTreeIndexEpoch != getTreeIndexEpoch())
          return false;
        if (getRef(ancestorLeft) != successor || isMarked(ancestorLeft) || isFlagged(ancestorLeft))
          return false;
        return updateLeft(ancestor, ancestorLeft, integrate(getUnflagged(flagged), currTreeIndexEpoch));
      } else {
        ancestorRight = ancestor->right;
        if (currTreeIndexEpoch != getTreeIndexEpoch())
          return false;
        if (getRef(ancestorRight) != successor || isMarked(ancestorRight) || isFlagged(ancestorRight))
          return false;
        return updateRight(ancestor, ancestorRight, integrate(getUnflagged(flagged), currTreeIndexEpoch));
      }
    
    }
    
    bool find(K key, TreeIndexNode **ancestor_p, TreeIndexNode **successor_p, TreeIndexNode **parent_p, TreeIndexNode **leaf_p) {
    
        TreeIndexNode *ancestor, *successor, *parent, *leaf, *parentField, *currField, *curr;
        K currKey;

      retry_find:
        currTreeIndexEpoch = getTreeIndexEpoch();
        ancestor = root;
        successor = getRef(root->left);
        parent = getRef(root->left);
        leaf = getRef(leftChild->left);
        
        parentField = parent->left;
        currField = leaf->left;
        curr = getRef(currField);
        
        while (curr != nullptr) {
          if (!isFlagged(parentField)) {
            ancestor = parent;
            successor = leaf;
          }
          parent = leaf;
          leaf = curr;
          parentField = currField;
          
          if (key < curr->key){
          	currField = curr->left;
          } else{
          	currField = curr->right;
          }
          curr = getRef(currField);
          if (currTreeIndexEpoch != getTreeIndexEpoch()) {

            goto retry_find;
          }
        }
        
        bool res = (leaf->key == key);
        if (currTreeIndexEpoch != getTreeIndexEpoch()) {

          goto retry_find;
        }
          
        *ancestor_p = ancestor;
        *successor_p = successor;
        *parent_p = parent;
        *leaf_p = leaf;

        
        return res;

    }
    
    DirectCTSNode<K,V> *findTreeIndexPred(K key, K *lessKey) {
        TreeIndexNode *leaf, *curr, *tmp;
        DirectCTSNode<K,V> *node;
        
        K currKey;

      retry_findTreeIndexPred:
        currTreeIndexEpoch = getTreeIndexEpoch();
        currKey = key;
        leaf = getRef(leftChild->left);
        curr = getRef(leaf->left);
        
        while (curr != nullptr) {
          leaf = curr;
          assert(leaf != nullptr);
          if (key <= curr->key){
          	curr = getRef(leaf->left);
          } else{
          	curr = getRef(leaf->right);
            if (curr != nullptr) {

              currKey = leaf->key;
            }
          }
          if (currTreeIndexEpoch != getTreeIndexEpoch()) {

            goto retry_findTreeIndexPred;
          }
        }
        
        assert(leaf != nullptr);
        node = getDirectCTSNode(leaf);
        if (currTreeIndexEpoch != getTreeIndexEpoch()) {
          goto retry_findTreeIndexPred;
        }
        
        assert(node != nullptr);
        
        *lessKey = currKey;
        return node;
     
    }
    
    


  public:
  
    inline uint64_t getTreeIndexEpoch(){
      return localTreeIndexAllocator->getEpoch();
	  }
     

  
    TreeIndex(int numProcesses)
    {
        globalTreeIndexAllocator = new Allocator(getTreeIndexNodeSize(), 2, numProcesses);

    }
    
    void initThread(int tid) {
      if (localTreeIndexAllocator == nullptr)
        localTreeIndexAllocator = new LocalAllocator(globalTreeIndexAllocator, tid);
      currTreeIndexEpoch = 0;
    }
    
    void init(DirectCTSNode<K,V> *head, K maxElement) {
        TreeIndexNode *inf0, *inf1, *inf2, *headInternal, *headLeaf;
        
        
        
        root = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        leftChild = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        inf0 = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        inf1 = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        inf2 = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        headInternal = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        headLeaf = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        
        currTreeIndexEpoch = getTreeIndexEpoch();
        
        root->init(currTreeIndexEpoch, maxElement);
        leftChild->init(currTreeIndexEpoch, maxElement - 1);
        inf0->init(currTreeIndexEpoch, maxElement - 2);
        inf1->init(currTreeIndexEpoch, maxElement - 1);
        inf2->init(currTreeIndexEpoch, maxElement);
        headInternal->init(currTreeIndexEpoch, maxElement - 2);
        headLeaf->init(currTreeIndexEpoch, head, head->ts);
        
        root->left = integrate(leftChild, currTreeIndexEpoch);
        leftChild->left = integrate(headInternal, currTreeIndexEpoch);
        inf0->left = integrate(nullptr, currTreeIndexEpoch);
        inf1->left = integrate(nullptr, currTreeIndexEpoch);
        inf2->left = integrate(nullptr, currTreeIndexEpoch);
        headInternal->left = integrate(headLeaf, currTreeIndexEpoch);
        headLeaf->left = integrate(nullptr, currTreeIndexEpoch);
               
        root->right = integrate(inf2, currTreeIndexEpoch);
        leftChild->right = integrate(inf1, currTreeIndexEpoch);
        inf0->right = integrate(nullptr, currTreeIndexEpoch);
        inf1->right = integrate(nullptr, currTreeIndexEpoch);
        inf2->right = integrate(nullptr, currTreeIndexEpoch);
        headInternal->right = integrate(inf0, currTreeIndexEpoch);
        headLeaf->right = integrate(nullptr, currTreeIndexEpoch);
        
    }
       

    
    bool insert(DirectCTSNode<K,V> *n, uint64_t nTS)
    {
       
        if (n->key % 2 == 0) return false;

        TreeIndexNode *ancestor, *successor, *parent, *leaf, *newInternal, *newLeaf, *parentLeft, *parentRight, *tmp;
        K key = n->key;
        K parentKey, leafKey;
        
        newInternal = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        newLeaf = (TreeIndexNode *) localTreeIndexAllocator->alloc();
        currTreeIndexEpoch = getTreeIndexEpoch();
        
        newLeaf->init(currTreeIndexEpoch, n, nTS);
        newLeaf->left = integrate(nullptr, currTreeIndexEpoch);
        newLeaf->right = integrate(nullptr, currTreeIndexEpoch);

        while (true) {
          if (find(key, &ancestor, &successor, &parent, &leaf) || n->ts != nTS) {
            localTreeIndexAllocator->returnAlloc(newInternal);
            localTreeIndexAllocator->returnAlloc(newLeaf);

            if (n->ts != nTS)
              return false;

            return updateNode(leaf, n, nTS);
          }
          
          
          
          parentKey = parent->key;
          leafKey = leaf->key;
          
                    
          if (key < leafKey) {
            newInternal->init(currTreeIndexEpoch, leafKey);
            newInternal->left = integrate(newLeaf, currTreeIndexEpoch);
            newInternal->right = integrate(leaf, currTreeIndexEpoch);
          } else {         
            newInternal->init(currTreeIndexEpoch, key);
            newInternal->left = integrate(leaf, currTreeIndexEpoch);
            newInternal->right = integrate(newLeaf, currTreeIndexEpoch);
          }
          
          if (key < parentKey) {
            parentLeft = parent->left;
            if (currTreeIndexEpoch != getTreeIndexEpoch() || getRef(parentLeft) != leaf) {

              continue;
            }
            if (isFlagged(parentLeft))
              cleanup(ancestor, successor, parent, true);
            else if (isMarked(parentLeft))
              cleanup(ancestor, successor, parent, false);
            else if (updateLeft(parent, parentLeft, integrate(newInternal, currTreeIndexEpoch)) == true)
              return true;

          } else {
            parentRight = parent->right;
            if (currTreeIndexEpoch != getTreeIndexEpoch()  || getRef(parentRight) != leaf) {

              continue;
            }
            if (isFlagged(parentRight))
              cleanup(ancestor, successor, parent, false);
            else if (isMarked(parentRight))
              cleanup(ancestor, successor, parent, true);
            else if (updateRight(parent, parentRight, integrate(newInternal, currTreeIndexEpoch)) == true)
              return true;
          }

          
          
        }

    }
    
    bool remove(K key)
    {

        if (key % 2 == 0) return false;
        TreeIndexNode *ancestor, *successor, *parent, *leaf;
        TreeIndexNode *newInternal, *newLeaf, *parentLeft, *parentRight, *tmp, *marked, *victimParent, *victimLeaf;
        bool injecting = true, res = false;
        K parentKey;
        
        while (true) {
          
          if (find(key, &ancestor, &successor, &parent, &leaf) == false) {
            break;
          }
          
          parentKey = parent->key;
          
          if (injecting) {
          
            if (key < parentKey) {
              parentLeft = parent->left;
              if (currTreeIndexEpoch != getTreeIndexEpoch() || leaf != getRef(parentLeft))
                continue;
              if (isFlagged(parentLeft))
                cleanup(ancestor, successor, parent, true);
              else if (isMarked(parentLeft))
                cleanup(ancestor, successor, parent, false);
              else {
                marked = getMarked(parentLeft);
                if (updateLeft(parent, parentLeft, marked) == true) {

                  injecting = false;
                  res = true;
                  victimParent = parent;
                  victimLeaf = leaf;
                  if (cleanup(ancestor, successor, parent, false)) 
                    break;
                }
              }

            } else {
              parentRight = parent->right;
              if (currTreeIndexEpoch != getTreeIndexEpoch() || leaf != getRef(parentRight))
                continue;
              if (isFlagged(parentRight))
                cleanup(ancestor, successor, parent, false);
              else if (isMarked(parentRight))
                cleanup(ancestor, successor, parent, true);
              else {
                marked = getMarked(parentRight);
                if (updateRight(parent, parentRight, marked) == true) {

                  injecting = false;
                  res = true;
                  victimParent = parent;
                  victimLeaf = leaf;
                  if (cleanup(ancestor, successor, parent, true)) 
                    break;
                }
              }
            }          
          } else {
            if (leaf != victimLeaf)
              break;
            bool flagLeftChild = (key >= parent->key);
            if (currTreeIndexEpoch == getTreeIndexEpoch() && cleanup(ancestor, successor, parent, flagLeftChild)) {
              break;
            }
          }
        }
        
        if (res) {
          localTreeIndexAllocator->returnAlloc(victimParent);
          localTreeIndexAllocator->returnAlloc(victimLeaf);
        }
        
        return res;

    }

    
    DirectCTSNode<K,V> *findPred(K key, K *lessKey) {

      return findTreeIndexPred(key, lessKey);
    }
    
    size_t getTreeIndexNodeSize() {
      return sizeof(TreeIndexNode);
    }
    
} __attribute__((aligned((64))));

#endif
