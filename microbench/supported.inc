
g++ ../common/test_htm_support.cpp -o ../common/test_htm_support > /dev/null
../common/test_htm_support &> /dev/null
htm_error=$?

datastructures="abtree bst lflist lazylist citrus skiplistlock"
rqtechniques="snapcollector lockfree rwlock unsafe rlu bundle"

if [ "$htm_error" -eq "0" ]; then
    echo "Note: HTM support detected; including HTM experiments."
    rqtechniques="$rqtechniques htm_rwlock"
else
    echo "Warning: HTM does not appear to be supported; skipping HTM experiments."
fi

check_ds_technique() {
    ## args: ds alg
    if [ "$2" != "vbrunsafe" ] && [ "$2" != "unsafe" ] && [ "$2" != "lockfree" ] && [ "$2" != "vcas" ] && [ "$2" != "mvccvbr" ] && [ "$2" != "lbundle" ] ; then return 1 ; fi
    return 0
}
export -f check_ds_technique

check_ds_size() {
    ## args: ds k
    if [ "$2" == "1000000" ] && [ "$1" != "tree" ]  && [ "$1" != "skiplist" ] ; then return 1 ; fi
    if [ "$2" == "100000" ] && [ "$1" != "tree" ] && [ "$1" != "skiplist" ] ; then return 1 ; fi
    if [ "$2" == "10000" ] && [ "$1" != "list" ] ; then return 1 ; fi
    return 0
}
export -f check_ds_size
