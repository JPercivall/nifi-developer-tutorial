BRANCH=`git show-ref | grep $(git show-ref -s -- HEAD) | sed 's|.*/\(.*\)|\1|' | grep -v HEAD | sort | uniq`
HASH=`git rev-parse $BRANCH`
PREV=`git rev-list --topo-order HEAD..$HASH | tail -1`
git checkout $PREV
