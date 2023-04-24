import sys
import pickle
from BTrees.OOBTree import OOBTree

sys.setrecursionlimit(1000000)

btree = OOBTree()
for i in range(100000):
    btree[i] = "1234567890"

with open("test_pickle", 'wb') as f:
    pickle.dump(btree, f)
