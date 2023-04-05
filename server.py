import os
import time
import dill
import threading
from BTrees.OOBTree import OOBTree

BTreePersisFileName = "zipBTree.btree"
snapShotInterVal = 10

def getstate(self):
    return self.__dict__

def setstate(self, state):
    self.__dict__ = state

def load_BTree():
    if not os.path.isfile(BTreePersisFileName):
        return OOBTree()
    else:
        with open(BTreePersisFileName, 'rb') as file:
            return dill.load(file)

def snapShotBuilder(BTree):
    while True:
        with open(BTreePersisFileName, 'wb') as file:
            dill.dump(BTree, file)
        time.sleep(snapShotInterVal)

def main():
    BTree = load_BTree()

    th1 = threading.Thread(target=snapShotBuilder, args=(BTree,))
    th1.start()

    print(BTree)
    print(list(BTree.keys()))

    exit()

    class_attrs_1 = {
        "name": "Alice",
        "mark": 97
    }
    class_attrs_2 = {
        "name": "Bob",
        "mark": 81
    }
    
    class1 = type("ins", (object,), class_attrs_1)
    class1.__getstate__ = getstate
    class1.__setstate__ = setstate

    class2 = type("ins", (object,), class_attrs_2)
    class2.__getstate__ = getstate
    class2.__setstate__ = setstate

    obj1 = class1()
    obj2 = class2()
    
    BTree.update({obj1.name:obj1})
    BTree.update({obj2.name:obj2})

    print(list(BTree.keys()))
    del BTree[obj1.name]
    print(list(BTree.keys()))
    print(BTree["Bob"].mark)

main()