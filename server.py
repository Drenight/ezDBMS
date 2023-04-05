import os
import time
import dill
import threading
from BTrees.OOBTree import OOBTree

BTreePersisFileName = "zipBTree.btree"
snapShotInterVal = 10

BTree = OOBTree()
BTreeLock = threading.Lock()

def initTest():
    global BTree, BTreeLock
    class_attrs_1 = {
        "name": "Alice",
        "mark": 97
    }
    class_attrs_2 = {
        "name": "Bob",
        "mark": 81
    }
    
    class1 = type("ins", (object,), class_attrs_1)
    class2 = type("ins", (object,), class_attrs_2)

    obj1 = class1()
    obj2 = class2()
    
    BTree.update({obj1.name:obj1})
    BTree.update({obj2.name:obj2})

    #print(list(BTree.keys()))
    #del BTree[obj1.name]
    #print(list(BTree.keys()))
    #print(BTree["Bob"].mark)

def load_BTree():
    if not os.path.isfile(BTreePersisFileName):
        return OOBTree()
    else:
        with open(BTreePersisFileName, 'rb') as file:
            return dill.load(file)

def snapshotBuilder():
    global BTree, BTreeLock
    while True:
        BTreeLock.acquire()
        with open(BTreePersisFileName, 'wb') as file:
            dill.dump(BTree, file)
        BTreeLock.release()
        time.sleep(snapShotInterVal)

def engine():
    global BTree, BTreeLock
    while True:
        id = input()
        BTreeLock.acquire()
        print(BTree[id])
        BTreeLock.release()


def main():
    global BTree, BTreeLock
    BTree = load_BTree()

    #initTest()
    #print(list(BTree.keys()))
    #print(BTree["Alice"])

    th_SnapshotBuilder = threading.Thread(target=snapshotBuilder)
    th_MainEngine = threading.Thread(target=engine)

    th_SnapshotBuilder.start()
    th_MainEngine.start()

main()