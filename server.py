import os
import time
import dill
import threading
from BTrees.OOBTree import OOBTree

BTreePersisFileName = "zipBTree.btree"
snapShotInterVal = 10

BTree = OOBTree()
BTreeLock = threading.Lock()

def reloadRelationMeta(meta_file):
    with open(meta_file) as f:
        meta_info = f.read().strip().split('\n')
    meta_dict = {}
    for info in meta_info:
        name, data_type = info.split()
        meta_dict[name] = data_type
    return meta_dict

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
        #print("snapshot built")
        BTreeLock.release()
        time.sleep(snapShotInterVal)

def engine():
    global BTree, BTreeLock
    while True:
        op, *cmd = input().split()
        BTreeLock.acquire()
        if int(op) == 0: # 0 Eve 37
            mp = {}
            meta_file = 'meta_mark.meta'
            meta_dict = reloadRelationMeta(meta_file)
            for i, name in enumerate(meta_dict):
                if i >= len(cmd):
                    print('Error: Too few attributes for', name)
                    break
                data_type = meta_dict[name]
                attr_value = cmd[i]
                try:
                    if data_type == 'int':
                        attr_value = int(attr_value)
                    elif data_type == 'str':
                        attr_value = str(attr_value)
                    mp[name] = attr_value
                except ValueError:
                    print('Error: Invalid type for', name)
                    break
            else:
                BTree.update({cmd[0]: mp})
                print(mp)
                print(BTree[cmd[0]])
        elif int(op) == 1:
            print(BTree[cmd[0]]["mark"])
            print(BTree[cmd[0]]["name"])
        BTreeLock.release()

def main():
    global BTree, BTreeLock
    BTree = load_BTree()

    print(list(BTree.keys()))

    th_SnapshotBuilder = threading.Thread(target=snapshotBuilder)
    th_MainEngine = threading.Thread(target=engine)

    th_SnapshotBuilder.start()
    th_MainEngine.start()

main()