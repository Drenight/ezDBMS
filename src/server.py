import os
import time
import dill
import uuid
import threading
from BTrees.OOBTree import OOBTree

from antlr4 import *
from parser import MySqlLexer
from parser import MySqlParser
from antlr4.tree.Trees import Trees

import metaModifier

testSQL = "SELECT * FROM table1 WHERE field1 = 'value1'"

#print(metaModifier.create_table("name2salary",("name","str"),("salary","int")))

baseDBDict = {}   #[relation][uuid:173]  -> {id:7, salary:1000}
BTreeDict = {}    #[relation][attribute] -> BTreeWrapper
#BTreePersisFileName = "zipBTree.btree"
snapShotInterVal = 10

class BTreeWrapper:
    def __init__(self):
        self.btree = OOBTree()
        self.lock = threading.Lock()

    def __getitem__(self, key):
        with self.lock:
            return self.btree[key]

    def __setitem__(self, key, value):
        with self.lock:
            self.btree[key] = value

    def __delitem__(self, key):
        with self.lock:
            del self.btree[key]
    
    def __rangequery__(self, key1, key2):
        pass

# BTree = OOBTree()
# BTreeLock = threading.Lock()

def getBTreeFileName(relationName, attributeName):
    return "btree/BTree|"+relationName+"|"+attributeName+".btree"
def getBaseDBFileName(relationName):
    return "baseDB/BaseDB|"+relationName+".db"

def reloadRelationMeta(meta_file):
    #meta_file = metaModifier.metaPrefix + meta_file
    with open(meta_file) as f:
        meta_info = f.read().strip().split('\n')
    meta_dict = {}
    for info in meta_info:
        name, data_type = info.split()
        meta_dict[name] = data_type
    return meta_dict

def load_BTree(relationName, attributeName):
    if not os.path.isfile(getBTreeFileName(relationName, attributeName)):
        return OOBTree()
    else:
        with open(getBTreeFileName(relationName, attributeName), 'rb') as file:
            return dill.load(file)

def load_BaseDB(relationName):
    baseDBDict.setdefault(relationName,{})

def snapshotBuilder():
    #global BTree, BTreeLock
    global BTreeDict
    while True:
        for k,BTreeWrapper in BTreeDict:
            with BTreeWrapper.lock:
                BTreeFileName = getBTreeFileName(k[0],k[1])
                with open(BTreeFileName, 'wb') as f:
                    dill.dump(BTreeWrapper.btree, f)
        time.sleep(snapShotInterVal)
        # BTreeLock.acquire()
        # with open(BTreePersisFileName, 'wb') as file:
        #     dill.dump(BTree, file)
        # #print("snapshot built")
        # BTreeLock.release()
        # time.sleep(snapShotInterVal)

def engine():
    #global BTree, BTreeLock
    while True:
        op = input()
        #BTreeLock.acquire()
        if int(op) == 0:    # metaModifier.create_table("id2salary",("id","int"),("salary","int")
            print(metaModifier.create_table("name2salary",("name","str"),("salary","int")))
        elif int(op) == 1: # 1 name2salary Eve 37
            cmd = input().split()
            relationName = cmd[0]
            cmd = cmd[1:]

            mp = {}
            meta_file = metaModifier.getDir(relationName) #'meta_mark.meta'
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
                # update base csv
                btree_value = uuid.uuid4()
                baseDBDict[relationName].setdefault(btree_value,{})
                baseDBDict[relationName][btree_value] = cmd

                # update index
                #for index in BTreeDict[relationName]:
                #    wp = BTreeDict[relationName][index]
                #    with wp.lock:
                #        wp.update(mp[index], cmd)
                
                print(baseDBDict)
                #BTree = BTreeDict[(relationName)]
                #BTree.update({cmd[0]: mp})
                #print(mp)
                #print(BTree[cmd[0]])
        elif int(op) == 2:    # query on attribute
            pass
            #print(BTree[cmd[0]]["mark"])
            #print(BTree[cmd[0]]["name"])
        elif int(op) == 3:    # create index
            pass
        #BTreeLock.release()

def main():
    load_BaseDB("name2salary")
    #global BTree, BTreeLock
    #BTree = load_BTree()

    #print(list(BTree.keys()))

    input_stream = FileStream("path_to_input_file.txt")
    lexer = MySqlLexer.MySqlLexer(input_stream)
    token_stream = CommonTokenStream(lexer)

    # 语法分析
    parser = MySqlParser.MySqlParser(token_stream)
    tree = parser.sqlStatement()

    # 可视化
    print(Trees.toStringTree(tree, None, parser))

    exit()

    th_SnapshotBuilder = threading.Thread(target=snapshotBuilder)
    th_MainEngine = threading.Thread(target=engine)

    th_SnapshotBuilder.start()
    th_MainEngine.start()

main()