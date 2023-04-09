import os
import glob
import csv
import dill
import uuid
import BTrees
from BTrees.OOBTree import OOBTree
import metaModifier

def ex():
    exit()

testSQL = "SELECT * FROM table1 WHERE field1 = 'value1'"

baseDBDict = {}   #[relation][uuid:173]  -> {id:7, salary:1000}
BTreeDict = {}    #[relation][attribute] -> BTree each key map to a tuple, tuple stores uuids, uuid points to the row
metaDict = {}     #[relation]            -> {id:int, name:str}

snapShotInterVal = 1

def base_lst2mp(relation, lst):
    mp = {}
    cnt = 0
    for k in metaDict[relation]:
        mp.setdefault(k, lst[cnt])
        cnt += 1
    return mp

# Full Write
def persist_snapshot():
    # persist meta
    for relation in metaDict:
        with open(getMetaFileName(relation), 'w', newline='') as f:
            for name in metaDict[relation]:
                datatype = metaDict[relation][name]
                f.write(f"{name} {datatype}\n")

    # persist baseDB
    for relation in baseDBDict:
        with open(getBaseDBFileName(relation), 'w', newline='') as f:
            writer = csv.writer(f)
            for uu in baseDBDict[relation]:
                lst = []
                for k in baseDBDict[relation][uu]:
                    lst.append(baseDBDict[relation][uu][k])
                row = [uu] + lst#baseDBDict[relation][uu]
                writer.writerow(row)

def load_snapshot():
    # load meta relation
    meta_path = '../meta/'
    meta_files = glob.glob(os.path.join(meta_path, '*.meta'))
    file_names = [os.path.basename(file) for file in meta_files]
    for f in file_names:
        relation = f[:-5]
        attr_dict = loadRelationMeta(metaModifier.metaPrefix + f)
        metaDict.setdefault(relation, attr_dict)
    print(metaDict)

    # load baseDB
    baseDB_path = '../baseDB/'
    baseDB_files = glob.glob(os.path.join(baseDB_path, '*.csv'))
    file_names = [os.path.basename(file) for file in baseDB_files]
    for fn in file_names:
        relation = fn[:-4]
        baseDBDict.setdefault(relation, {})
        with open(baseDB_path+fn, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                uuid = row[0]
                lst = row[1:]
                inner_mp = base_lst2mp(relation, lst)
                baseDBDict[relation].setdefault(uuid, inner_mp)
    print(baseDBDict)

def getMetaFileName(relationName):
    return "../meta/"+relationName+".meta"
def getBaseDBFileName(relationName):
    return "../baseDB/"+relationName+".csv"
def getBTreeFileName(relationName, attributeName):
    return "../btree/"+relationName+"|"+attributeName+".btree"


def loadRelationMeta(meta_file):
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
    if not os.path.isfile(getBaseDBFileName(relationName)):       
        baseDBDict.setdefault(relationName,{})
    else:
        with open(getBaseDBFileName(relationName), 'rb') as file:
            mp = dill.load(file)
            print(mp)
            baseDBDict.setdefault(relationName, mp)

def create_table(relationName, cmd):
    #print(metaModifier.create_table("name2salary",[("name","str"),("salary","int")),...])
    #tuList = []
    mp = {}
    iter = 0
    while iter < len(cmd[1:]):
        attName = str(cmd[iter])
        knd = str(cmd[iter+1])
        #tuList.append((attName, knd,))
        mp.setdefault(attName, knd)
        iter += 2
    #metaDict.setdefault(relationName, tuList)
    metaDict.setdefault(relationName, mp)
    #metaModifier.create_table(relationName, tuList)

def write_row(relationName, cmd):
    if relationName not in baseDBDict.keys():
        load_BaseDB(relationName)

    mp = {}
    meta_dict = metaDict[relationName]
    print(meta_dict)
    for i, k in enumerate(meta_dict.keys()):
        if i >= len(cmd):
            print('Error: Too few attributes for', k)
            break
        data_type = meta_dict[k]
        attr_value = cmd[i]
        try:
            if data_type == 'int':
                attr_value = int(attr_value)
            elif data_type == 'str':
                attr_value = str(attr_value)
            mp[k] = attr_value
        except ValueError:
            print('Error: Invalid type for', k)
            break
    else:
        # update base csv
        btree_value = uuid.uuid4()
        baseDBDict[relationName].setdefault(btree_value,{})
        inner_mp = base_lst2mp(relationName, cmd)
        baseDBDict[relationName][btree_value] = inner_mp
        print(baseDBDict)

        # TODO: update index
        #for index in BTreeDict[relationName]:
        #    wp = BTreeDict[relationName][index]
        #    with wp.lock:
        #        wp.update(mp[index], cmd)
    
        #BTree = BTreeDict[(relationName)]
        #BTree.update({cmd[0]: mp})
        #print(mp)
        #print(BTree[cmd[0]])

# ptr id 909
# check if exists indexing(hash/btree), else O(n) scan
def query_equal(relationName, cmd):
    found = False
    attr = cmd[0]
    #if attr not in metaDict[relationName].keys():           #can be solved centralized, plus relationName...
    #    print("No attr in " + str(relationName) + ".")
    #    return
    ret_list = []
    if attr in BTreeDict.keys():
        # TODO: test
        btree = BTreeDict[relationName][attr]
        uu_tuple = btree[cmd[1]]
        for uu in uu_tuple:
            ret_list.append(baseDBDict[uu])
        return ret_list
    else:
        for uu in baseDBDict[relationName]:
            row_mp = baseDBDict[relationName][uu]
            if row_mp[attr] == cmd[1]:
                print("Found:" + str(row_mp))
                found = True
    if not found:
        print("Nothing found.")

def engine():
    load_snapshot()
    operatorCounter = 0
    while True:
        operatorCounter += 1

        # start work
        op = input()
        if int(op) == 0:      # metaModifier.create_table("id2salary",("id","int"),("salary","int")
            cmd = input().split()
            create_table(cmd[0], cmd[1:])
        elif int(op) == 1:    # 1 name2salary Eve 37
            cmd = input().split()
            relationName = cmd[0]
            cmd = cmd[1:]
            write_row(relationName, cmd)
        elif int(op) == 2:    # 2 ptr id 909
            cmd = input().split()
            query_equal(cmd[0],cmd[1:])
        elif int(op) == 3:    # create index
            pass

        # persist
        if operatorCounter % snapShotInterVal == 0:
            persist_snapshot()

def main():
    engine()

main()