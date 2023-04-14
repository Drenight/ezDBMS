import os
import glob
import csv
import dill
import uuid
import logging
#import BTrees
import pickle
import parser_CreateTable
from BTrees.OOBTree import OOBTree

############################### Core Mem Data Structure & magic number config #############
baseDBDict = {}                         # [relation][uuid:173]  -> {id:7, salary:1000}
BTreeDict = {}                          # [relation][attribute] -> BTree, each key map to a set, set stores uuids, uuid points to the row
metaDict = {}                           # [relation]            -> {id:int, name:str}
constraintDict = {}                     # [relation]            -> {primary: attr, foreign:{attr:id, rela2:, rela_attr:}}

snapShotInterVal = 2
tmpSQLLog = []

############################### dev tools, for debug and log ###############################
# create a formatter that prints ERROR messages in red
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.ERROR:
            record.msg = '\033[31m' + record.msg + '\033[0m'
        return super(ColoredFormatter, self).format(record)

# configure the root logger to handle all log levels
logging.basicConfig(level=logging.DEBUG)

# create a console handler and set its formatter to the colored formatter
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_handler.setFormatter(ColoredFormatter('%(levelname)s: %(message)s'))

# add the console handler to the root logger
logging.getLogger().addHandler(console_handler)
err_logger = logging.getLogger('err_logger')

def ex():
    exit()

def raiseErr(message):
    raise Exception(message)

class PrimaryKeyError(Exception):
    pass
class ForeignKeyError(Exception):
    pass

############################### functions to utilize core data structure ############################### 

def base_lst2mp(relation, lst):
    mp = {}
    cnt = 0
    for k in metaDict[relation]:
        if metaDict[relation][k] == "str":
            mp.setdefault(k, str(lst[cnt]))
        else:
            mp.setdefault(k, int(lst[cnt]))
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

    # persist btree
    for relation in BTreeDict:
        for attr in BTreeDict[relation]:
            btree = BTreeDict[relation][attr]
            file_name = getBTreeFileName(relation, attr)
            dir_name = os.path.dirname(file_name)
            os.makedirs(dir_name, exist_ok=True)
            with open(file_name, 'wb') as f:
                pickle.dump(btree, f)

    #persist constraint
    for relation in constraintDict:
        with open(getConstraintFileName(relation), 'wb') as f:
            pickle.dump(constraintDict[relation], f)

def load_snapshot():
    # load meta relation
    meta_path = 'meta/'
    meta_files = glob.glob(os.path.join(meta_path, '*.meta'))
    file_names = [os.path.basename(file) for file in meta_files]
    for f in file_names:
        relation = f[:-5]
        attr_dict = loadRelationMeta("meta/" + f)
        metaDict.setdefault(relation, attr_dict)
    logging.debug("Successfully load metaDict " + str(metaDict))

    # load baseDB
    baseDB_path = 'baseDB/'
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
    logging.debug("Successfully load baseDBDict " + str(baseDBDict))

    # load btree
    btree_path = 'btree/'
    for dir_path, sub_dirs, files in os.walk(btree_path):
        relation = dir_path[6:]
        if relation == "": # root, nothing
            continue
        BTreeDict.setdefault(relation, {})
        for file_name in files:
            file_path = os.path.join(dir_path, file_name)
            #print(dir_path, sub_dirs, files, file_path)
            attr = file_name[:-6]
            with open(file_path, 'rb') as f:
                btree = pickle.load(f)
                BTreeDict[relation].setdefault(attr, btree)
                #print(BTreeDict[relation][attr])
    logging.debug("Successfully load BTreeDict " + str(BTreeDict))
    #print(BTreeDict["ptr"]["id"]["909"])
            
    #load constraint
    constraint_path = 'constraint/'
    constraint_files = glob.glob(os.path.join(constraint_path, '*.constrain'))
    file_names = [os.path.basename(file) for file in constraint_files]
    for fn in file_names:
        relation = fn[:-10]
        with open(getConstraintFileName(relation), 'rb') as file:
            mp = pickle.load(file)
            constraintDict.setdefault(relation, mp)
    logging.debug("Successfully load constraintDict " + str(constraintDict))
    
def getMetaFileName(relationName):
    return "meta/"+relationName+".meta"
def getBaseDBFileName(relationName):
    return "baseDB/"+relationName+".csv"
def getBTreeFileName(relationName, attributeName):
    return "btree/"+relationName+"/"+attributeName+".btree"
def getConstraintFileName(relationName):
    return "constraint/"+relationName+".constrain"

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
            logging.debug(mp)#print(mp)
            baseDBDict.setdefault(relationName, mp)
             
def create_table(relationName, cmd):
    if relationName in metaDict.keys():
        raiseErr("relation exists")

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
        #load_BaseDB(relationName)
        # TODO 不确定
        baseDBDict.setdefault(relationName, {})

    mp = {}
    meta_dict = metaDict[relationName]
    logging.debug(meta_dict)
    #print(meta_dict)
    for i, k in enumerate(meta_dict.keys()):
        if i >= len(cmd):
            raiseErr('Error: Too few attributes for', k)
        data_type = meta_dict[k]
        attr_value = cmd[i]
        try:
            if data_type == 'int':
                attr_value = int(attr_value)
            elif data_type == 'str':
                attr_value = str(attr_value)
            mp[k] = attr_value
        except ValueError:
            raiseErr('Error: Invalid type for', k)
    else:
        # check primary key 
        if 'primary' in constraintDict[relationName]:
            pk = constraintDict[relationName]['primary']
            lst = query_equal(relationName,[pk, mp[pk],])
            if len(lst) != 0:
                raise PrimaryKeyError('Error: Primary key duplicate', mp[pk])
        # check foreign key

        # update base csv
        btree_value = str(uuid.uuid4())
        baseDBDict[relationName].setdefault(btree_value,{})
        inner_mp = base_lst2mp(relationName, cmd)
        baseDBDict[relationName][btree_value] = inner_mp
        logging.debug(baseDBDict)

        # update index
        # if relationName not in BTreeDict.
        for index in BTreeDict[relationName]:
            btree = BTreeDict[relationName][index]
            k = inner_mp[index]
            if k not in btree.keys():
                btree.setdefault(k, set())
            btree[k].add(btree_value)

# ptr id 909
# check if exists indexing(hash/btree), else O(n) scan
def query_equal(relationName, cmd):
    found = False
    attr = cmd[0]
    if metaDict[relationName][attr] == "int":
        val = int(cmd[1])
    else:
        val = str(cmd[1])

    ret_list = []
    if relationName in BTreeDict.keys() and  attr in BTreeDict[relationName].keys():
        # TODO: test
        btree = BTreeDict[relationName][attr]
        #print(relationName, attr)
        logging.debug(str(relationName)+" " + str(attr))

        #print(list(btree.keys()))
        uu_set = btree[val]
        #print(uu_set)
        logging.debug(uu_set)
        for uu in uu_set:
            logging.debug(uu, relationName)
            ret_list.append(baseDBDict[relationName][uu])
        print("By index, found: "+str(ret_list))
    else:
        for uu in baseDBDict[relationName]:
            row_mp = baseDBDict[relationName][uu]
            if row_mp[attr] == val:
                print("By linear scaning, Found: " + str(row_mp))
                found = True
                ret_list.append(baseDBDict[relationName][uu])
    if not found:
        print("Nothing found.")
    return ret_list

def query_range(relationName, cmd):
    # TODO 最大最小未指定
    mn = int(cmd[1])
    mx = int(cmd[2])
    k = cmd[0]
    found = False
    if k in BTreeDict[relationName].keys():
        btree = BTreeDict[relationName][k]
        print("range query by indexing...")

        logging.debug((list(btree.keys())))
        logging.debug((mn, mx))
        for key, value in btree.items(min=mn, max=mx):
            found = True
            print(key, value)
    else:
        for uu in baseDBDict[relationName]:
            row_mp = baseDBDict[relationName][uu]
            if row_mp[k]>=mn and row_mp[k]<=mx:
                found = True
                print(row_mp)
    if not found:
        print("Nothing found.")

def create_index(relationName, cmd):
    indexAttr = cmd[0]
    if relationName in BTreeDict.keys() and indexAttr in BTreeDict[relationName].keys():
        raiseErr("Index exists.")
    if relationName not in BTreeDict.keys():
        BTreeDict.setdefault(relationName, {})
    if indexAttr not in BTreeDict[relationName].keys():
        BTreeDict[relationName].setdefault(indexAttr, OOBTree())
    for uu in baseDBDict[relationName]:
        btree = BTreeDict[relationName][indexAttr]
        k = baseDBDict[relationName][uu][indexAttr]
        if k not in btree.keys():
            btree[k] = set()
        btree[k].add(uu)
        logging.debug(BTreeDict[relationName][indexAttr][baseDBDict[relationName][uu][indexAttr]])

def del_row(relationName, cmd):
    uu = cmd[0]
    row_mp = baseDBDict[relationName][uu]
    for k in BTreeDict[relationName]:
        BTreeDict[relationName][k][row_mp[k]].remove(uu)
        if len(BTreeDict[relationName][k][row_mp[k]]) == 0:
            del BTreeDict[relationName][k][row_mp[k]]
    del baseDBDict[relationName][uu]

def create_primary(relationName, attr):
    logging.debug(relationName, attr)
    if relationName not in constraintDict.keys():
        constraintDict.setdefault(relationName, {})
    if "primary" in constraintDict[relationName].keys():
        raise PrimaryKeyError(f"{relationName} already has a primary key.")
    else:
        constraintDict[relationName].setdefault("primary", attr)

def create_foreign(relationName, attr, rela2, attr2):
    logging.debug(relationName, attr, rela2, attr2)
    if attr2 not in metaDict[rela2].keys():
        pass
    if metaDict[rela2][attr2] != metaDict[relationName][attr]:
        pass
    if "foreign" in constraintDict[relationName].keys():
        raise ForeignKeyError(f"{relationName} already has a foreign key.")
    else:
        constraintDict[relationName].setdefault("foreign", {})
        constraintDict[relationName]["foreign"].setdefault("rela2", rela2)
        constraintDict[relationName]["foreign"].setdefault("attr2", attr2)

def read_sql():
    sql = ""
    while True:
        line = input()
        if line.endswith(";"):
            sql += line
            break
        else:
            sql += line + " "
    return sql

def mem_exec(sql):
    op = 999
    # create table 
    # CREATE TABLE TTT(ID INT PRIMARY KEY);
    # TODO? 根据语法树根节点，不采用字符串查询，不搞
    if sql.upper().find("CREATE TABLE") != -1:
        virtual_plan = parser_CreateTable.virtual_plan_create(sql)
        logging.debug(virtual_plan.__dict__)
        lst = []
        for pr in virtual_plan.columns:
            lst.append(pr['name'])
            lst.append(pr['type'])
        create_table(virtual_plan.table_name, lst)
        if virtual_plan.primary_key:
            create_primary(virtual_plan.table_name, virtual_plan.primary_key)                       #promise, single attr
            # create_index(virtual_plan.table_name, virtual_plan.primary_key)
            # 只需要注册就行，不需要调用creat_index，因为baseDB没东西
            BTreeDict.setdefault(virtual_plan.table_name, OOBTree())
        if virtual_plan.foreign_key:
            create_foreign(
            virtual_plan.table_name, virtual_plan.foreign_key["local_columns"][0], 
            virtual_plan.foreign_key["table"], virtual_plan.foreign_key["foreign_columns"][0]
            )        #promise, single attr
        
    elif sql.upper().find("DROP TABLE") != -1:
        pass

    # basic demo
    if int(op) == 0:      # 0 test_table id int name str
        cmd = input().split()
        create_table(cmd[0], cmd[1:])
    elif int(op) == 1:    # 1 ptr 909 Alice, write a row
        cmd = input().split()
        write_row(cmd[0], cmd[1:])
    elif int(op) == 2:    # 2 ptr id 909, equal query
        cmd = input().split()
        query_equal(cmd[0], cmd[1:])
    elif int(op) == 3:    # 3 ptr id 700 1000, range query
        cmd = input().split()
        query_range(cmd[0], cmd[1:])
    elif int(op) == 4:    # 4 ptr id, create index
        cmd = input().split()
        create_index(cmd[0], cmd[1:])
    elif int(op) == 5:    # 5 ptr uuid, del a row
        cmd = input().split()
        del_row(cmd[0], cmd[1:])

def dirty_cache_rollback_and_commit():
    metaDict.clear()
    baseDBDict.clear()
    BTreeDict.clear()
    constraintDict.clear()
    load_snapshot()
    for query in tmpSQLLog:
        mem_exec(query)
    persist_snapshot()

def engine():
    load_snapshot()
    sqlCounter = 0
    while True:
        # TODO:
        # ctrl+c之类关机，把tmpSQLLog里的东西写进磁盘，再关机
        # 或者，commit的东西，汇报commit

        # start work
        print("Waiting for your sql, input quit to exit without cmd+C...")
        print(">",end='')
        sql = read_sql()
        if sql == "quit;":
            print("Bye!")
            exit(0)

        try:
            mem_exec(sql)
        except Exception as e:  # Capture any exception, raise runnable error like primary/foreign here
            err_logger.error(f"sql runtime error: {e}")
            #print("sql runtime error:", e)
            print("Start cleaning dirty cache, rollback to commit previous sql...")
            dirty_cache_rollback_and_commit()
            tmpSQLLog.clear()
            print("==============================================================================")
            continue # skip regular persist
        sqlCounter += 1
        # persist
        if sqlCounter % snapShotInterVal == 0:
            #global baseDBDict, BTreeDict, metaDict, constraintDict
            persist_snapshot()
            tmpSQLLog.clear()
            print("Periodcally persists done, all previous sql committed!")
            print("==============================================================================")

def main():
    engine()

main()