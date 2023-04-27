import re
import os
import csv
import sys
import glob
import math
import dill
import uuid
import time
import pydoc
import pickle
import shutil
import logging
import threading
import traceback
from prettytable import PrettyTable
from tqdm import tqdm
from BTrees.OOBTree import OOBTree

# my parsers
import parser_CreateTable
import parser_Insert
import parser_Select
import parser_DropTable
import parser_Delete
import parser_Update

from Treap import treap

############################### Core Mem Data Structure & magic number config #############
baseDBDict = {}                         # [relation][uuid:173]  -> {id:7, salary:1000}
BTreeDict = {}                          # [relation][attribute] -> BTree, each key map to a set, set stores uuids, uuid points to the row
TreapDict = {}                          # [relation][attribute] -> Treap_root, store only key
metaDict = {}                           # [relation]            -> {id:int, name:str}
constraintDict = {}      #TODO 0->1     # [relation]            -> {primary: attr, foreign0:[{attr:, rela2:, attr2:},...], foreign1:[{attr:, rela2:, attr2:},...]}

snapShotInterVal = 1
tmpSQLLog = []
lazyDropRelationList = []

conditionOptimizerFlag = True  # only support int index
joinOptimizerFlag = True

sys.setrecursionlimit(10000000)
threading.stack_size(67108864)

############################### dev tools, for debug and log ###############################
# create a formatter that prints ERROR messages in red
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.ERROR:
            record.msg = '\033[31m' + record.msg + '\033[0m'
        return super(ColoredFormatter, self).format(record)

# configure the root logger to handle all log levels
logging.basicConfig(level=logging.ERROR)

# create a console handler and set its formatter to the colored formatter
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
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

    # persist treap
    for relation in TreapDict:
        for attr in TreapDict[relation]:
            treap = TreapDict[relation][attr]
            file_name = getTreapFileName(relation, attr)
            dir_name = os.path.dirname(file_name)
            os.makedirs(dir_name, exist_ok=True)
            with open(file_name, 'wb') as f:
                pickle.dump(treap, f)

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
    #logging.debug("Successfully load metaDict " + str(metaDict))

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
    #logging.debug("Successfully load baseDBDict " + str(baseDBDict))

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
    #logging.debug("Successfully load BTreeDict " + str(BTreeDict))
    #print(BTreeDict["ptr"]["id"]["909"])

    # load treap
    treap_path = 'treap/'
    for dir_path, sub_dirs, files in os.walk(treap_path):
        relation = dir_path[6:]
        if relation == "": # root, nothing
            continue
        TreapDict.setdefault(relation, {})
        for file_name in files:
            file_path = os.path.join(dir_path, file_name)
            #print(dir_path, sub_dirs, files, file_path)
            attr = file_name[:-6]
            with open(file_path, 'rb') as f:
                treap = pickle.load(f)
                TreapDict[relation].setdefault(attr, treap)
    #logging.debug("Successfully load TreapDict " + str(TreapDict))

    #load constraint
    constraint_path = 'constraint/'
    constraint_files = glob.glob(os.path.join(constraint_path, '*.constrain'))
    file_names = [os.path.basename(file) for file in constraint_files]
    for fn in file_names:
        relation = fn[:-10]
        with open(getConstraintFileName(relation), 'rb') as file:
            mp = pickle.load(file)
            constraintDict.setdefault(relation, mp)
    #logging.debug("Successfully load constraintDict " + str(constraintDict))
    
def getMetaFileName(relationName):
    return "meta/"+relationName+".meta"
def getBaseDBFileName(relationName):
    return "baseDB/"+relationName+".csv"
def getBTreeFileName(relationName, attributeName):
    return "btree/"+relationName+"/"+attributeName+".btree"
def getTreapFileName(relationName, attributeName):
    return "treap/"+relationName+"/"+attributeName+".treap"
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

#def load_BTree(relationName, attributeName):
#    if not os.path.isfile(getBTreeFileName(relationName, attributeName)):
#        return OOBTree()
#    else:
#        with open(getBTreeFileName(relationName, attributeName), 'rb') as file:
#            return dill.load(file)

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
        mp.setdefault(attName, knd)
        iter += 2
    metaDict.setdefault(relationName, mp)
    baseDBDict.setdefault(relationName, {})

# DONE 外键约束，实现上直接让外键约束失效吧？rela1和rela2就可以统一处理了
def drop_table(relationName):
    lazyDropRelationList.append(relationName)
    del baseDBDict[relationName]
    if relationName in BTreeDict.keys():    #不能爆，万一没呢，兼容下不带主键建表吧虽然默认带主键了
        del BTreeDict[relationName]
    if relationName in TreapDict.keys():
        del TreapDict[relationName]
    del metaDict[relationName]

    if relationName in constraintDict.keys():
        if 'foreign0' in constraintDict[relationName].keys():
            constrainMpList = constraintDict[relationName]['foreign0']
            toDelMp = []

            for constrainMP in constrainMpList:
                attr = constrainMP['attr']
                rela2 = constrainMP['rela2']
                attr2 = constrainMP['attr2']
                for constrainMP2 in constraintDict[rela2]['foreign1']:
                    if constrainMP2['attr'] == attr2 and constrainMP2['rela2'] == relationName and constrainMP2['attr2'] == attr:
                        toDelMp.append(constrainMP2)
                for constrainMP2 in toDelMp:
                    constraintDict[rela2]['foreign1'].remove(constrainMP2)
                if len(constraintDict[rela2]['foreign1']) == 0:
                    del constraintDict[rela2]['foreign1']
                logging.debug("Should have clean arrow head, rela2 is:"+rela2+" "+str(constraintDict[rela2]))

        if 'foreign1' in constraintDict[relationName].keys():
            relaS = ""
            for constrainMP in constraintDict[relationName]['foreign1']:
                relaS += str(constrainMP['rela2'])+'\n'
            raise ForeignKeyError('This relation\'s primary key is still a reference key in these relations:\n' + relaS +'Cant drop now.')

        del constraintDict[relationName]

# DONE 主键外键约束，底层模块做
def write_row(relationName, cmd):
    mp = {}
    meta_dict = metaDict[relationName]
    logging.debug("meta_dict: "+str(meta_dict))

    for i, k in enumerate(meta_dict.keys()):
        if i >= len(cmd):
            raiseErr('Error: Too few attributes for' + str(k))
        data_type = meta_dict[k]
        attr_value = cmd[i]
        try:
            if data_type == 'int':
                attr_value = int(attr_value)
            elif data_type == 'str':
                attr_value = str(attr_value)
            mp[k] = attr_value
        except ValueError:
            raiseErr('Error: Invalid type for' + str(k))
    else:
        # check primary key 
        if relationName in constraintDict:
            if 'primary' in constraintDict[relationName]:
                primaryKey = constraintDict[relationName]['primary']
                lst = query_equal(relationName,[primaryKey, mp[primaryKey],])
                if len(lst) != 0:
                    raise PrimaryKeyError('Error: Primary key duplicate ' + str(mp[primaryKey]))
        
        # check foreign key
        if relationName in constraintDict.keys() and 'foreign0' in constraintDict[relationName].keys():
            for foreign_mp in constraintDict[relationName]['foreign0']: #箭尾，检测箭头的主键是否有值
                rela2 = foreign_mp['rela2']
                attr2 = foreign_mp['attr2']
                attr = foreign_mp['attr']
                if mp[attr] not in BTreeDict[rela2][attr2].keys():  #由于是箭头一定是主键，必定可以直接check b树, attr2是主键
                    raise ForeignKeyError('New value didnt show in the referenced relation '+str(rela2)+'.'+str(attr2))

        # update base csv
        btree_value = str(uuid.uuid4())
        baseDBDict[relationName].setdefault(btree_value,{})
        inner_mp = base_lst2mp(relationName, cmd)
        baseDBDict[relationName][btree_value] = inner_mp
        logging.debug(baseDBDict)

        # update index
        if relationName in BTreeDict:
            for index in BTreeDict[relationName]:
                btree = BTreeDict[relationName][index]
                k = inner_mp[index]
                if k not in btree.keys():
                    btree.setdefault(k, set())
                btree[k].add(btree_value)

        if relationName in TreapDict:
            for index in TreapDict[relationName]:
                treap_root = TreapDict[relationName][index]
                k = inner_mp[index]
                #if treap.find(treap_root, k) == 0:
                TreapDict[relationName][index] = treap.insert(treap_root, k)
                #if k not in btree.keys():
                #    btree.setdefault(k, set())
                #btree[k].add(btree_value)

        #print(btree[inner_mp[primaryKey]])

def judge_row(row_uu, virtual_plan):
    #print("77")
    if virtual_plan.where_expr != None:
        brkRelaNameIndex1 = 0
        brkRelaNameIndex2 = 0
        brkAttrNameIndex1 = 0
        brkAttrNameIndex2 = 0

        #print("expr1:", virtual_plan.where_expr1_eval)
        #print("expr2:", virtual_plan.where_expr2_eval)

        for ch in virtual_plan.where_expr1_eval:
            if not ch.isalnum() and ch=='.':
                brkRelaNameIndex1 = brkAttrNameIndex1
            if not ch.isalnum() and ch!='.' and ch!='_':
                break
            brkAttrNameIndex1 += 1
        if virtual_plan.where_expr2_eval != None:
            for ch in virtual_plan.where_expr2_eval:
                if not ch.isalnum() and ch=='.':
                    brkRelaNameIndex2 = brkAttrNameIndex2
                if not ch.isalnum() and ch!='.' and ch!='_':
                    break
                brkAttrNameIndex2 += 1

        where_expr1_rela = virtual_plan.where_expr1_eval[:brkRelaNameIndex1]
        where_expr1_attr = virtual_plan.where_expr1_eval[brkRelaNameIndex1+1:brkAttrNameIndex1]
        logging.debug("First where expr rela&attr is "+str(where_expr1_rela)+"&"+str(where_expr1_attr))
        if virtual_plan.where_expr2_eval != None:
            where_expr2_rela = virtual_plan.where_expr2_eval[:brkRelaNameIndex2]
            where_expr2_attr = virtual_plan.where_expr2_eval[brkRelaNameIndex2+1:brkAttrNameIndex2]
            logging.debug("Second where expr rela&attr is "+str(where_expr2_rela)+"&"+str(where_expr2_attr))

    if virtual_plan.where_expr == None:
        return True
    elif virtual_plan.where_expr2_eval == None:   # use where filter rows
        ans = baseDBDict[where_expr1_rela][row_uu][where_expr1_attr]
        if metaDict[where_expr1_rela][where_expr1_attr] == 'str':
            ans = '\''+ans+'\''

        tmpEvalS = virtual_plan.where_expr1_eval.replace(virtual_plan.where_expr1_eval[:brkAttrNameIndex1], str(ans))
        if eval(tmpEvalS):
            return True
    else:   # double expr && no join
        ans1_where_no_join = baseDBDict[where_expr1_rela][row_uu][where_expr1_attr]
        if metaDict[where_expr1_rela][where_expr1_attr] == 'str':
            ans1_where_no_join = '\''+ans1_where_no_join+'\''
        ans2_where_no_join = baseDBDict[where_expr2_rela][row_uu][where_expr2_attr]
        if metaDict[where_expr2_rela][where_expr2_attr] == 'str':
            ans2_where_no_join = '\''+ans2_where_no_join+'\''

        tmpEvalS1 = virtual_plan.where_expr1_eval.replace(virtual_plan.where_expr1_eval[:brkAttrNameIndex1], str(ans1_where_no_join))
        tmpEvalS2 = virtual_plan.where_expr2_eval.replace(virtual_plan.where_expr2_eval[:brkAttrNameIndex2], str(ans2_where_no_join))
        logging.debug("ok double where without join: "+str(tmpEvalS1)+" "+str(tmpEvalS2))
        if eval(tmpEvalS1+" "+str(virtual_plan.where_logic)+" "+tmpEvalS2):
            return True

#del by uu
def del_row_using_uu_func(relationName, uu):
    row_mp = baseDBDict[relationName][uu]
    for k in BTreeDict[relationName]:
        BTreeDict[relationName][k][row_mp[k]].remove(uu)
        if len(BTreeDict[relationName][k][row_mp[k]]) == 0:
            del BTreeDict[relationName][k][row_mp[k]]
    for k in TreapDict[relationName]:
        treap_root = TreapDict[relationName][k]
        #print(k, "attr")
        #print()
        TreapDict[relationName][k] = treap.delete(treap_root, row_mp[k])
    del baseDBDict[relationName][uu]
def erase_row(virtual_plan):
    rela = virtual_plan.table_name
    del_row_uu = []
    for row_uu in baseDBDict[rela]:
        if judge_row(row_uu, virtual_plan):
            del_row_uu.append(row_uu)
    logging.debug("del_row_uu now is: "+str(del_row_uu))
    if "primary" not in constraintDict[rela].keys() or "foreign1" not in constraintDict[rela].keys():
        for row_uu in del_row_uu:
            del_row_using_uu_func(rela, row_uu)
    else:    
        cantDelPrimaryKeyList = []
        #外键，check主键是不是被指向，list这些行不能被删，存在就不做这次操作，打印这些行的主键
        attr = constraintDict[rela]['primary']

        constrainMpList = constraintDict[rela]['foreign1']
        for constrainMP in constrainMpList:
            rela2 = constrainMP['rela2']
            attr2 = constrainMP['attr2']
            for row_uu in del_row_uu:
                if attr2 in BTreeDict[rela2].keys():
                    if baseDBDict[rela][row_uu][attr] in BTreeDict[rela2][attr2].keys(): #attr2是主键
                        cantDelPrimaryKeyList.append(str(baseDBDict[rela][row_uu][attr])+" in "+str(rela2)+"."+str(attr2))
                else:
                    for row_uu2 in baseDBDict[rela2]:
                        if baseDBDict[rela][row_uu][attr] == baseDBDict[rela2][row_uu2][attr2]:
                            cantDelPrimaryKeyList.append(str(baseDBDict[rela][row_uu][attr])+" in "+str(rela2)+"."+str(attr2))
                            break
        if len(cantDelPrimaryKeyList) == 0:
            for row_uu in del_row_uu:
                del_row_using_uu_func(rela, row_uu)
        else:
            raise ForeignKeyError('This relation has following primary keys which is still a reference key in other relations:\n' + str(cantDelPrimaryKeyList) +'\nDelete nothing.')

def update_row(virtual_plan):
    rela = virtual_plan.table_name
    # primary key cant upd
    if rela in constraintDict.keys() and "primary" in constraintDict[rela].keys():
        if constraintDict[rela]["primary"] in virtual_plan.columnMp.keys():
            raise PrimaryKeyError('primary key cant upd, nothing changed')

    # where linear scan
    rela = virtual_plan.table_name
    upd_row_uu = []
    for row_uu in baseDBDict[rela]:
        if judge_row(row_uu, virtual_plan):
            upd_row_uu.append(row_uu)
    logging.debug("upd_row_uu now is: "+str(upd_row_uu))

    if len(upd_row_uu) == 0:
        print("No row matched, nothing updated.")
        return

    # foreign constrains
    # check 这个row的外键列【新】值v 在不在1表出现
    if rela in constraintDict.keys() and "foreign0" in constraintDict[rela].keys():
        for cons in constraintDict[rela]["foreign0"]:
            attr  = cons['attr']
            rela2 = cons['rela2']
            attr2 = cons['attr2']
            new_v = virtual_plan.columnMp[attr]
            if new_v not in BTreeDict[rela2][attr2].keys():
                raise ForeignKeyError('New value:'+str(new_v)+ ' didnt show in the referenced relation '+str(rela2)+'.'+str(attr2))

    # check 这个row的外键列【原】值v是不是唯一一个 && 0表里出现了这个v  
    # 蠢了，外键列是主键，当然是唯一一个，直接检查改没改主键就done
    #if rela in constraintDict.keys() and "foreign1" in constraintDict[rela].keys():
    #    if constraintDict[rela]['primary'] in virtual_plan.columnMp.keys():
    #额，甚至不用做了，改主键在最上面就check过了，也就是foreign1，箭头的变更被连带维护了

    #...没写baseDB
    for uu in upd_row_uu:
        for attr in virtual_plan.columnMp.keys():
            baseDBDict[rela][uu][attr] = virtual_plan.columnMp[attr]

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
        logging.debug(str(relationName)+" " + str(attr))

        if val not in btree.keys():
            return ret_list
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

# optional, not maintained for a while, don't use directly
# 0421 recycling
def create_index(relationName, indexAttr):
    #BTreeDict.setdefault(virtual_plan.table_name, {})
    #BTreeDict[virtual_plan.table_name].setdefault(virtual_plan.primary_key, OOBTree())
    if relationName in BTreeDict.keys() and indexAttr in BTreeDict[relationName].keys():
        raiseErr("Index exists.")
    if relationName not in BTreeDict.keys():
        BTreeDict.setdefault(relationName, {})
        TreapDict.setdefault(relationName, {})
    if indexAttr not in BTreeDict[relationName].keys():
        BTreeDict[relationName].setdefault(indexAttr, OOBTree())
        TreapDict[relationName].setdefault(indexAttr, None)
    for uu in baseDBDict[relationName]:
        btree = BTreeDict[relationName][indexAttr]
        treap_root = TreapDict[relationName][indexAttr]

        k = baseDBDict[relationName][uu][indexAttr]

        if k not in btree.keys():
            btree[k] = set()
        btree[k].add(uu)
        logging.debug("now btree set is " + str(BTreeDict[relationName][indexAttr][baseDBDict[relationName][uu][indexAttr]]))

        TreapDict[relationName][indexAttr] = treap.insert(treap_root, k)
        logging.debug("in treap, k shows " +str(treap.find(treap_root, k)))

# 下面两个操作真正实现约束在增删改，因为只会在建表调用他们
# 由于这个操作只在建表有，这里不做重复值检测
# 涉及加主键列
def create_primary_cons(relationName, attr):
    logging.debug("relation Name: "+str(relationName)+"attr: "+str(attr))
    if relationName not in constraintDict.keys():
        constraintDict.setdefault(relationName, {})
    if "primary" in constraintDict[relationName].keys():
        raise PrimaryKeyError(f"{relationName} already has a primary key.")
    else:
        constraintDict[relationName].setdefault("primary", attr)

# 这个引用关系用于确保在该表中的数据始终关联到另一个表中的已有数据
# 由于这个操作只在建表有，这里不做参考存在检测，只需要检测是不是ref键是不是主键
# rela1：涉及加外键列
# rela2：涉及删主键列
# relationName[attr] -> rela2[attr2]
def create_foreign_cons(relationName, attr, rela2, attr2):
    logging.debug("rela1: "+str(relationName)+"attr1: "+str(attr)+"rela2: "+str(rela2)+"attr2: "+str(attr2))
    if rela2 not in constraintDict.keys():
        raiseErr("Didn't find " + str(rela2) + " has primary key constrains, or it does'nt exist at all")
    if attr2 != constraintDict[rela2]["primary"]:
        raiseErr(str(attr2)+" is not a primary key in "+str(rela2))
    if metaDict[rela2][attr2] != metaDict[relationName][attr]:
        raiseErr("meta for two attrs mismatch, like int points to str")
    # arrow tail
    constraintDict[relationName].setdefault("foreign0", list())
    tmpMP = {}
    tmpMP.setdefault("attr", attr)
    tmpMP.setdefault("rela2", rela2)
    tmpMP.setdefault("attr2", attr2)
    constraintDict[relationName]["foreign0"].append(tmpMP)
    # arrow head
    constraintDict[rela2].setdefault("foreign1", list())
    tmpMP = {}
    tmpMP.setdefault("attr", attr2)
    tmpMP.setdefault("rela2", relationName)
    tmpMP.setdefault("attr2", attr)
    constraintDict[rela2]["foreign1"].append(tmpMP)

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

def aggr_row_func(rowList, aggr_func, target_attr):  # row is a list of {'id2salary.id': 909, 'id2salary.salary': 2000}
    logging.debug("rowList is: "+str(rowList))
    if aggr_func.upper() == "MIN" or aggr_func.upper() == "MAX":
        ret = rowList[0][target_attr]
        for row in rowList:
            if aggr_func.upper() == "MIN":
                ret = min(ret, row[target_attr])
            if aggr_func.upper() == "MAX":
                ret = max(ret, row[target_attr])
    elif aggr_func.upper() == "SUM" or aggr_func.upper() == "AVG":
        ret = 0
        for row in rowList:
            ret += row[target_attr]
        if aggr_func.upper() == "AVG":
            ret /= len(rowList)
    elif aggr_func.upper() == "COUNT":
        return len(rowList)
    elif aggr_func.upper() == "DISTINCT":
        st = set()
        for row in rowList:
            st.add(row[target_attr])
        return len(st)
    return ret

def satisfy_condition_optimizer(where_expr1_rela, where_expr1_attr, where_expr2_rela, where_expr2_attr):
    if conditionOptimizerFlag:
        if metaDict[where_expr1_rela][where_expr1_attr]=='int' and metaDict[where_expr2_rela][where_expr2_attr]=='int':
            return True
    return False

def calc_rows_by_treap(rela, attr, op, val):
    # TreapDict[rela][attr]
    if rela not in TreapDict.keys() or attr not in TreapDict[rela].keys() or TreapDict[rela][attr]==None:
        return -1
    if op == '==':
        return treap.find(TreapDict[rela][attr], val)
    if op == '<':
        return treap.rank(TreapDict[rela][attr], val)
    if op == '<=':
        return treap.rank(TreapDict[rela][attr], val+1)
    if op == '>':
        return TreapDict[rela][attr].sub_tree_size - treap.rank(TreapDict[rela][attr], val+1)
    if op == '>=':
        return TreapDict[rela][attr].sub_tree_size - treap.rank(TreapDict[rela][attr], val)
    if op == '!=':
        return TreapDict[rela][attr].sub_tree_size - treap.find(TreapDict[rela][attr], val)

def join_judger(rela1, ans1, ans2, logic, mpAttr, uu1, uu2):
    row_cnt = 0
    if logic == '==' and ans1 == ans2:
            for relaTMP in mpAttr.keys():
                for attr in mpAttr[relaTMP]:
                    if relaTMP == rela1:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu1][attr])
                    else:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu2][attr])
            row_cnt += 1

    if logic == '>=' and ans1 >= ans2:
            for relaTMP in mpAttr.keys():
                for attr in mpAttr[relaTMP]:
                    if relaTMP == rela1:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu1][attr])
                    else:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu2][attr])
            row_cnt += 1

    elif logic == '>' and ans1 > ans2:
            #print("3: ",time.time() - tmp)
            for relaTMP in mpAttr.keys():
                for attr in mpAttr[relaTMP]:
                    if relaTMP == rela1:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu1][attr])
                    else:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu2][attr])
            row_cnt += 1

    elif logic == '<=' and ans1 <= ans2:
            #print("3: ",time.time() - tmp)
            for relaTMP in mpAttr.keys():
                for attr in mpAttr[relaTMP]:
                    if relaTMP == rela1:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu1][attr])
                    else:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu2][attr])
            row_cnt += 1

    elif logic == '<' and ans1 < ans2:
            #print("3: ",time.time() - tmp)
            for relaTMP in mpAttr.keys():
                for attr in mpAttr[relaTMP]:
                    if relaTMP == rela1:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu1][attr])
                    else:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu2][attr])
            row_cnt += 1
    
    elif logic == '!=' and ans1 != ans2:
            #print("3: ",time.time() - tmp)
            for relaTMP in mpAttr.keys():
                for attr in mpAttr[relaTMP]:
                    if relaTMP == rela1:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu1][attr])
                    else:
                        mpAttr[relaTMP][attr].append(baseDBDict[relaTMP][uu2][attr])
            row_cnt += 1
    return row_cnt

def check_sort_faster(n, m):
    base = 2
    return n * math.log(n, base) + m * math.log(m, base) + n + m - 1 < n * m

def join_sort(uu, rela, join_attr):
    return baseDBDict[rela][uu][join_attr]

parsing_consume = 0
storage_consume = 0
def mem_exec(sql):
    global parsing_consume
    global storage_consume    
    op = 999
    # create table 
    # CREATE TABLE TTT(ID INT PRIMARY KEY);
    # TODO? 根据语法树根节点，不采用字符串查询，不搞
    if sql.upper().find("CREATE TABLE") != -1:
        virtual_plan = parser_CreateTable.virtual_plan_create(sql)
        logging.debug("virtual_plan dict is: " + str(virtual_plan.__dict__))
        lst = []
        for pr in virtual_plan.columns:
            lst.append(pr['name'])
            lst.append(pr['type'])
        create_table(virtual_plan.table_name, lst)
        if virtual_plan.primary_key:
            create_primary_cons(virtual_plan.table_name, virtual_plan.primary_key)                       #promise, single attr
            # 只需要注册就行，不需要调用creat_index，因为baseDB没东西
            # 0421，兼容treap，重用create_index
            create_index(virtual_plan.table_name, virtual_plan.primary_key)
            #BTreeDict.setdefault(virtual_plan.table_name, {})
            #BTreeDict[virtual_plan.table_name].setdefault(virtual_plan.primary_key, OOBTree())
        if virtual_plan.foreign_key:
            create_foreign_cons(
            virtual_plan.table_name, virtual_plan.foreign_key["local_columns"][0], 
            virtual_plan.foreign_key["table"], virtual_plan.foreign_key["foreign_columns"][0]
            )        #promise, single attr
        
    elif sql.upper().find("DROP TABLE") != -1:
        virtual_plan = parser_DropTable.virtual_plan_create(sql)
        #print(virtual_plan.__dict__)
        drop_table(virtual_plan.table_name)

    elif sql.upper().find("INSERT INTO") != -1:
        tmp = time.time()
        virtual_plan = parser_Insert.virtual_plan_create(sql)
        parsing_consume += time.time()-tmp

        logging.debug(virtual_plan.__dict__)    # {'table_name': 'students', 'columnsKey': ['id', 'name', 'gender', 'age', 'score'], 'columnsValue': [1, 'Alice', 'F', 18, 95], 'asName': 'qq'}
        op_list = []
        if len(virtual_plan.columnsKey) != len(virtual_plan.columnsValue):
            raiseErr("kv len not equal")
        for i in range(len(virtual_plan.columnsKey)):
            #op_list.append(virtual_plan.columnsKey[i])
            op_list.append(virtual_plan.columnsValue[i])
        tmp2 = time.time()
        write_row(virtual_plan.table_name, op_list)
        storage_consume += time.time()-tmp2
    
    elif sql.upper().find("DELETE FROM") != -1:
        virtual_plan = parser_Delete.virtual_plan_create(sql)
        logging.debug(virtual_plan.__dict__)
        erase_row(virtual_plan)
 
    elif sql.upper().find("UPDATE") != -1:
        virtual_plan = parser_Update.virtual_plan_create(sql)
        logging.debug(virtual_plan.__dict__)
        update_row(virtual_plan)

    #forjoin select *, id2salary.id, name from ptr;
    # select *, name from ptr;
    elif sql.upper().find("SELECT") != -1:
        ret_list = []
        ret_list_dict = []   #TODO 兼容join，消除上面那个？新表每行替换成字典的版本(列名带上ptr.)，不维护顺序，维护列对应
        # 不支持重复列了吧，麻烦死了，重复列
        virtual_plan = parser_Select.virtual_plan_create(sql)
        logging.debug("virtual plan done, it's like: " + str(virtual_plan.__dict__))

        # 重复列的冗余优化？考虑删了
        # 登记待填列
        mpAttr = {}
        for tu in virtual_plan.queryAttr:
            rela = tu[0]
            if rela == 'special_WHERE':
                rela = virtual_plan.table_name
            if rela not in mpAttr.keys():
                mpAttr.setdefault(rela,{})
            if tu[1] == '*':
                for attr in metaDict[rela].keys():
                    if attr not in mpAttr[rela].keys():
                        mpAttr[rela].setdefault(attr, [])
            else:
                if tu[1] not in mpAttr[rela].keys():
                    mpAttr[rela].setdefault(tu[1], [])
        logging.debug("mpAttr is like:" + str(mpAttr))
        
        # Start linear scanning...
        if virtual_plan.where_expr != None:
            brkRelaNameIndex1 = 0
            brkRelaNameIndex2 = 0
            brkAttrNameIndex1 = 0
            brkAttrNameIndex2 = 0

            for ch in virtual_plan.where_expr1_eval:
                if not ch.isalnum() and ch=='.':
                    brkRelaNameIndex1 = brkAttrNameIndex1
                if not ch.isalnum() and ch!='.' and ch!='_':
                    break
                brkAttrNameIndex1 += 1
            if virtual_plan.where_expr2_eval != None:
                for ch in virtual_plan.where_expr2_eval:
                    if not ch.isalnum() and ch=='.':
                        brkRelaNameIndex2 = brkAttrNameIndex2
                    if not ch.isalnum() and ch!='.' and ch!='_':
                        break
                    brkAttrNameIndex2 += 1

            where_expr1_rela = virtual_plan.where_expr1_eval[:brkRelaNameIndex1]
            where_expr1_attr = virtual_plan.where_expr1_eval[brkRelaNameIndex1+1:brkAttrNameIndex1]
            logging.debug("First where expr rela&attr is "+str(where_expr1_rela)+"&"+str(where_expr1_attr))
            if virtual_plan.where_expr2_eval != None:
                where_expr2_rela = virtual_plan.where_expr2_eval[:brkRelaNameIndex2]
                where_expr2_attr = virtual_plan.where_expr2_eval[brkRelaNameIndex2+1:brkAttrNameIndex2]
                logging.debug("Second where expr rela&attr is "+str(where_expr2_rela)+"&"+str(where_expr2_attr))
            #ans = baseDBDict[rela][row_uu][where_expr1_attr]


        # optimizer: condition
        swap_expr_flag = False
        if virtual_plan.where_expr != None and virtual_plan.where_expr2_eval != None:
            if satisfy_condition_optimizer(where_expr1_rela, where_expr1_attr, where_expr2_rela, where_expr2_attr):
                row_cnt_satisfy_expr1 = -1
                row_cnt_satisfy_expr2 = -1
                
                op1 = virtual_plan.where_expr1_eval[brkAttrNameIndex1]
                if virtual_plan.where_expr1_eval[brkAttrNameIndex1+1] == '=':
                    op1 += '='
                    val1 = int(virtual_plan.where_expr1_eval[brkAttrNameIndex1+2:])    
                else:
                    val1 = int(virtual_plan.where_expr1_eval[brkAttrNameIndex1+1:])

                op2 = virtual_plan.where_expr2_eval[brkAttrNameIndex2]
                if virtual_plan.where_expr2_eval[brkAttrNameIndex2+1] == '=':
                    op2 += '='
                    val2 = int(virtual_plan.where_expr2_eval[brkAttrNameIndex2+2:])
                else:
                    val2 = int(virtual_plan.where_expr2_eval[brkAttrNameIndex2+1:])

                #print(op1, op2, val1, val2)
                row_cnt_satisfy_expr1 = int(calc_rows_by_treap(where_expr1_rela, where_expr1_attr, op1, val1))
                row_cnt_satisfy_expr2 = int(calc_rows_by_treap(where_expr2_rela, where_expr2_attr, op2, val2))
                logging.debug("sats 1: "+str(row_cnt_satisfy_expr1))
                logging.debug("sats 2: "+str(row_cnt_satisfy_expr2))
                if row_cnt_satisfy_expr1 == -1 or row_cnt_satisfy_expr2 == -1:
                    pass
                elif row_cnt_satisfy_expr1 < row_cnt_satisfy_expr2:
                    if virtual_plan.where_logic == 'or':
                        swap_expr_flag = True
                elif row_cnt_satisfy_expr1 > row_cnt_satisfy_expr2:
                    if virtual_plan.where_logic == 'and':
                        swap_expr_flag = True

        if swap_expr_flag:
            virtual_plan.where_expr1_eval, virtual_plan.where_expr2_eval = virtual_plan.where_expr2_eval, virtual_plan.where_expr1_eval
            brkRelaNameIndex1, brkRelaNameIndex2 = brkRelaNameIndex2, brkRelaNameIndex1
            brkAttrNameIndex1, brkAttrNameIndex2 = brkAttrNameIndex2, brkAttrNameIndex1 
            where_expr1_rela, where_expr2_rela = where_expr2_rela, where_expr1_rela
            where_expr1_attr, where_expr2_attr = where_expr2_attr, where_expr1_attr
            # op: won't use

        row_cnt = 0
        rela_list = list(mpAttr.keys())
        rela = rela_list[0]#for rela in mpAttr.keys():  # 就一个或者两个吧？？
        if virtual_plan.join_expr_eval == None: #tested
            indexEqualFlag = False
            if virtual_plan.where_expr != None and virtual_plan.where_expr2_eval == None and virtual_plan.where_expr1_eval.index('=='):
                pos = virtual_plan.where_expr1_eval.index('==')
                __k = virtual_plan.where_expr1_eval[:pos]
                #print(__k)
                __k = __k[__k.index('.')+1:]
                if rela in BTreeDict.keys() and __k in BTreeDict[rela].keys():
                    indexEqualFlag = True
            # index diff
            if indexEqualFlag:
                k = virtual_plan.where_expr1_eval[:pos]
                k = k[k.index('.')+1:]
                v = eval(virtual_plan.where_expr1_eval[pos+2:])
                uu_set = BTreeDict[rela][k][v]

                for uu in uu_set:
                    row_cnt += 1
                    for attr in mpAttr[rela]:
                        mpAttr[rela][attr].append(baseDBDict[rela][uu][attr])
                #print(mpAttr)
            else:
                for row_uu in baseDBDict[rela]: #线性扫描，TODO 索引直接拉where后的数据       
                    if virtual_plan.where_expr == None:
                        for attr in mpAttr[rela]:
                        #if attr in baseDBDict[rela][row_uu].keys(): 别判断，让他自动炸了，外面有捕获
                            mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                        row_cnt += 1
                    elif virtual_plan.where_expr2_eval == None:   # use where filter rows
                        #if rela == where_expr1_rela: 不用了吧，join分类后
                        ans = baseDBDict[where_expr1_rela][row_uu][where_expr1_attr]
                        tmpEvalS = virtual_plan.where_expr1_eval.replace(virtual_plan.where_expr1_eval[:brkAttrNameIndex1], str(ans))
                        #print(tmpEvalS)
                        if eval(tmpEvalS):
                            for attr in mpAttr[rela]:
                                mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                            row_cnt += 1
                    else:   # double expr && no join
                        ans1_where_no_join = baseDBDict[where_expr1_rela][row_uu][where_expr1_attr]
                        if metaDict[where_expr1_rela][where_expr1_attr] == 'str':
                            ans1_where_no_join = '\''+ans1_where_no_join+'\''
                        tmpEvalS1 = virtual_plan.where_expr1_eval.replace(virtual_plan.where_expr1_eval[:brkAttrNameIndex1], str(ans1_where_no_join))
                        if eval(tmpEvalS1):
                            if virtual_plan.where_logic == 'or':
                                for attr in mpAttr[rela]:
                                    mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                                row_cnt += 1
                            else:   # and
                                ans2_where_no_join = baseDBDict[where_expr2_rela][row_uu][where_expr2_attr]
                                if metaDict[where_expr2_rela][where_expr2_attr] == 'str':
                                    ans2_where_no_join = '\''+ans2_where_no_join+'\''
                                tmpEvalS2 = virtual_plan.where_expr2_eval.replace(virtual_plan.where_expr2_eval[:brkAttrNameIndex2], str(ans2_where_no_join))
                                if eval(tmpEvalS2):
                                    for attr in mpAttr[rela]:
                                        mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                                    row_cnt += 1
                        else:   # tmpEvalS1 is False
                            if virtual_plan.where_logic == 'or':
                                ans2_where_no_join = baseDBDict[where_expr2_rela][row_uu][where_expr2_attr]
                                if metaDict[where_expr2_rela][where_expr2_attr] == 'str':
                                    ans2_where_no_join = '\''+ans2_where_no_join+'\''
                                tmpEvalS2 = virtual_plan.where_expr2_eval.replace(virtual_plan.where_expr2_eval[:brkAttrNameIndex2], str(ans2_where_no_join))
                                if eval(tmpEvalS2):
                                    for attr in mpAttr[rela]:
                                        mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                                    row_cnt += 1
                        #logging.debug("ok double where without join: "+str(tmpEvalS1)+" "+str(tmpEvalS2))
                        #if eval(str(eval(tmpEvalS1))+" "+str(virtual_plan.where_logic)+" "+str(eval(tmpEvalS2))):
                        #if eval(tmpEvalS1+" "+str(virtual_plan.where_logic)+" "+tmpEvalS2):
                        #    for attr in mpAttr[rela]:
                        #        mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                        #select * from rela_i_i_100000 where rela_i_i_100000.key=1 AND rela_i_i_100000.val=1;
        else:   #join, 叉积新表
            # 1. test row_uu and row_uu2 for where
            # 2. test join_expr
            rela1 = rela_list[0]
            rela2 = rela_list[1]
            rela_tmp_list = [rela1, rela2]

            join_uu_list1 = []
            join_uu_list2 = []
            row_cnt1 = 0
            row_cnt2 = 0
            logging.debug("join epxr " + str(virtual_plan.join_expr_eval))

            for rela_now in rela_tmp_list:
                for row_uu in baseDBDict[rela_now]:
                    if virtual_plan.where_expr == None:
                        if rela_now == rela1: 
                            join_uu_list1.append(row_uu)
                            row_cnt1 += 1
                        else:
                            join_uu_list2.append(row_uu)
                            row_cnt2 += 1
                    elif virtual_plan.where_expr2_eval == None:   # use where filter rows
                        if rela_now != where_expr1_rela:
                            if rela_now == rela1: 
                                join_uu_list1.append(row_uu)
                                row_cnt1 += 1
                            else:
                                join_uu_list2.append(row_uu)
                                row_cnt2 += 1
                        else:
                            ans = baseDBDict[where_expr1_rela][row_uu][where_expr1_attr]
                            tmpEvalS = virtual_plan.where_expr1_eval.replace(virtual_plan.where_expr1_eval[:brkAttrNameIndex1], str(ans))
                            #print("tmpEvalS at join one where", tmpEvalS)
                            if eval(tmpEvalS):
                                if rela_now == rela1: 
                                    join_uu_list1.append(row_uu)
                                    row_cnt1 += 1
                                else:
                                    join_uu_list2.append(row_uu)
                                    row_cnt2 += 1
                    else:   # double expr
                        ans1_where_no_join = "True"
                        ans2_where_no_join = "True"
                        if rela_now == where_expr1_rela:
                            ans1_where_no_join = baseDBDict[where_expr1_rela][row_uu][where_expr1_attr]
                        if rela_now == where_expr2_rela:
                            ans2_where_no_join = baseDBDict[where_expr2_rela][row_uu][where_expr2_attr]
                        if ans1_where_no_join != "True":
                            tmpEvalS1 = virtual_plan.where_expr1_eval.replace(virtual_plan.where_expr1_eval[:brkAttrNameIndex1], str(ans1_where_no_join))
                        else:
                            tmpEvalS1 = "True"
                        if ans2_where_no_join != "True":
                            tmpEvalS2 = virtual_plan.where_expr2_eval.replace(virtual_plan.where_expr2_eval[:brkAttrNameIndex2], str(ans2_where_no_join))
                        else:
                            tmpEvalS2 = "True"
                        logging.debug("ok double where without join: "+str(tmpEvalS1)+" "+str(tmpEvalS2))
                        if eval(tmpEvalS1+" "+str(virtual_plan.where_logic)+" "+tmpEvalS2):
                            if rela_now == rela1: 
                                join_uu_list1.append(row_uu)
                                row_cnt1 += 1
                            else:
                                join_uu_list2.append(row_uu)
                                row_cnt2 += 1
            
            #logging.debug("join_uu_list1: " + str(join_uu_list1))
            #logging.debug("join_uu_list2: " + str(join_uu_list2))
            # last, filter pairs
            join_expr = virtual_plan.join_expr_eval
            match = re.search("(<=|>=|==|!=|<|>)", join_expr)
            logic = match.group()
            left, right = join_expr.split(logic)
            join_rela1 = left[:left.index('.')]
            join_attr1 = left[left.index('.')+1:]
            #join_rela2 = right[:right.index('.')]
            join_attr2 = right[right.index('.')+1:]

            join_swap_flag = True
            if join_rela1 == rela1:
                join_swap_flag = False
            
            if logic == '==' and joinOptimizerFlag and check_sort_faster(len(join_uu_list1), len(join_uu_list2)): # sort it
                if not join_swap_flag:
                    sorted_uu_list1 = sorted(join_uu_list1, key = lambda x:join_sort(x, rela1, join_attr1))
                    sorted_uu_list2 = sorted(join_uu_list2, key = lambda x:join_sort(x, rela2, join_attr2))
                else:
                    sorted_uu_list1 = sorted(join_uu_list1, key = lambda x:join_sort(x, rela1, join_attr2))
                    sorted_uu_list2 = sorted(join_uu_list2, key = lambda x:join_sort(x, rela2, join_attr1))
                
                if min(len(sorted_uu_list1), len(sorted_uu_list2)) == 0:
                    pass
                else:
                    ptr1 = 0
                    len1 = len(sorted_uu_list1)
                    ptr2 = 0
                    len2 = len(sorted_uu_list2)
 
                    with tqdm(total=min(len1, len2)) as pbar:
                        while ptr1<len1 and ptr2<len2:
                            ans1 = baseDBDict[rela1][sorted_uu_list1[ptr1]][join_attr1]
                            ans2 = baseDBDict[rela2][sorted_uu_list2[ptr2]][join_attr2]

                            if ans1 < ans2:
                                ptr1 += 1
                                continue
                            if ans1 > ans2:
                                ptr2 += 1
                                continue
                            if ans1 == ans2:
                                step1 = 0
                                for tmp in range(0, len1-ptr1):
                                    if baseDBDict[rela1][sorted_uu_list1[ptr1 + tmp]][join_attr1] == ans1:
                                        step1 += 1
                                    else:
                                        break
                                step2 = 0
                                for tmp in range(0, len2-ptr2):
                                    if baseDBDict[rela2][sorted_uu_list2[ptr2 + tmp]][join_attr2] == ans2:
                                        step2 += 1
                                    else:
                                        break
                                
                                for pp1 in range(ptr1, ptr1+step1):
                                    for pp2 in range(ptr2, ptr2+step2):
                                        row_cnt += join_judger(rela1, ans1, ans2, logic, mpAttr, sorted_uu_list1[pp1], sorted_uu_list2[pp2])
                                
                                ptr1 = ptr1+step1
                                ptr2 = ptr2+step2
                            pbar.update(1)
            # join_judger's rela1 is for telling uu1 is connected to who, rela2 is got in mpAttr, just rela1

            else:
                if not join_swap_flag:
                    for uu1 in tqdm(join_uu_list1):
                        for uu2 in tqdm(join_uu_list2, leave=False):
                            # TODO 挪出来变成逻辑分支可以更快
                            ans1 = baseDBDict[rela1][uu1][join_attr1]
                            ans2 = baseDBDict[rela2][uu2][join_attr2]                                
                            row_cnt += join_judger(rela1, ans1, ans2, logic, mpAttr, uu1, uu2)
                else:
                    for uu1 in tqdm(join_uu_list1):
                        for uu2 in tqdm(join_uu_list2, leave=False):
                            ans1 = baseDBDict[rela2][uu2][join_attr1]
                            ans2 = baseDBDict[rela1][uu1][join_attr2]
                            row_cnt += join_judger(rela1, ans1, ans2, logic, mpAttr, uu1, uu2)

        #logging.debug("After filling, mpAttr is like: " + str(mpAttr))

        #print(row_cnt)

        # aggr_func = virtual_plan.having_expr1_eval[:virtual_plan.having_expr1_eval.index('(')] #MIN

        # Fill in ret, zip, one table meant to be aligned, no join
        # ret_list_dict = []

        for iter in tqdm(range(row_cnt)):
            tmpLst = []
            tmpDict = {}
            for tu in virtual_plan.queryAttr:
                rela = tu[0]
                if rela == 'special_WHERE':
                    rela = virtual_plan.table_name
                if tu[1] == '*':
                    for attr in metaDict[rela].keys():
                        now_entry = mpAttr[rela][attr][iter]
                        tmpLst.append(now_entry)
                        com_attr = rela+"."+attr
                        if com_attr not in tmpDict.keys():
                            tmpDict.setdefault(com_attr, now_entry)
                        else:
                            # TODO check
                            logging.warning("Weird path, check please.")
                else:
                    now_entry = mpAttr[rela][tu[1]][iter]
                    tmpLst.append(now_entry)
                    com_attr = rela+"."+tu[1]
                    if com_attr not in tmpDict.keys():
                        tmpDict.setdefault(com_attr, now_entry)
            if not virtual_plan.Aggr:
                ret_list.append(tmpLst)
                tmpDict.clear()
            else:
                ret_list_dict.append(tmpDict)
                tmpLst.clear()
        
        #logging.debug("ret_list_dict"+str(ret_list_dict))

        # Time to print results
        print("By linear scaning, query done, result as follows:")
        colNameForPrint = [] 
        colNameSet = set()  # interesting point, PrettyTable bans duplicate field names

        # 重复列trick，进dict不准备沿用了，太难维护
        # 很快，不用看
        for tu in virtual_plan.queryAttr:
            rela = tu[0]
            if rela == 'special_WHERE':
                rela = virtual_plan.table_name
            if tu[1] == '*':
                for attr in metaDict[rela].keys():
                    col = str(rela)+'.'+str(attr)
                    if tu[2] != "":
                        col = tu[2]+'('+col+')'
                    while col in colNameSet:        # trick
                        logging.debug("making more longer name for PrettyTable")
                        col = col+" "
                    colNameSet.add(col)
                    colNameForPrint.append(col)
            else:
                col = str(rela)+'.'+str(tu[1])
                if tu[2] != "":
                    col = tu[2]+'('+col+')'
                while col in colNameSet:
                    logging.debug("making more longer name for PrettyTable")
                    col = col+" "
                colNameSet.add(col)
                colNameForPrint.append(col)

        # no group by, just print
        if not virtual_plan.Aggr:
            # Basic linear scan, done
            #+--------+----------+---------+-----------+
            #| ptr.id | ptr.name | ptr.id  | ptr.name  |
            #+--------+----------+---------+-----------+
            #|  707   |   Bob    |   707   |    Bob    |
            #|  1037  |  Alice   |   1037  |   Alice   |
            #|  122   |  Alice   |   122   |   Alice   |
            #+--------+----------+---------+-----------+
            table4Print = PrettyTable(colNameForPrint)
            for row in tqdm(ret_list):
                table4Print.add_row(row)
                row.clear()
            if virtual_plan.orderByAttr != None:
                table4Print.sortby = virtual_plan.orderByAttr
                table4Print.reversesort = not virtual_plan.orderByAsc
            if virtual_plan.limit != None:
                #if len(table4Print.rows) > virtual_plan.limit:
                if table4Print.reversesort:
                    print(table4Print[-virtual_plan.limit:])
                else:
                    print(table4Print[:virtual_plan.limit])
            else:
                if len(ret_list) >= 1000000:
                    print("The result is too big, use limit next time, here only return 1000000 rows:")
                    if table4Print.reversesort:
                        print(table4Print[-1000000:])
                    else:
                        print(table4Print[:1000000])
                    #print(table4Print[:1000000])
                else:
                    print(table4Print)
            sys.stdout.flush() # force flushing the output buffer
        # Time to do group by && having
        else:   #这种的，记得列名打全了
            #+--------------+-----------------------+
            #| id2salary.id | MIN(id2salary.salary) |
            #+--------------+-----------------------+
            #|     909      |          2000         |
            #|     707      |          1500         |
            #+--------------+-----------------------+
            logging.info("dict ver: " + str(ret_list_dict))
            grpMP = {}
            #for row_uu in baseDBDict[rela]:                    
            #    for attr in mpAttr[rela]:
                    #if attr in baseDBDict[rela][row_uu].keys(): 别判断，让他自动炸了，外面有捕获
            #        mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
            #    row_cnt += 1    

            for rowMP in ret_list_dict:
                #print("GROUP", row, virtual_plan.group_attr, virtual_plan.having_expr)
                # TODO 矩阵里，哪一列是group_attr？
                # zip时候另缝一个字典版本？一份表当然可以重用mpAttr，但兼容join的话还是让新表重新生成一个字典版本比较好？
                if rowMP[virtual_plan.group_attr] not in grpMP.keys():
                    grpMP.setdefault(rowMP[virtual_plan.group_attr], [])
                grpMP[rowMP[virtual_plan.group_attr]].append(rowMP)
                #grpMP[virtual_plan.group_attr].append()

            #print("grouping done, now grpMP is:", grpMP) 
            # grouping done, now grpMP is: {909: [{'id2salary.id': 909, 'id2salary.salary': 2000}], 707: [{'id2salary.id': 707, 'id2salary.salary': 2000}, {'id2salary.id': 707, 'id2salary.salary': 1500}]}

            # Filter by having
            valid_groupAttr_set = set()

            # TODO NO HAVING

            #having_expr = virtual_plan.having_expr1_eval
            aggr_func = virtual_plan.having_expr1_eval[:virtual_plan.having_expr1_eval.index('(')] #MIN
            if virtual_plan.having_expr2_eval != None:
                aggr_func2 = virtual_plan.having_expr2_eval[:virtual_plan.having_expr2_eval.index('(')] #MIN
        
            target_attr = virtual_plan.having_expr1_eval[virtual_plan.having_expr1_eval.index('(')+1 : virtual_plan.having_expr1_eval.index(')')]
            if virtual_plan.having_expr2_eval != None:
                target_attr2 = virtual_plan.having_expr2_eval[virtual_plan.having_expr2_eval.index('(')+1 : virtual_plan.having_expr2_eval.index(')')]
            
            logging.debug("aggr1"+str(aggr_func)+" "+str(target_attr))
            if virtual_plan.having_expr2_eval != None:
                logging.debug("aggr2"+str(aggr_func2)+" "+str(target_attr2))
                logging.debug("having logic is "+str(virtual_plan.having_logic))

            for aggr_attr_value in grpMP.keys():
                #for rowMP in grpMP[aggr_attr_value]:
                ans = aggr_row_func(grpMP[aggr_attr_value], aggr_func, target_attr)
                tmpEvalS = virtual_plan.having_expr1_eval.replace(virtual_plan.having_expr1_eval[:virtual_plan.having_expr1_eval.index(')')+1], str(ans))
                #print(tmpEvalS)
                if virtual_plan.having_expr2_eval == None:
                    if eval(tmpEvalS):  # 天才？！这是不是就是SQL注入啊？
                        valid_groupAttr_set.add(aggr_attr_value)
                else:
                    ans2 = aggr_row_func(grpMP[aggr_attr_value], aggr_func2, target_attr2)
                    tmpEvalS2 = virtual_plan.having_expr2_eval.replace(virtual_plan.having_expr2_eval[:virtual_plan.having_expr2_eval.index(')')+1], str(ans2))
                    logging.debug("conditions: "+ str(tmpEvalS) +" "+str(virtual_plan.having_logic) +" "+str(tmpEvalS2))
                    
                    # having上不了条件优化了，用的treap
                    if eval(str(eval(tmpEvalS))+" "+virtual_plan.having_logic+" "+str(eval(tmpEvalS2))):
                        valid_groupAttr_set.add(aggr_attr_value)

                #final_list.append(grpMP)
            logging.debug("valid_groupAttr_set is: " + str(valid_groupAttr_set))
            
            table = PrettyTable(colNameForPrint)

            for aggr_attr_value in grpMP.keys():
                if aggr_attr_value in valid_groupAttr_set:
                    tmpList = []
                    for tu in virtual_plan.queryAttr:
                        #print("tu is", tu)
                        if tu[2] == '': #group by
                            tmpList.append(aggr_attr_value)
                        else:   #手动聚合
                            #print("start argc:", grpMP[aggr_attr_value], tu[2], tu[0]+'.'+tu[1])
                            tmpList.append(aggr_row_func(grpMP[aggr_attr_value], tu[2], tu[0]+'.'+tu[1]))
                    table.add_row(tmpList)
            
            if virtual_plan.orderByAttr != None:
                table.sortby = virtual_plan.orderByAttr
                table.reversesort = not virtual_plan.orderByAsc
            if virtual_plan.limit != None:
                #if len(table4Print.rows) > virtual_plan.limit:
                if table.reversesort:
                    print(table[-virtual_plan.limit:])
                else:
                    print(table[:virtual_plan.limit])
            else:
                print(table)    
            sys.stdout.flush()
        #else:   #has aggr
        #    pass

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
        #del_row(cmd[0], cmd[1:])

def dirty_cache_rollback_and_commit():
    metaDict.clear()
    baseDBDict.clear()
    BTreeDict.clear()
    TreapDict.clear()
    constraintDict.clear()
    load_snapshot()
    for query in tmpSQLLog:
        mem_exec(query)
    persist_snapshot()

def lazy_file_del():
    for rela in lazyDropRelationList:
        metaF = getMetaFileName(rela)
        baseF = getBaseDBFileName(rela)
        consF = getConstraintFileName(rela)
        btreDir = "btree/"+rela
        trepDir = "treap/"+rela
        if os.path.exists(metaF):
            os.remove(metaF)
        if os.path.exists(baseF):
            os.remove(baseF)
        if os.path.exists(consF):
            os.remove(consF)
        if os.path.exists(btreDir):
            shutil.rmtree(btreDir)
        if os.path.exists(trepDir):
            shutil.rmtree(trepDir)

def engine():
    load_snapshot()
    sqlCounter = 0
    while True:
        # TODO:
        # ctrl+c之类关机，把tmpSQLLog里的东西写进磁盘，再关机
        # 或者，commit的东西，汇报commit

        # start work
        print("============================================================================================================================================================")
        print("Waiting for your sql, input quit; to exit without cmd+C...")
        print(">",end='')
        sql = read_sql()
        if sql == "quit;":
            print("Bye!")
            exit(0)
        st_time = time.time()
        try:
            mem_exec(sql)
        except Exception as e:  # Capture any exception, raise runnable error like primary/foreign here
            err_logger.error(f"sql runtime error: {type(e)}: {e}")
            with open('log_err_trace/err_trace_'+str(time.strftime('%y%m%d_%H', time.localtime()))+'.log', 'a') as f:
                f.write(traceback.format_exc())
                f.write("\n")
            #print("sql runtime error:", e)
            print("A bad query happened, dont worry, we will clean its impact now.")
            print("Start cleaning dirty cache, rollback to commit previous sql...")
            lazyDropRelationList.clear()
            dirty_cache_rollback_and_commit()
            tmpSQLLog.clear()
            continue # skip regular persist
        sqlCounter += 1
        print("Time consumed:",time.time()-st_time,"seconds.")
        # persist
        if sqlCounter % snapShotInterVal == 0:
            #global baseDBDict, BTreeDict, metaDict, constraintDict
            lazy_file_del()
            lazyDropRelationList.clear()
            persist_snapshot()
            tmpSQLLog.clear()
            print("Periodcally persists done, all previous sql committed!")

def main(canned_query_file):
    if canned_query_file == None:
        engine()
    else:
        try:
            st_time = time.time()
            load_snapshot()
            tot = 0
            with open(canned_query_file, 'r') as f:
                for line in tqdm(f, desc="Processing lines"):
                    tmp = time.time()
                    mem_exec(line)
                    tot += time.time()-tmp
        except Exception as e:
            err_logger.error(f"sql runtime error: {type(e)}: {e}")
            exit(1)
    lazy_file_del()
    persist_snapshot()
    print("Time consumed:",time.time()-st_time,"seconds.")
    print("Successfuly completed, bye!")
    print("exact running consume", tot)
    print("parsing:",parsing_consume)
    print("storage:",storage_consume)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        main(None)