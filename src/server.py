import os
import glob
import csv
import sys
import dill
import uuid
import time
import logging
import pickle
# my parsers
import parser_CreateTable
import parser_Insert
import parser_Select
from prettytable import PrettyTable
from BTrees.OOBTree import OOBTree

############################### Core Mem Data Structure & magic number config #############
baseDBDict = {}                         # [relation][uuid:173]  -> {id:7, salary:1000}
BTreeDict = {}                          # [relation][attribute] -> BTree, each key map to a set, set stores uuids, uuid points to the row
metaDict = {}                           # [relation]            -> {id:int, name:str}
constraintDict = {}                     # [relation]            -> {primary: attr, foreign:{attr:id, rela2:, rela_attr:}}

snapShotInterVal = 2
tmpSQLLog = []

conditionOptimizerFlag = False

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

# TODO 主键外键约束，底层模块做
def write_row(relationName, cmd):
    if relationName not in baseDBDict.keys():
        #load_BaseDB(relationName)
        # TODO 不确定
        baseDBDict.setdefault(relationName, {})

    mp = {}
    
    meta_dict = metaDict[relationName]
    logging.debug(meta_dict)

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
                pk = constraintDict[relationName]['primary']
                lst = query_equal(relationName,[pk, mp[pk],])
                if len(lst) != 0:
                    raise PrimaryKeyError('Error: Primary key duplicate' + str(mp[pk]))
        # check foreign key
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

class aggrAffi:
    minFlag = False
    nowMin = None

def aggr_row_func(rowList, aggr_func, target_attr):  # row is a list of {'id2salary.id': 909, 'id2salary.salary': 2000}
    print(rowList)
    ret = rowList[0][target_attr]
    for row in rowList:
        if aggr_func.upper() == "MIN":
            ret = min(ret, row[target_attr])
    
    return ret

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

    elif sql.upper().find("INSERT INTO") != -1:
        virtual_plan = parser_Insert.virtual_plan_create(sql)
        logging.debug(virtual_plan.__dict__)    # {'table_name': 'students', 'columnsKey': ['id', 'name', 'gender', 'age', 'score'], 'columnsValue': [1, 'Alice', 'F', 18, 95], 'asName': 'qq'}
        op_list = []
        if len(virtual_plan.columnsKey) != len(virtual_plan.columnsValue):
            raiseErr("kv len not equal")
        for i in range(len(virtual_plan.columnsKey)):
            #op_list.append(virtual_plan.columnsKey[i])
            op_list.append(virtual_plan.columnsValue[i])
        write_row(virtual_plan.table_name, op_list)
    
    #forjoin select *, id2salary.id, name from ptr;
    # select *, name from ptr;
    elif sql.upper().find("SELECT") != -1:
        ret_list = []
        ret_list_dict = []   #TODO 兼容join，消除上面那个？新表每行替换成字典的版本(列名带上ptr.)，不维护顺序，维护列对应
        # 不支持重复列了吧，麻烦死了，重复列
        virtual_plan = parser_Select.virtual_plan_create(sql)
        logging.debug("virtual plan done, it's like: " + str(virtual_plan.__dict__))
        if not virtual_plan.Join:
            if not virtual_plan.Where :
                # 重复列的冗余优化？考虑删了
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
                row_cnt = 0
                for rela in mpAttr.keys():
                    for row_uu in baseDBDict[rela]:                    
                        for attr in mpAttr[rela]:
                            #if attr in baseDBDict[rela][row_uu].keys(): 别判断，让他自动炸了，外面有捕获
                            mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                        row_cnt += 1    

                # Fill in ret, zip, one table meant to be aligned, no join
                # ret_list_dict = []
                for iter in range(row_cnt):
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
                    ret_list.append(tmpLst)
                    ret_list_dict.append(tmpDict)
                
                logging.debug("ret_list_dict"+str(ret_list_dict))

                # Time to print results
                print("By linear scaning, query done, result as follows:")
                colNameForPrint = []
                colNameSet = set()  # interesting point, PrettyTable bans duplicate field names

                # 重复列trick，进dict不准备沿用了，太难维护
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
                    for row in ret_list:
                        table4Print.add_row(row)
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
                    for row_uu in baseDBDict[rela]:                    
                        for attr in mpAttr[rela]:
                            #if attr in baseDBDict[rela][row_uu].keys(): 别判断，让他自动炸了，外面有捕获
                            mpAttr[rela][attr].append(baseDBDict[rela][row_uu][attr])
                        row_cnt += 1    

                    for rowMP in ret_list_dict:
                        #print("GROUP", row, virtual_plan.group_attr, virtual_plan.having_expr)
                        # TODO 矩阵里，哪一列是group_attr？
                        # zip时候另缝一个字典版本？一份表当然可以重用mpAttr，但兼容join的话还是让新表重新生成一个字典版本比较好？
                        if rowMP[virtual_plan.group_attr] not in grpMP.keys():
                            grpMP.setdefault(rowMP[virtual_plan.group_attr], [])
                        grpMP[rowMP[virtual_plan.group_attr]].append(rowMP)
                        #grpMP[virtual_plan.group_attr].append()

                    print("grouping done, now grpMP is:", grpMP) 
                    # grouping done, now grpMP is: {909: [{'id2salary.id': 909, 'id2salary.salary': 2000}], 707: [{'id2salary.id': 707, 'id2salary.salary': 2000}, {'id2salary.id': 707, 'id2salary.salary': 1500}]}

                    # Filter by having
                    valid_groupAttr_set = set()
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
                        print(tmpEvalS)
                        if virtual_plan.having_expr2_eval == None:
                            if eval(tmpEvalS):  # 天才？！这是不是就是SQL注入啊？
                                valid_groupAttr_set.add(aggr_attr_value)
                        else:
                            ans2 = aggr_row_func(grpMP[aggr_attr_value], aggr_func2, target_attr2)
                            tmpEvalS2 = virtual_plan.having_expr2_eval.replace(virtual_plan.having_expr2_eval[:virtual_plan.having_expr2_eval.index(')')+1], str(ans2))
                            logging.debug("conditions: "+ str(tmpEvalS) + str(tmpEvalS2))
                            if not conditionOptimizerFlag:
                                if eval(str(eval(tmpEvalS))+" "+virtual_plan.having_logic+" "+str(eval(tmpEvalS2))):
                                    valid_groupAttr_set.add(aggr_attr_value)
                            else:   # 条件优化
                                pass

                        #final_list.append(grpMP)
                    logging.debug("valid_groupAttr_set is: " + str(valid_groupAttr_set))
                    
                    table = PrettyTable(colNameForPrint)

                    for aggr_attr_value in grpMP.keys():
                        print("看看", grpMP[aggr_attr_value])
                        if aggr_attr_value in valid_groupAttr_set:
                            tmpList = []
                            for tu in virtual_plan.queryAttr:
                                print("tu is", tu)
                                if tu[2] == '': #group by
                                    tmpList.append(aggr_attr_value)
                                else:   #手动聚合
                                    print("start argc:", grpMP[aggr_attr_value], tu[2], tu[0]+'.'+tu[1])
                                    tmpList.append(aggr_row_func(grpMP[aggr_attr_value], tu[2], tu[0]+'.'+tu[1]))
                            table.add_row(tmpList)
                   
                    print(table)        
                    sys.stdout.flush()
                #else:   #has aggr
                #    pass
            else:   # has where, might utlize indexing
                pass
        else:   # has join
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
        print("==============================================================================")
        print("Waiting for your sql, input quit to exit without cmd+C...")
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
            #print("sql runtime error:", e)
            print("Start cleaning dirty cache, rollback to commit previous sql...")
            dirty_cache_rollback_and_commit()
            tmpSQLLog.clear()
            continue # skip regular persist
        sqlCounter += 1
        print("Time consumed:",time.time()-st_time,"seconds.")
        # persist
        if sqlCounter % snapShotInterVal == 0:
            #global baseDBDict, BTreeDict, metaDict, constraintDict
            persist_snapshot()
            tmpSQLLog.clear()
            print("Periodcally persists done, all previous sql committed!")

def main():
    engine()

main()