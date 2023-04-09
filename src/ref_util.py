from sqlalchemy import text

# SQL query to parse
query = "SELECT * FROM table WHERE column = 'value'"

# Parse the query and generate AST
ast = text(query).compile(dialect="sqlite").parsed

# Print the AST
print(ast)


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


#global BTree, BTreeLock
#BTree = load_BTree()

#print(list(BTree.keys()))

#th_SnapshotBuilder = threading.Thread(target=snapshotBuilder)
#th_MainEngine = threading.Thread(target=engine)

#th_SnapshotBuilder.start()
#th_MainEngine.start()

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

# 创建一个空的 B-Tree，并将节点类型指定为 set 类型
btree = OOBTree()
btree[2] = set()

# 在 B-Tree 中插入元素

# 查找某个键对应的节点
node = btree.get(2)
btree[2].add(3)
btree[2].add(7)

# 输出节点内容
print(node)  # 输出 {4, 5, 6}