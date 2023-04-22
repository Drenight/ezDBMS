import ctypes

# 加载动态链接库
treap = ctypes.CDLL('src/Treap/treap.so')

# 定义 C++ 函数的参数类型和返回值类型
treap.insert.argtypes = [ctypes.c_void_p, ctypes.c_int]
treap.del_node.argtypes = [ctypes.c_void_p, ctypes.c_int]
treap.query.argtypes = [ctypes.c_void_p, ctypes.c_int]
treap.query.restype = ctypes.c_int
treap.kth.argtypes = [ctypes.c_void_p, ctypes.c_int]
treap.kth.restype = ctypes.c_int

# 创建根节点
root = treap.create()

# 插入数据
treap.insert(root, 1)
treap.insert(root, 2)
treap.insert(root, 3)

# 查询数据
print(treap.query(root, 3))  # 输出 3

# 删除数据
treap.del_node(root, 2)

# 查询数据
print(treap.query(root, 3))  # 输出 2

# 按照尺寸查找第 k 大的元素
print(treap.kth(root, 2))  # 输出 1

# 删除树
treap.remove(root)
