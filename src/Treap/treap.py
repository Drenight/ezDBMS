import random

class TreapNode:
    def __init__(self, key):
        self.key = key                      # v is indexing attr
        self.priority = random.random()     # r
        self.sub_tree_size = 1              # s sub-tree size
        self.key_frequency = 1              # w frequency of this key
        self.child = [None, None]

    def __lt__(self, other):
        return self.priority<other.priority

    def cmp(self, x):
        if x == self.key:
            return -1
        if x<self.key:
            return 0
        else:
            return 1
    
    def cmp_for_kth(self, x):   # decide which way to go deeper
        sz = self.key_frequency
        if self.child[0] != None:
            sz += self.child[0].sub_tree_size
        if sz-self.key_frequency+1<=x and x<=sz:
            return -1
        if x<=sz-self.key_frequency:
            return 0
        return 1 

    def maintain(self):
        self.sub_tree_size = self.key_frequency
        if self.child[0] != None:
            self.sub_tree_size += self.child[0].sub_tree_size
        if self.child[1] != None:
            self.sub_tree_size += self.child[1].sub_tree_size

def rotate(node, d):
    node_tmp = node.child[d^1]
    node.child[d^1] = node_tmp.child[d]
    node_tmp.child[d] = node
    node.maintain()
    node_tmp.maintain()
    node = node_tmp
    return node
    
def insert(node, key):
    if node is None:
        return TreapNode(key)
    else:
        d = node.cmp(key)
        if d == -1:
            node.key_frequency += 1
        else:
            node.child[d] = insert(node.child[d], key)
            if node.child[d] > node:
                node = rotate(node, d^1)
    node.maintain()    
    return node

def delete(node, key):
    d = node.cmp(key)
    if d == -1:
        if node.key_frequency > 1:
            node.key_frequency -= 1
        elif node.child[0]!=None and node.child[1]!=None:
            d2 = 0
            if node.child[0] > node.child[1]:
                d2 = 1
            node = rotate(node, d2)
            node.child[d2] = delete(node.child[d2], key)
        else:
            if node.child[0] != None:
                node = node.child[0]
            else:
                node = node.child[1]
    else:
        node.child[d] = delete(node.child[d], key)
    if node!=None:
        node.maintain()
    return node

def find(node, key):
    if node == None:
        return 0
    d = node.cmp(key)
    if d == -1:
        return node.key_frequency
    return find(node.child[d], key)

def kth(node, k):
    if node == None:
        return 0
    d = node.cmp_for_kth(k)
    sz = node.key_frequency
    if node.child[0] != None:
        sz+=node.child[0].sub_tree_size
    if d == -1:
        return node.key
    if d == 0:
        return kth(node.child[0], k)
    return kth(node.child[1], k-sz)

def rank(node, x):
    if node == None:
        return 0
    d = node.cmp(x)
    sz = node.key_frequency
    if node.child[0] != None:
        sz += node.child[0].sub_tree_size
    if d == -1:
        return sz - node.key_frequency
    elif d == 0:
        return rank(node.child[0], x)
    else:
        return rank(node.child[1], x) + sz

def main1():
    root = None
    n = int(input())
    while n:
        n -= 1
        op, x = map(int, input().split())
        if op == 1:
            root = insert(root, x)
        elif op == 2:
            root = delete(root, x)
        elif op == 3:
            print(rank(root, x)+1)
        elif op == 4:
            print(kth(root, x))
        elif op == 5:
            sz = rank(root, x)
            print(kth(root, sz))
        elif op == 6:
            sz = rank(root, x)
            sz += find(root, x)+1
            print(kth(root, sz)) 

def main():
    root = None
    root = insert(root, 3)
    root = insert(root, 7)
    
    print(rank(root, 1), rank(root, 3), rank(root, 4), rank(root, 6), rank(root, 8))
    print(rank(root, 4)-rank(root, 3))
    print(find(root, 3))
#    0 0 1 1 2
#    1
#    1
    print(root.sub_tree_size)


if __name__ == '__main__':
    main()
