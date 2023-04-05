def demo_dynamic_class():
    # Define class attributes and methods
    class_attrs = {
        'x': 1,
        'y': 2,
        'name': 'MyClass',
        'add': lambda self: self.x + self.y
    }

    # Create the class using the type() function
    MyClass = type('MyClass', (object,), class_attrs)

    # Create an instance of the class and access the attribute
    obj = MyClass()
    print(obj.name)  # Output: "MyClass"
def demo_dict_as_btree():
    BTree = load_BTree()

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

    BTree.setdefault(obj1.mark, obj1)
    BTree.setdefault(obj2.mark, obj2)

    #print(BTree)                {97: <__main__.ins object at 0x100a1a110>, 81: <__main__.ins object at 0x100a1a150>}
    #print(BTree[97].name)       Alice

    print(BTree.keys())
