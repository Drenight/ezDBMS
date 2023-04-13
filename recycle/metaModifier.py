import os

metaPrefix = "meta/"
fileExists = -1
# MOON (name, str) (mark, int)

#CREATE TABLE table_name (
#    column1 datatype,
#    column2 datatype,
#    column3 datatype,
#   ....
#);

def getDir(relationName):
    return metaPrefix + relationName + ".meta"

def create_table(relationName, attributes):
    #print(relationName, attributes)
    if os.path.exists(getDir(relationName)):
        return "Relation already exists!"
    with open(getDir(relationName), "w") as f:
        for attribute in attributes:
            print(attribute)
            name, datatype = attribute
            f.write(f"{name} {datatype}\n")
        return 0

def drop_table(relationName):
    if not os.path.exists(getDir(relationName)):
        return "No such relation!"
    os.remove(getDir(relationName))
