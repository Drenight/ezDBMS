from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class CreateIndexPlan:
    def __init__(self):
        self.table_name = None
        self.index_attr = None # TODO 多键？

class CreateIndexListener(SQLiteParserListener):
    def __init__(self):
        self.plan = CreateIndexPlan()

    def enterCreate_index_stmt(self, ctx: SQLiteParser.Create_index_stmtContext):
        self.plan.table_name = ctx.table_name
        return super().enterCreate_index_stmt(ctx)

# 直接复制
def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = CreateListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

# 自测这个单个模块用的
# CREATE TABLE TTT(ID INT PRIMARY KEY);
def main():
    sql = """
        CREATE INDEX
    """

    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = CreateListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    print(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()