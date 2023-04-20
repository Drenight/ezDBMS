from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class DropPlan:
    def __init__(self):
        self.table_name = None
        
class DropListener(SQLiteParserListener):
    def __init__(self):
        self.plan = DropPlan()

    def enterDrop_stmt(self, ctx: SQLiteParser.Drop_stmtContext):
        self.plan.table_name = ctx.any_name().getText()
        logging.debug(self.plan.table_name)
        return super().enterDrop_stmt(ctx)

def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = DropListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

def main():
    sql = """
        DROP TABLE customer_name;
    """
    sql2 = """
        DROP TABLE orders;
    """
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = DropListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    logging.debug(listener.plan.__dict__)

if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG)
   main()