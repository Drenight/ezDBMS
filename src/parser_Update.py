from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class UpdatePlan:
    def __init__(self):
        self.table_name = None
        self.columnsKey = []
        self.operators = []
        self.columnsValue = []

        
class UpdateListener(SQLiteParserListener):
    def __init__(self):
        self.plan = UpdatePlan()


    def enterUpdate_stmt(self, ctx: SQLiteParser.Update_stmtContext):
        self.plan.table_name = ctx.qualified_table_name().getText()
        logging.debug(ctx.qualified_table_name().getText())
        for k in ctx.column_name():
            logging.debug(k.getText())
            self.plan.columnsKey.append(k.getText())
        for k in ctx.ASSIGN():
            logging.debug(k.getText())
            self.plan.operators.append(k.getText())
        for k in ctx.expr():
            logging.debug(k.getText())
            self.plan.columnsValue.append(k.getText())
        return super().enterUpdate_stmt(ctx)


        # TODO 验证name中插入的数据是否是str，age中插入的数据是否是int
        # TODO Where
    def enterExpr(self, ctx: SQLiteParser.ExprContext):
        
        return super().enterExpr(ctx)
        


def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = UpdateListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

def main():
    sql = """
        UPDATE ptr 
        SET 
        name = 'Alice',
        age = 16
        WHERE ptr.id = 123;
    """
    


    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = UpdateListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    logging.debug(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
