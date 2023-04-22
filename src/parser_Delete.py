from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class DeletePlan:
    def __init__(self):
        self.table_name = None

        self.where_expr = None
        self.where_logic= None
        self.where_expr1_eval = None
        self.where_expr2_eval = None
        
class DeleteListener(SQLiteParserListener):
    def __init__(self):
        self.plan = DeletePlan()

    def enterDelete_stmt(self, ctx:SQLiteParser.Delete_stmtContext):
        self.plan.table_name = ctx.qualified_table_name().getText()
        self.plan.where_expr = ctx.expr().getText()
        logging.debug(ctx.qualified_table_name().getText())

        whereExprCtx = ctx.expr()
        if str(whereExprCtx.AND_()).upper() == 'AND':
            self.plan.where_logic = 'and'
        elif str(whereExprCtx.OR_()).upper() == 'OR':
            self.plan.where_logic = 'or'

        if self.plan.where_logic != None:
            self.plan.where_expr1_eval = self.plan.where_expr[:self.plan.where_expr.index(self.plan.where_logic.upper())]
            self.plan.where_expr2_eval = self.plan.where_expr[self.plan.where_expr.index(self.plan.where_logic.upper())+len(self.plan.where_logic):]
        else:
            self.plan.where_expr1_eval = self.plan.where_expr
            self.plan.where_expr2_eval = None 
        # = -> ==   
        s1 = self.plan.where_expr1_eval
        s2 = self.plan.where_expr2_eval
        #print(s1,s2)
        if '!' not in s1 and '<' not in s1 and '>' not in s1 and '=' in s1:
            self.plan.where_expr1_eval = self.plan.where_expr1_eval.replace('=', '==')
        if s2 != None and '!' not in s2 and '<' not in s2 and '>' not in s2 and '=' in s2:
            self.plan.where_expr2_eval = self.plan.where_expr2_eval.replace('=', '==')
        #print(self.plan.where_expr1_eval)

def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = DeleteListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

def main():
    sql = """
        DELETE FROM customer_name WHERE customer_name.id=7 AND customer_name.customer_name='Bob';
    """
    sql2 = """
        DELETE FROM orders WHERE orders.id=1 AND orders.customer_id=1;
    """
    sql3 = """
        DELETE FROM orders WHERE orders.id=1;
    """

    input_stream = InputStream(sql3)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = DeleteListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    logging.debug(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()