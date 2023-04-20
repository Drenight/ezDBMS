from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class UpdatePlan:
    def __init__(self):
        self.table_name = None
        self.columnsKey = []
        self.columnsValue = []
        self.columnMp = {}

        self.where_expr = None
        self.where_logic= None
        self.where_expr1_eval = None
        self.where_expr2_eval = None

        
class UpdateListener(SQLiteParserListener):
    def __init__(self):
        self.plan = UpdatePlan()

    def enterUpdate_stmt(self, ctx: SQLiteParser.Update_stmtContext):
        self.plan.table_name = ctx.qualified_table_name().getText()
        logging.debug("tablename: "+ctx.qualified_table_name().getText())

        #print(self.plan.table_name)

        for k in ctx.column_name():
            logging.debug("ctx_colname: " + k.getText())
            self.plan.columnsKey.append(k.getText())
        for k in ctx.expr():
            logging.debug("ctx_expr: "+k.getText())
            self.plan.columnsValue.append(k.getText())

        if ctx.WHERE_!=None:
            whereExprCtx = ctx.expr()[len(ctx.expr())-1]
            if str(whereExprCtx.AND_()).upper() == 'AND':
                self.plan.where_logic = 'and'
            elif str(whereExprCtx.OR_()).upper() == 'OR':
                self.plan.where_logic = 'or'

            #print(self.plan.where_logic)
            whereExpr = whereExprCtx.getText()
            self.plan.where_expr = whereExpr
            self.plan.columnsValue = self.plan.columnsValue[:-1]

            #tuple_obj = eval(self.plan.columnsValue)
            eval_list = []
            for v in self.plan.columnsValue:
                eval_list.append(eval(v))
            self.plan.columnsValue = eval_list

            for i in range(len(self.plan.columnsKey)):
                self.plan.columnMp[self.plan.columnsKey[i]] = self.plan.columnsValue[i]

            #print(whereExprCtx.getText())

            if self.plan.where_logic != None:
                self.plan.where_expr1_eval = whereExpr[:whereExpr.index(self.plan.where_logic.upper())]
                self.plan.where_expr2_eval = whereExpr[whereExpr.index(self.plan.where_logic.upper())+len(self.plan.where_logic):]
            else:
                self.plan.where_expr1_eval = whereExpr
                self.plan.where_expr2_eval = None 
            
            # !!!special for update, cause lacking rela name:
            #print(self.plan.table_name, "and", self.plan.where_expr1_eval)
            self.plan.where_expr1_eval = self.plan.table_name+"."+self.plan.where_expr1_eval
            if self.plan.where_expr2_eval != None:
                self.plan.where_expr2_eval = self.plan.table_name+"."+self.plan.where_expr2_eval

            # = -> ==   
            s1 = self.plan.where_expr1_eval
            s2 = self.plan.where_expr2_eval
            #print(s1,s2)
            if '!' not in s1 and '<' not in s1 and '>' not in s1 and '=' in s1:
                self.plan.where_expr1_eval = self.plan.where_expr1_eval.replace('=', '==')
            if s2 != None and '!' not in s2 and '<' not in s2 and '>' not in s2 and '=' in s2:
                self.plan.where_expr2_eval = self.plan.where_expr2_eval.replace('=', '==')
            #print(self.plan.where_expr1_eval)
        
        return super().enterUpdate_stmt(ctx)

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
        SET name = 'Alice', id = 9903
        WHERE id = 122 AND name = 'Alice';
    """
    sql2 = """
        UPDATE ptr
        SET name = 'Alice', id = 9903
        WHERE id = 122;
    """
    sql3 = """
        UPDATE orders SET customer_id = 7 WHERE id = 1;
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
