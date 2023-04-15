from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class SelectPlan:
    def __init__(self):
        self.table_name = None
        self.star = None        # whether star showed
        self.attrs = []         # rela -> tuple of attr TODO 特殊键WHERE怎么样？可以合并掉star
        self.asName = None
        self.orderBy = None
        self.limit = None

# select_core:
#    (
#        SELECT_ (DISTINCT_ | ALL_)? result_column (COMMA result_column)* (
#            FROM_ (table_or_subquery (COMMA table_or_subquery)* | join_clause)
#        )? (WHERE_ whereExpr=expr)? (
#          GROUP_ BY_ groupByExpr+=expr (COMMA groupByExpr+=expr)* (
#              HAVING_ havingExpr=expr
#          )?)? (
#            WINDOW_ window_name AS_ window_defn (
#                COMMA window_name AS_ window_defn
#            )*
#        )?
#    )
#    | values_clause
#;

class SelectListener(SQLiteParserListener):
    def __init__(self):
        self.plan = SelectPlan()

    def enterSelect_core(self, ctx: SQLiteParser.Select_coreContext):
        # get attrs
        tmpList = list(ctx.result_column())
        for attr in tmpList:
            attr = attr.getText()
            self.plan.attrs.append(attr)
        logging.debug(self.plan.attrs)

        res_ctx = ctx.result_column()


        return super().enterSelect_core(ctx)    

    def enterExpr(self, ctx: SQLiteParser.ExprContext): # ((schema_name DOT)? table_name DOT)? column_name 语法，特定表列
        return super().enterExpr(ctx)

    def enterResult_column(self, ctx: SQLiteParser.Result_columnContext):       # enterSelect_core只会进一次，我们不嵌套，所以开个新的
        # *进来的
        if ctx.STAR():  # tested
            self.plan.star = True
        
        # table1.* 进来的
        if ctx.table_name() != None: 
            print("table name is") 
            print(ctx.table_name().getText())
            pass
        
        # expr进来的，不带*
        expr_ctx = ctx.expr()
        if expr_ctx != None:
            if expr_ctx.table_name() != None:
                # 捕获给定rela.的列
                print("given rela attrs: ",expr_ctx.table_name().getText())
            elif expr_ctx.column_name() != None:
                # 捕获where的列，min之类聚合的不会被捕获
                print(expr_ctx.column_name().getText())
            elif expr_ctx.function_name()!= None:
                # 聚合列
                # function_name OPEN_PAR ((DISTINCT_? expr ( COMMA expr)*) | STAR)? CLOSE_PAR filter_clause? over_clause?
                if expr_ctx.expr() == []: 
                    # avg(*)这种
                    print(expr_ctx.function_name().getText())
                    print("*")
                else:
                    function_ctx = expr_ctx.expr()[0]
                    print(expr_ctx.function_name().getText())   #avg这种
                    if function_ctx != None:
                        print("fin:",function_ctx.column_name().getText())

        return super().enterResult_column(ctx)

    def enterSelect_stmt(self, ctx: SQLiteParser.Select_stmtContext):
        return super().enterSelect_stmt(ctx)

    def enterOrder_by_stmt(self, ctx: SQLiteParser.Order_by_stmtContext):
        return super().enterOrder_by_stmt(ctx)




def virtual_plan_create(sql):
    pass

def main():
    sql = """
        SELECT ptr.*, ptr.id, ptr.name, name, min(id), max(name), avg(*) FROM ptr WHERE name = "Alice" and id = 1037 ORDER BY id LIMIT 10;
    """
    sql2 = """
        SELECT * FROM ptr WHERE name = "Alice" and id = 1037 ORDER BY id LIMIT 10;
    """

    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()

    listener = SelectListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    logging.debug(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()