from ntpath import join
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

# expr疑似都可以处理一下，直接调用eval？
class SelectListener(SQLiteParserListener):
    def __init__(self):
        self.plan = SelectPlan()

    def enterSelect_core(self, ctx: SQLiteParser.Select_coreContext):
        # get general attrs
        tmpList = list(ctx.result_column())
        for attr in tmpList:
            attr = attr.getText()
            self.plan.attrs.append(attr)
        logging.debug(self.plan.attrs)

        # get from's relation
        if ctx.table_or_subquery() != []:
            fromCtx = ctx.table_or_subquery()[0]        # 单表，其他都写在join
            tableName = fromCtx.table_name().getText()
            print("from table is:", tableName)

        # get join, if join table_or_subquery==[]
        joinCtx = ctx.join_clause()
        if joinCtx != None:
            for tar in joinCtx.table_or_subquery():
                print("ww",tar.getText())

            #Antlr-Python疑似有问题，INNER这种会被解析到rela1上来，这里手动解开
            bad_set = set()
            lst = ['INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'NATURAL']
            for tu in lst:
                bad_set.add(tu)
                bad_set.add(tu.lower())
            #print(bad_set)
            rela1 = joinCtx.table_or_subquery()[0].getText()
            rela2 = joinCtx.table_or_subquery()[1].getText()
            joinOP = ""
            for join_str in bad_set:
                if join_str in rela1:
                    joinOP = join_str
                    rela1 = rela1.split(join_str)[0]
                    print(rela1)
                    break
            print("rela1 is gonna join rela2:", rela1, rela2)
            print("JOIN OP is", joinOP)
            #joinOP = joinCtx.join_operator()[0]
            #print(len(joinCtx.join_operator()))
            #print("JOIN OP IS", joinOP.getText())

            if joinCtx.join_constraint != None:
                joinCons = joinCtx.join_constraint()[0]
                expr = joinCons.expr().getText()
                print("expr is", expr)


        return super().enterSelect_core(ctx)    

    # HOLD, attrs不是已经拿到了吗，如果只是为了填充虚拟计划，这样解析的意义是什么？  
    # 合法性吗？ 不是，SELECT )*( FROM TABLE A; 就算不写下面的解析也是会报错的
    # 似乎手解if-else也不会比这个简单多少？avg(*),avg(table.col1),avg(col)，这个用框架做逻辑分类，降低心智成本？
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
                if expr_ctx.STAR(): #等效 expr_ctx.expr() == []: 
                    # avg(*)这种
                    print(expr_ctx.function_name().getText())
                    print("fin: *")
                else:
                    function_ctx = expr_ctx.expr()[0]
                    print(expr_ctx.function_name().getText())   #avg这种
                    if function_ctx != None:
                        print("fin:",function_ctx.column_name().getText())
            else:   # 全None，是表达式
                print("REA==================")
                print(expr_ctx.PLUS())

        return super().enterResult_column(ctx)

    def enterSelect_stmt(self, ctx: SQLiteParser.Select_stmtContext):
        return super().enterSelect_stmt(ctx)

    def enterOrder_by_stmt(self, ctx: SQLiteParser.Order_by_stmtContext):
        return super().enterOrder_by_stmt(ctx)


def virtual_plan_create(sql):
    pass

def main():
    sql = """
        SELECT *, ptr.*,ptr.id, ptr.name, id, name, min(id), max(name), avg(*) FROM ptr LEFT JOIN ptr2 WHERE name = "Alice" and id = 1037 ORDER BY id LIMIT 10;
    """
    sql2 = """
        SELECT * FROM ptr WHERE name = "Alice" and id = 1037 ORDER BY id LIMIT 10;
    """
    sql3 = """
        SELECT * FROM A LEFT JOIN B ON 1=1;
    """

    input_stream = InputStream(sql3)
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