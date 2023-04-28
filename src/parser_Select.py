from ntpath import join
from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

#   | expr ( LT2 | GT2 | AMP | PIPE) expr
#   | expr ( LT | LT_EQ | GT | GT_EQ) expr
#   | expr (
#       ASSIGN
#       | EQ
#       | NOT_EQ1
#       | NOT_EQ2
#       | IS_
#       | IS_ NOT_
#       | IN_
#       | LIKE_
#       | GLOB_
#       | MATCH_
#       | REGEXP_
#   ) expr
#   | expr AND_ expr
#   | expr OR_ expr
class SelectPlan:
    def __init__(self):
        self.queryAttr = []         # standard as (relaName, attr, min)
        #self.rela2attr = {}         # rela -> tuple of attr
        self.table_name = None 

        self.Aggr  = False
        self.Join  = False

        self.where_expr = None
        self.where_logic = None
        self.where_expr1_eval = None   # MIN(ptr.id) >= 3700
        self.where_expr2_eval = None   # MIN(ptr.name) == 'Alice'

        self.having_expr = None
        self.having_logic = None        # "AND".lower()
        self.having_expr1_eval = None   # MIN(ptr.id) >= 3700
        self.having_expr2_eval = None   # MIN(ptr.name) == 'Alice'

        self.joinRela1 = None
        self.joinRela2 = None
        self.joinOP = None      # INNER
        self.join_expr_eval = None
 
        self.Group = False
        self.group_attr = None

        self.orderByAttr = None
        self.orderByAsc = True
        self.limit = None
        
        self.txtAttrs = []
        self.asName = None

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
            self.plan.txtAttrs.append(attr)
        logging.debug(self.plan.txtAttrs)

        # get from's relation, join不会进
        if ctx.table_or_subquery() != []:
            fromCtx = ctx.table_or_subquery()[0]
            tableName = fromCtx.table_name().getText()
            self.plan.table_name = tableName
            logging.debug("from table is:" + str(tableName))

        # get group 
        groupAttrList = ctx.groupByExpr
        if groupAttrList != []:
            self.plan.Group = True
            self.plan.group_attr = groupAttrList[0].getText()              # TODO，约定单列group

        # get having, make it eval-available
        if ctx.havingExpr != None:
            # solve logic and or
            self.plan.having_expr = ctx.havingExpr.getText()
            havingExprCtx = ctx.havingExpr
            if str(havingExprCtx.AND_()) == 'AND':
                self.plan.having_logic = 'and'
            elif str(havingExprCtx.OR_()) == 'OR':
                self.plan.having_logic = 'or'

            #print("logic is ", self.plan.having_logic)
            #print("having expr:", self.plan.having_expr)
            if self.plan.having_logic != None:
                self.plan.having_expr1_eval = self.plan.having_expr[:self.plan.having_expr.index(self.plan.having_logic.upper())]
                self.plan.having_expr2_eval = self.plan.having_expr[self.plan.having_expr.index(self.plan.having_logic.upper())+len(self.plan.having_logic):]
            else:
                self.plan.having_expr1_eval = self.plan.having_expr
                self.plan.having_expr2_eval = None 
            # = -> ==   
            s1 = self.plan.having_expr1_eval
            s2 = self.plan.having_expr2_eval
            if '!' not in s1 and '<' not in s1 and '>' not in s1 and '=' in s1:
                self.plan.having_expr1_eval = self.plan.having_expr1_eval.replace('=', '==')
            if s2 != None and '!' not in s2 and '<' not in s2 and '>' not in s2 and '=' in s2:
                self.plan.having_expr2_eval = self.plan.having_expr2_eval.replace('=', '==')
        # having done?

        # get where
        if ctx.whereExpr != None:
            #print(ctx.whereExpr.getText())
            self.plan.where_expr = ctx.whereExpr.getText()
            whereExprCtx = ctx.whereExpr
            if str(whereExprCtx.AND_()) == 'AND':
                self.plan.where_logic = 'and'
            elif str(whereExprCtx.OR_()) == 'OR':
                self.plan.where_logic = 'or'

            #print(self.plan.where_logic)

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
        # where done?

        # get join, if join table_or_subquery==[]
        joinCtx = ctx.join_clause()
        if joinCtx != None:
            self.plan.Join = True
            #for tar in joinCtx.table_or_subquery():
                #print("ww",tar.getText())

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
                    #print(rela1)
                    break
            #print("rela1 is gonna join rela2:", rela1, rela2)
            self.plan.joinRela1 = rela1
            self.plan.joinRela2 = rela2
            self.plan.joinOP = joinOP
            #print("JOIN OP is", joinOP)
            #joinOP = joinCtx.join_operator()[0]
            #print(len(joinCtx.join_operator()))
            #print("JOIN OP IS", joinOP.getText())

            if joinCtx.join_constraint() != []:
                joinCons = joinCtx.join_constraint()[0]
                expr = joinCons.expr().getText()
                if '!' not in expr and '<' not in expr and '>' not in expr and '=' in expr:
                    self.plan.join_expr_eval = expr.replace('=', '==')
                else:   #多捞哦，这给忘了
                    self.plan.join_expr_eval = expr

        return super().enterSelect_core(ctx)    

    # HOLD, attrs不是已经拿到了吗，如果只是为了填充虚拟计划，这样解析的意义是什么？  
    # 合法性吗？ 不是，SELECT )*( FROM TABLE A; 就算不写下面的解析也是会报错的
    # 似乎手解if-else也不会比这个简单多少？avg(*),avg(table.col1),avg(col)，这个用框架做逻辑分类，降低心智成本？
    def enterResult_column(self, ctx: SQLiteParser.Result_columnContext):       # enterSelect_core只会进一次，我们不嵌套，所以开个新的
        # *进来的
        if ctx.STAR() and ctx.table_name() == None:
            #print("pure *, from where", ctx.getText())
            self.plan.queryAttr.append(("special_WHERE", '*', ''))
        
        # table1.* 进来的
        if ctx.table_name() != None: 
            #self.plan.rela2attr.setdefault(ctx.table_name().getText(), {'*'})
            self.plan.queryAttr.append((ctx.table_name().getText(), '*', ''))
            #print("table name is", ctx.table_name().getText()) 
        
        # expr进来的，不带*
        expr_ctx = ctx.expr()
        if expr_ctx != None:
            if expr_ctx.table_name() != None:
                # 捕获给定rela.的列
                #print("given rela attrs: ",expr_ctx.table_name().getText())
                if expr_ctx.column_name() != None:  # 一定会进吧，SELECT 没有只给rela名的
                    #print(expr_ctx.column_name().getText())
                    self.plan.queryAttr.append((expr_ctx.table_name().getText(), expr_ctx.column_name().getText(), ''))
            elif expr_ctx.column_name() != None:
                # 捕获where的列，min之类聚合的不会被捕获
                #print("feel col", expr_ctx.column_name().getText())
                self.plan.queryAttr.append(("special_WHERE", expr_ctx.column_name().getText(), ''))
            elif expr_ctx.function_name()!= None:
                # 聚合列
                # 聚合列的填充，放到分完组之后再做吧？是的，还得在having之后
                self.plan.Aggr = True
                # function_name OPEN_PAR ((DISTINCT_? expr ( COMMA expr)*) | STAR)? CLOSE_PAR filter_clause? over_clause?
                if expr_ctx.STAR(): #等效 expr_ctx.expr() == []: 
                    # avg(*)这种
                    #print(expr_ctx.function_name().getText())
                    #print("fin: *")
                    self.plan.queryAttr.append(("special_WHERE", '*', expr_ctx.function_name().getText()))
                else:
                    function_ctx = expr_ctx.expr()[0]
                    #print(expr_ctx.function_name().getText())   #avg这种
                    if function_ctx != None:
                        if function_ctx.table_name()!=None:
                            #print("fin:",function_ctx.table_name().getText(), function_ctx.column_name().getText())
                            self.plan.queryAttr.append((function_ctx.table_name().getText(), function_ctx.column_name().getText(), expr_ctx.function_name().getText()))
                        else:
                            self.plan.queryAttr.append(('special_WHERE', function_ctx.column_name().getText(), expr_ctx.function_name().getText()))
            else:   # 全None，是表达式，暂不支持
                print("REA==================")
                print(expr_ctx.PLUS())

        logging.debug("queryAttr updated, now: " + str(self.plan.queryAttr))

        #print(self.plan.queryAttr)

        return super().enterResult_column(ctx)

    def enterSelect_stmt(self, ctx: SQLiteParser.Select_stmtContext):
        order_by_Ctx = ctx.order_by_stmt()
        if order_by_Ctx != None:
            self.plan.orderByAttr = order_by_Ctx.ordering_term()[0].expr().getText()
            if order_by_Ctx.ordering_term()[0].asc_desc().getText() == 'DESC':
                self.plan.orderByAsc = False

        limitCtx = ctx.limit_stmt()
        if limitCtx!= None:
            self.plan.limit = eval(limitCtx.expr()[0].getText())

        return super().enterSelect_stmt(ctx)

    def enterOrder_by_stmt(self, ctx: SQLiteParser.Order_by_stmtContext):
        return super().enterOrder_by_stmt(ctx)


def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = SelectListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

def main():
    sql0 = """
        SELECT tst.id, tst.num FROM tst WHERE tst.id>7 AND tst.id < 5;
    """
    # only for parsing
    sql1 = """
        SELECT *, ptr.*,ptr.id, ptr.name, id, name, MIN(ptr.id), MIN(id), MAX(name), AVG(*) FROM ptr LEFT JOIN ptr2 WHERE name = "Alice" and id = 1037 ORDER BY id LIMIT 10;
    """
    sql2 = """
        SELECT * FROM ptr WHERE name = "Alice" and id = 1037 ORDER BY id LIMIT 10;
    """
    sql3 = """
        SELECT * FROM A LEFT JOIN B ON 1=1;
    """
    sql4 = """
        SELECT min(id2salary.id), min(id2salary.salary) FROM id2salary GROUP BY id2salary.id HAVING MIN(id2salary.salary)>=1500 AND MIN(id2salary.salary)<2000;
    """
    sql5 = """
        SELECT id2salary.id, MIN(id2salary.salary) FROM id2salary GROUP BY id2salary.id HAVING MIN(id2salary.salary)>=1500;
    """
    sql6 = """
        SELECT min(id2salary.id), min(id2salary.salary) FROM id2salary GROUP BY id2salary.id HAVING MIN(id2salary.salary)>=1500 OR MIN(id2salary.salary)<2000;
    """
    sql7 = """
        SELECT * FROM id2salary WHERE id2salary.salary>=1500;
    """
    sql8 = """
        SELECT * FROM id2salary WHERE id2salary.salary>1500 AND id2salary.id<=1000;
    """
    sql9 = """
        SELECT min(id2salary.id), min(id2salary.salary) FROM id2salary WHERE id2salary.id=707 AND id2salary.salary>=1000 GROUP BY id2salary.id HAVING MIN(id2salary.salary)>=1500 OR MIN(id2salary.salary)<2000;
    """
    sql10 = """
        SELECT ptr.id, id2salary.id, ptr.name, id2salary.salary FROM ptr INNER JOIN id2salary ON ptr.id <= id2salary.id WHERE ptr.id>=300 AND id2salary.salary<7000;
    """
    sql11 = """
        SELECT min(ptr.id), id2salary.id, min(ptr.name), min(id2salary.salary) FROM ptr INNER JOIN id2salary ON ptr.id <= id2salary.id WHERE ptr.id>=300 AND id2salary.salary<7000 GROUP BY id2salary.id HAVING MIN(id2salary.salary)>=1500 OR MIN(id2salary.salary)<2000;
    """

    sql12 = """
        SELECT customer_name.id, customer_name.customer_name, orders.id, orders.customer_id FROM orders INNER JOIN customer_name ON orders.customer_id = customer_name.id;
    """
    # This is ok cause input order is correct, but 12 is temporary bugging
    # bug fixed at "ans1 = baseDBDict[rela2][uu2][join_attr1]"
    # commit 7250162
    sql13 = """
        SELECT 
        customer_name.id, MIN(customer_name.customer_name), SUM(orders.id), MIN(orders.customer_id) 
        FROM 
        customer_name 
        INNER JOIN 
        orders 
        ON 
        customer_name.id = orders.customer_id 
        WHERE
        orders.id>0 OR orders.id>=1000
        GROUP BY 
        customer_name.id 
        HAVING 
        MIN(orders.customer_id)>=0 OR MIN(orders.customer_id)=1
        ORDER BY 
        customer_name.id ASC
        LIMIT 
        15;
    """

    sql14 = """
        SELECT rela_i_i_10000.key, rela_i_i_10000.val FROM rela_i_i_10000 WHERE rela_i_i_10000.key>=7 AND rela_i_i_10000.val<=99993;
    """

    sql15 = """
        SELECT * FROM rela_i_i_100000 ORDER BY rela_i_i_100000.key DESC LIMIT 15;
    """

    input_stream = InputStream(sql15)
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