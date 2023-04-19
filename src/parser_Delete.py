from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

# 核心要改的内容
class DeletePlan:
    def __init__(self):
        # 这些属性，要改，根据你是什么语句，决定你要存哪些参数
        self.table_name = None
        self.condition = []  # TODO  这里是否用[]来储存
        

# 核心要改的内容
class DeleteListener(SQLiteParserListener):
    def __init__(self):
        self.plan = DeletePlan()

    # enter写那些这条语句，的语法树的各个节点的enter，拿信息
    def enterDelete_stmt(self, ctx:SQLiteParser.Delete_stmtContext):
        self.plan.table_name = ctx.qualified_table_name().getText()
        logging.debug(ctx.qualified_table_name().getText())

    def enterExpr(self, ctx:SQLiteParser.ExprContext):
        if ctx.parentCtx.getRuleIndex() == SQLiteParser.RULE_delete_stmt: #判断当前表达式所属的上下文（parentCtx）是否是一个DELETE语句（RULE_delete_stmt）
            self.plan.condition.append(ctx.getText()) #如果是，则将表达式添加到查询计划的条件（condition）中

    # TODO WHERE



# 直接复制
def virtual_plan_delete(sql):
    logging.debug(sql)
    # 创建一个输入流
    input_stream = InputStream(sql)

    # 创建一个词法分析器
    lexer = SQLiteLexer(input_stream)

    # 创建一个词法记号流
    token_stream = CommonTokenStream(lexer)

    # 创建一个语法分析器
    parser = SQLiteParser(token_stream)

    # 解析 SQL 语句，生成语法树
    tree = parser.parse()

    # 创建一个 Listener 实例
    listener = DeleteListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

# 自测这个单个模块用的
# CREATE TABLE TTT(ID INT PRIMARY KEY);
def main():
    # 要解析的 SQL 语句
    sql = """
        DELETE FROM ptr WHERE mask>=60 ;
    """
   # sql = """
    #    DELETE FROM ptr AS ptr_ex WHERE mask >= 60 AND ; 不兼容AS语法
   # """

    

    # 创建一个输入流
    input_stream = InputStream(sql)

    # 创建一个词法分析器
    lexer = SQLiteLexer(input_stream)

    # 创建一个词法记号流
    token_stream = CommonTokenStream(lexer)

    # 创建一个语法分析器
    parser = SQLiteParser(token_stream)

    # 解析 SQL 语句，生成语法树
    tree = parser.parse()

    # 创建一个 Listener 实例
    listener = DeleteListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    logging.debug(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()