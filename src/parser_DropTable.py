from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

#要改的内容
class DropPlan:
    def __init__(self):
        self.table_name = None
        
        

# 核心要改的内容
# 修改类名为 DropListener 并继承 SQLiteParserListener
class DropListener(SQLiteParserListener):
    def __init__(self):
        self.plan = DropPlan()

    # 修改为处理 DROP TABLE 语句的方法
    def enterDrop_stmt(self, ctx: SQLiteParser.Drop_stmtContext):
        self.plan.table_name = ctx.any_name().getText()#ctx.table_name().getText()
        logging.debug(self.plan.table_name)
        return super().enterDrop_stmt(ctx)


# 直接复制
def virtual_plan_drop(sql):
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
    listener = DropListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

# 自测这个单个模块用的
# CREATE TABLE TTT(ID INT PRIMARY KEY);
def main():
    # 要解析的 SQL 语句
    sql = """
        DROP TABLE table_name;
    """
    ## {
    # 'table_name': 'orders', 
    # 'columns': [
    #   {'name': 'id', 'type': 'INT', 'constraints': [{'type': 'PRIMARYKEY'}]}, 
    #   {'name': 'customer_id', 'type': 'INT', 'constraints': []}, 
    #   {'name': 'order_date', 'type': 'DATE', 'constraints': []}], 
    # 'primary_key': 'id', 
    # 'foreign_key': {'table': 'customers', 'local_columns': ['customer_id'], 'foreign_columns': ['id']}
    # }

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
    listener = DropListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    logging.debug(listener.plan.__dict__)

if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG)
   main()