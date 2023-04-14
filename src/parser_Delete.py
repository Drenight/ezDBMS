# 复制
from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

# 英雄登场
# 核心要改的内容
class DeletePlan:
    def __init__(self):
        # 这些属性，要改，根据你是什么语句，决定你要存哪些参数
        self.table_name = None
        self.columns = []  # 列的信息，每一项包含列名、数据类型、是否为主键等
        self.primary_key = None  # 主键的信息，包含主键名和包含哪些列
        self.foreign_key = {}

# 核心要改的内容
class DeleteListener(SQLiteParserListener):
    def __init__(self):
        self.plan = CreatePlan()

    # 开发的时候下面删掉
    # enter写那些这条语句，的语法树的各个节点的enter，拿信息
    def enterCreate_table_stmt(self, ctx:SQLiteParser.Create_table_stmtContext):
        # 获取表名
        self.plan.table_name = ctx.table_name().getText()

    def enterTable_constraint(self, ctx: SQLiteParser.Table_constraintContext):


# 直接复制
def virtual_plan_create(sql):
    print(sql)
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
        DELETE FROM table_name WHERE condition;
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
    listener = DeleteListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    print(listener.plan.__dict__)

main()