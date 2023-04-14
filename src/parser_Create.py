from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

class CreatePlan:
    def __init__(self):
        self.table_name = None
        self.columns = []  # 列的信息，每一项包含列名、数据类型、是否为主键等
        self.primary_key = None  # 主键的信息，包含主键名和包含哪些列
        self.foreign_key = {}

class CreateListener(SQLiteParserListener):
    def __init__(self):
        self.plan = CreatePlan()

    def enterCreate_table_stmt(self, ctx:SQLiteParser.Create_table_stmtContext):
        # 获取表名
        self.plan.table_name = ctx.table_name().getText()

    def enterTable_constraint(self, ctx: SQLiteParser.Table_constraintContext):
        if ctx.PRIMARY_():
            print(1)
        if ctx.FOREIGN_():
            constraint = ctx.foreign_key_clause()
            foreign_key = {"table": constraint.foreign_table().getText()}
            print(foreign_key)
            local_columns = []
            foreign_columns = []
            print(ctx.indexed_column())

            cnt = 0
            for indexed_column in ctx.column_name():
                print(indexed_column.getText())
                local_columns.append(indexed_column.getText())
                foreign_columns.append(ctx.foreign_key_clause().column_name()[0].getText())
                cnt += 1

            foreign_key["local_columns"] = local_columns
            foreign_key["foreign_columns"] = foreign_columns
            #constraints.append({"type": "FOREIGN KEY", "foreign_key": foreign_key})
            self.plan.foreign_key = foreign_key
        return super().enterTable_constraint(ctx)

    def enterColumn_def(self, ctx:SQLiteParser.Column_defContext):
        # 获取列的信息
        column_name = ctx.column_name().getText()
        data_type = ctx.type_name().getText()
        constraints = []

        if ctx.column_constraint():
            for constraint in ctx.column_constraint():
                if constraint.foreign_key_clause():
                    foreign_key = ctx.foreign_key_clause()
                    print(f"Found foreign key constraint: {foreign_key.name(0)}({foreign_key.indexed_column(0).getText()}) references {foreign_key.foreign_table().getText()}({foreign_key.foreign_column(0).getText()})")

                if constraint.PRIMARY_():
                    constraints.append({"type": "PRIMARYKEY"})
                    self.plan.primary_key = column_name

        self.plan.columns.append({"name": column_name, "type": data_type, "constraints": constraints})


# 定义一个继承自 SQLiteParserListener 的 Listener 类
class MyListener(SQLiteParserListener):
    
    # 处理 select_stmt
    def enterSelect_stmt(self, ctx:SQLiteParser.Select_stmtContext):
        print("enter select_stmt")

    # 处理 table_or_subquery
    def enterTable_or_subquery(self, ctx:SQLiteParser.Table_or_subqueryContext):
        print("enter table_or_subquery:", ctx.getText())

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
    listener = CreateListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

# CREATE TABLE TTT(ID INT PRIMARY KEY);
def main():
    # 要解析的 SQL 语句
    sql = """
        CREATE TABLE orders (
        id INT PRIMARY KEY,
        customer_id INT,
        FOREIGN KEY (customer_id) REFERENCES ptr(id)
        );
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
    listener = CreateListener()

    # 遍历语法树
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    print(listener.plan.__dict__)

main()