# 复制
from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

# 英雄登场
# 核心要改的内容
class InsertPlan:
    def __init__(self):
        # 这些属性，要改，根据你是什么语句，决定你要存哪些参数
        self.table_name = None
        self.columns = []  # 列的信息，每一项包含列名、数据类型、是否为主键等
        self.primary_key = None  # 主键的信息，包含主键名和包含哪些列
        self.foreign_key = {}

# 核心要改的内容
class InsertListener(SQLiteParserListener):
    def __init__(self):
        self.plan = InsertPlan()

    # 开发的时候下面删掉
    # enter写那些这条语句，的语法树的各个节点的enter，拿信息
    def enterCreate_table_stmt(self, ctx:SQLiteParser.Create_table_stmtContext):
        # 获取表名
        self.plan.table_name = ctx.table_name().getText()

    def enterTable_constraint(self, ctx: SQLiteParser.Table_constraintContext):
        if ctx.PRIMARY_():
            logging.debug(1)
        if ctx.FOREIGN_():
            constraint = ctx.foreign_key_clause()
            foreign_key = {"table": constraint.foreign_table().getText()}
            logging.debug(foreign_key)
            local_columns = []
            foreign_columns = []
            logging.debug(ctx.indexed_column())

            cnt = 0
            for indexed_column in ctx.column_name():
                logging.debug(indexed_column.getText())
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
                    logging.debug(f"Found foreign key constraint: {foreign_key.name(0)}({foreign_key.indexed_column(0).getText()}) references {foreign_key.foreign_table().getText()}({foreign_key.foreign_column(0).getText()})")

                if constraint.PRIMARY_():
                    constraints.append({"type": "PRIMARYKEY"})
                    self.plan.primary_key = column_name

        self.plan.columns.append({"name": column_name, "type": data_type, "constraints": constraints})

# 直接复制
def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = CreateListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

# 自测这个单个模块用的
# CREATE TABLE TTT(ID INT PRIMARY KEY);
def main():
    sql = """
        CREATE TABLE orders (
        id INT PRIMARY KEY,
        customer_id INT,
        FOREIGN KEY (customer_id) REFERENCES ptr(id)
        );
    """

    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = CreateListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    print(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()