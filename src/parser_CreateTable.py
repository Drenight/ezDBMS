from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging
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

def main():
    sql = """
        CREATE TABLE customer_name (
        id int PRIMARY KEY,
        customer_name str
        );
    """

    sql2 = """
        CREATE TABLE orders (
        id int PRIMARY KEY,
        customer_id int,
        FOREIGN KEY (customer_id) REFERENCES customer_name(id)
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

    logging.debug(listener.plan.__dict__)

if __name__ == '__main__':
    #print("22")
    logging.basicConfig(level=logging.DEBUG)
    main()