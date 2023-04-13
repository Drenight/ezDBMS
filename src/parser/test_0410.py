import sys
from antlr4 import *
from MySqlLexer import MySqlLexer
from MySqlParser import MySqlParser
from MySqlParserListener import MySqlParserListener
from antlr4.InputStream import InputStream
from typing import List

class CreateDatabaseListener(MySqlParserListener):
    def __init__(self):
        self.table_name = None
        self.column_names = []

    def enterCreateDefinitions(self, ctx: MySqlParser.CreateDefinitionsContext):
        self.table_name = ctx.parentCtx.tableName().getText()

    def get_table_name(self):
        return self.table_name

    def enterColumnDeclaration(self, ctx: MySqlParser.ColumnDeclarationContext):
        column_name = ctx.getChild(0).getText()
        self.column_names.append(column_name)

    def get_table_name(self):
        return self.table_name

    def get_column_names(self):
        return self.column_names

def main():
    sql = "CREATE TABLE table1 (COLUMN_NAME INT);"
    print(sql)


    lexer = MySqlLexer(InputStream(sql))
    parser = MySqlParser(CommonTokenStream(lexer))


    listener = CreateDatabaseListener()
    ParseTreeWalker.DEFAULT.walk(listener, parser.sqlStatements())


    table_name = listener.get_table_name()
    column_names = listener.get_column_names()


    if table_name:
        print("Table name:", table_name)
    else:
        print("No table name found.")


    if column_names:
        print("Column names:", column_names)
    else:
        print("No column names found.")


if __name__ == "__main__":
    main()
