from antlr4 import *
from parser.antlr.SQLiteLexer import SQLiteLexer
from parser.antlr.SQLiteParser import SQLiteParser
from parser.antlr.SQLiteParserListener import SQLiteParserListener

import logging

class InsertPlan:
    def __init__(self):
        self.table_name = None
        self.columnsKey = []  
        self.columnsValue = []
        self.asName = None

class InsertListener(SQLiteParserListener):
    def __init__(self):
        self.plan = InsertPlan()

    def enterInsert_stmt(self, ctx: SQLiteParser.Insert_stmtContext):
        self.plan.table_name = ctx.table_name().getText()
        logging.debug(ctx.table_name().getText())
        if ctx.table_alias() != None:
            self.plan.asName = ctx.table_alias().getText()
        for k in ctx.column_name():
            logging.debug(k.getText())
            self.plan.columnsKey.append(k.getText())
        return super().enterInsert_stmt(ctx)

    def enterValues_clause(self, ctx: SQLiteParser.Values_clauseContext):
        textLst = ctx.value_row()[0].getText()
        logging.debug(textLst)
        tuple_obj = eval(textLst)
        list_obj = []
        for i in range(len(tuple_obj)):
            if isinstance(tuple_obj[i], str):
                list_obj.append(str(tuple_obj[i]))
            elif isinstance(tuple_obj[i], int):
                list_obj.append(int(tuple_obj[i]))
            else:
                raise ValueError("Unexpected data type")
        logging.debug(list_obj)  
        self.plan.columnsValue = list_obj
        return super().enterValues_clause(ctx)

def virtual_plan_create(sql):
    logging.debug(sql)
    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = InsertListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    return listener.plan

def main():
    sql = """
        INSERT INTO ptr (id, name)
        VALUES (122, 'Alice');
    """
    sql2 = """
        INSERT INTO ptr (id,name) VALUES (909,'Alice');
    """

    input_stream = InputStream(sql)
    lexer = SQLiteLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = SQLiteParser(token_stream)
    tree = parser.parse()
    listener = InsertListener()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)
    logging.debug(listener.plan.__dict__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()