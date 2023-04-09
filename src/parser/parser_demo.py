from antlr4 import *
from MySqlLexer import MySqlLexer
from MySqlParser import MySqlParser
from MySqlParserVisitor import MySqlParserVisitor


class MySQLVisitor(MySqlParserVisitor):
    def visitSqlStatement(self, ctx):
        #print(ctx.)
        return ctx.getText()


def parse_sql(sql_statement):
    input_stream = InputStream(sql_statement)
    lexer = MySqlLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = MySqlParser(stream)
    tree = parser.sqlStatement()

    visitor = MySqlParserVisitor()
    return visitor.visit(tree)


parsed_tree = parse_sql("SELECT id, name FROM users WHERE age > 18;")
print(parsed_tree)
