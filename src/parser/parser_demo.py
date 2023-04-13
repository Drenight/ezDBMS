from antlr4 import *
from MySqlLexer import MySqlLexer
from MySqlParser import MySqlParser
from MySqlParserVisitor import MySqlParserVisitor

class MySQLVisitor(MySqlParserVisitor):
    def __init__(self):
        self.table_name = None
        self.column_names = []

    def visitSqlStatement(self, ctx: MySqlParser.SqlStatementContext):
        return super().visitSqlStatement(ctx)
    
    def visitDmlStatement(self, ctx: MySqlParser.DmlStatementContext):
        #print(1)
        return super().visitDmlStatement(ctx)

    def visitTableSources(self, ctx: MySqlParser.TableSourcesContext):
        #print(222)
        return super().visitTableSources(ctx)

    def visitFromClause(self, ctx: MySqlParser.FromClauseContext):
        table_names = []
        for table_source in ctx.tableSources().tableSource():
            if table_source.tableAlias() is not None:
                table_names.append((table_source.tableName().getText(), table_source.tableAlias().getText()))
            else:
                table_names.append((table_source.tableName().getText(), None))
        print(table_names)

        print(ctx.tableSources())
        #for table_expr in ctx.tableSources().tableSource():
        #    print(table_expr)
        return super().visitFromClause(ctx)

def parse_sql(sql_statement):
    input_stream = InputStream(sql_statement)
    lexer = MySqlLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = MySqlParser(stream)
    tree = parser.sqlStatement()

    visitor = MySQLVisitor()
    print(visitor.visit(tree))
    #print(visitor.get_table_name())


def main():
    parsed_tree = parse_sql("SELECT * FROM table1 WHERE field1 = 'value1';")
    print(parsed_tree)

main()

