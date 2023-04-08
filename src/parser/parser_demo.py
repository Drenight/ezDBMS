from antlr4 import *
from antlr4.tree.Trees import Trees
from MySqlLexer import MySqlLexer
from MySqlParser import MySqlParser

class MyVisitor(ParseTreeVisitor):
    def visitSqlStatement(self, ctx: MySqlParser.SqlStatementContext):
        print("Visiting sql statement: " + ctx.getText())
        return self.visitChildren(ctx)

    def visitSelectStatement(self, ctx: MySqlParser.SelectStatementContext):
        print("Visiting select statement: " + ctx.getText())
        return self.visitChildren(ctx)

    def visitFromClause(self, ctx: MySqlParser.FromClauseContext):
        print("Visiting from clause: " + ctx.getText())
        return self.visitChildren(ctx)

    def visitTableReference(self, ctx: MySqlParser.TableReferenceContext):
        print("Visiting table reference: " + ctx.getText())
        return self.visitChildren(ctx)

    def visitWhereClause(self, ctx: MySqlParser.WhereClauseContext):
        print("Visiting where clause: " + ctx.getText())
        return self.visitChildren(ctx)

    def visitChildren(self, node):
        for child in node.children:
            self.visit(child)

# 读取输入文件并创建词法分析器和语法分析器
input_stream = FileStream("path_to_input_file.txt")
lexer = MySqlLexer(input_stream)
token_stream = CommonTokenStream(lexer)
parser = MySqlParser(token_stream)

# 生成语法树
tree = parser.sqlStatement()

# 打印语法树
print(Trees.toStringTree(tree, None, parser))

# 遍历语法树
visitor = MyVisitor()
visitor.visit(tree)
