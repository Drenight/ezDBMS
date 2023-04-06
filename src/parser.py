import sqlparse

sql = "SELECT * FROM my_table WHERE age > 18"
parsed = sqlparse.parse(sql)[0]
ast = parsed.tokens

print(ast)
