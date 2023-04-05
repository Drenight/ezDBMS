from sqlalchemy import text

# SQL query to parse
query = "SELECT * FROM table WHERE column = 'value'"

# Parse the query and generate AST
ast = text(query).compile(dialect="sqlite").parsed

# Print the AST
print(ast)
