from lark import Lark

grammar = """
    start: select_statement
    select_statement: "SELECT" column_list "FROM" table_name ["WHERE" expression]
    column_list: "*" | column_name ("," column_name)*
    table_name: CNAME
    column_name: CNAME
    expression: term (("AND" | "OR") term)*
    term: factor ((">" | ">=" | "<" | "<=" | "=" | "!=") factor)*
    factor: column_name | NUMBER | STRING | "(" expression ")"
    %import common.CNAME
    %import common.NUMBER
    %import common.STRING
    %import common.WS
    %ignore WS
"""

parser = Lark(grammar, start='start')
