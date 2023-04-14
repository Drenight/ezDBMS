
try:
    input = raw_input   # For Python2 compatibility
except NameError:
    pass

from lark import Lark
from beautifultable import BeautifulTable

sql_grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS

	EQUAL: "="
	LT: "<"
	GT: ">"
	LTE: "<="
	GTE: ">="
	comparison: EQUAL | LT | GT | LTE | GTE
	COMMA: ","
	PERIOD: "."
	LEFT_PAREN: "("
	RIGHT_PAREN: ")"
	SEMICOLON: ";"
	QUOTE: "'"

	NAME: CNAME+
	NUMBER: SIGNED_NUMBER+

	ALL: "*"
	AND: "AND"i
	ASC: "ASC"i
	BETWEEN: "BETWEEN"i
	BY: "BY"i
	DESC: "DESC"i
	FROM: "FROM"i
	MAX: "MAX"i
	MIN: "MIN"i
	ORDER: "ORDER"i
	SELECT: "SELECT"i
	WHERE: "WHERE"i


	char_num: (NAME | NUMBER)*

	start: [select_statement end]

	end: SEMICOLON

	select_statement: SELECT selection from_clause options*

	selection: ALL
		|   num_word_commalist

	num_word_commalist: [max_min] num_word
		|   num_word_commalist COMMA [max_min] num_word

	num_word: column_table
		|	LEFT_PAREN num_word RIGHT_PAREN
		|   item

	options: where_clause
		|   order_by_clause

	from_clause: FROM column_table_commalist

	column_table_commalist: column_table
		|	column_table_commalist COMMA column_table

	column_table: char_num
		|   char_num PERIOD char_num

	where_clause: WHERE search_condition

	search_condition: search_condition AND search_condition
		|	LEFT_PAREN search_condition RIGHT_PAREN
		|	predicate

	predicate: between_predicate
		|	comparison_predicate

	between_predicate: num_word BETWEEN num_word AND num_word

	comparison_predicate: num_word comparison num_word

	item_commalist: item
		|	item_commalist COMMA item

	item: QUOTE char_num QUOTE

	order_by_clause: ORDER BY order_by_commalist

	order_by_commalist: order_by
		|	order_by_commalist COMMA order_by

	order_by: column_table asc_desc

	asc_desc: ASC
		|   DESC

	max_min: MAX
		| MIN
"""

datatables = {
	"people": {
		"first_name": ["Elvis", "Elton", "Ariana", "Katy", "Blake"],
		"last_name": ["Presley", "John", "Grande", "Perry", "Lively"],
		"age": [42, 75, 36, 37, 34],
		"city": ["Memphis", "Pinner", "Boca Raton", "Santa Barbara", "Los Angeles"],
		"day": [8, 25, 6, 25, 25],
		"month": [1, 3, 6, 10, 8],
		"year": [1935, 1947, 1993, 1984, 1987],
		"alive": ["no", "yes", "yes", "yes", "yes"]
	},
	"sports": {
		"team": ["Arsenal", "Manchester United", "Brentford", "Liverpool"],
		"city": ["London", "Manchester", "Brentford", "Liverpool"],
		"standing": [4, 6, 12, 2],
		"year_founded": [1886, 1878, 1889, 1892]
	}
}

comparisons = ["=", ">", "<", ">=", "<="]
select_cols = []
tables = []
where_items = []
max_min_cols = []
order_by_cols = []
asc_desc = ""
max_min = ""

end_flag = False
all_flag = False
from_flag = False
column_flag = False
where_flag = False
between_flag = False
max_min_flag = False
order_by_flag = False

result = BeautifulTable()

def apply(token):
	global asc_desc, max_min, end_flag, all_flag, column_flag, from_flag, where_flag, between_flag, max_min_flag, order_by_flag

	if token == "SELECT":
		column_flag = True
	elif token == ";":
		end_flag = True
	elif token == "ORDER":
		order_by_flag = True
		from_flag = False
	elif token == "BY":
		pass
	elif token == "FROM":
		from_flag = True
	elif token == "WHERE":
		where_flag = True
	elif token == "BETWEEN":
		between_flag = True
	elif token == "AND":
		pass
	elif token == "ASC":
		asc_desc = "ASC"
	elif token == "DESC":
		asc_desc = "DESC"
	elif order_by_flag:
		if asc_desc != "ASC" and asc_desc != "DESC":
			order_by_cols.append(token)
	elif token == "MAX":
		max_min = "MAX"
		max_min_flag = True
	elif token == "MIN":
		max_min = "MIN"
		max_min_flag = True
	elif where_flag:
		where_items.append(token)
	elif from_flag:
		if token.type == "NAME":
			tables.append(token)
	elif max_min_flag:
		max_min_cols.append(token)
		max_min_flag = False
		select_cols.append(token)
	elif column_flag:
		if token == "*":
			all_flag = True
		else:
			select_cols.append(token)

	if end_flag:
		if where_flag:
			if between_flag:
				matches = {}
				for name in tables:
					for key, values in datatables[name].items():
						between_vals = []
						if key == where_items[0]:
							for v in values:
								for item in where_items[1:]:
									if item.type == "NUMBER":
										between_vals.append(int(item))
									elif item.type == "NAME":
										between_vals.append(str(item))
								if between_vals[0] < v < between_vals[1]:
									if key in matches:
										matches[key].append(v)
									else:
										matches[key] = [v]

							indices = []
							for v in values:
								for match in matches[key]:
									if v in matches[key]:
										indices.append(values.index(match))						
					datatables[name] = {k:[elt for ind, elt in enumerate(v) if ind in indices] for k,v in datatables[name].items()}
			else:
				temp_cols = []
				for name in tables:
					for key, value in datatables[name].items():
						temp_comps = []
						temp_vals = []
						quote_count = 0
						concat_val = ""
						for item in where_items:
							if key == item:
								temp_cols.append(key)
							elif item in comparisons:
								temp_comps.append(item)
							if item not in temp_cols and item not in temp_comps:
								if item == "'":
									quote_count += 1
								if item.type == "NUMBER":
									temp_vals.append(int(item))
								elif item.type == "NAME":
									temp_vals.append(str(item))
									if quote_count % 2 != 0:
										concat_val = " ".join([concat_val, item])
										temp_vals.remove(item)
								if quote_count % 2 == 0:
									temp_vals.append(concat_val)
									concat_val = ""

					indices = []
					valid_data = []
					for col in temp_cols:
						for sign in temp_comps:
							for val in temp_vals:
								if val == "":
									break
								elif isinstance(val, str):
									val = val[1:]
								for data in datatables[name][col]:
									if sign == "=":
										if data == val:
											valid_data.append(data)
									if sign == ">":
										if data > val:
											valid_data.append(data)
									if sign == "<":
										if data < val:
											valid_data.append(data)
									if sign == ">=":
										if data >= val:
											valid_data.append(data)
									if sign == "<=":
										if data <= val:
											valid_data.append(data)
						
							for key, values in datatables[name].items():
								for v in values:
									for d in valid_data:
										if v in valid_data:
											indices.append(values.index(d))
										
					datatables[name] = {k:[elt for ind, elt in enumerate(v) if ind in indices] for k,v in datatables[name].items()}

		if order_by_flag:
			for name in tables:
				for key in datatables[name].keys():
					for col in order_by_cols:
						if key == col:
							if asc_desc == "ASC":
								datatables[name][key].sort()
							elif asc_desc == "DESC":
								datatables[name][key].sort(reverse=True)

		if all_flag:
			for name in tables:
				for value in datatables[name].values():
					result.columns.append(value)
				result.columns.header = datatables[name].keys()
			print(result)
		else:
			for name in tables:
				datatables[name] = {k : datatables[name][k] for k in select_cols}
				index = []
				if max_min == "MAX":
					for col in max_min_cols:
						index.append(datatables[name][col].index(max(datatables[name][col])))
					datatables[name] = {k:[elt for ind, elt in enumerate(v) if ind in index] for k,v in datatables[name].items()}
				elif max_min == "MIN":
					for col in max_min_cols:
						index.append(datatables[name][col].index(min(datatables[name][col])))
					datatables[name] = {k:[elt for ind, elt in enumerate(v) if ind in index] for k,v in datatables[name].items()}

				for key, value in datatables[name].items():
					if key in select_cols:
						result.columns.append(value)
			result.columns.header = select_cols
			print(result)


def eval_tree(tree):
	if tree.data == "start":
		eval_tree(tree.children[0]) #select_statement
		eval_tree(tree.children[1]) #end
	elif tree.data == "end":
		apply(tree.children[0]) #SEMICOLON
	elif tree.data == "select_statement":
		apply(tree.children[0]) #SELECT
		eval_tree(tree.children[1]) #selection
		eval_tree(tree.children[2]) #from clause
		if len(tree.children) > 3:
			eval_tree(tree.children[3])
	elif tree.data == "selection":
		if tree.children[0] == "*":
			apply(tree.children[0]) # (all) *
		else:
			eval_tree(tree.children[0])
	elif tree.data == "num_word_commalist":
		#tree.children[1] 	comma
		if len(tree.children) == 2:
			if tree.children[0] != None: #optional max/min
				eval_tree(tree.children[0])
			eval_tree(tree.children[1])
		else:
			eval_tree(tree.children[0])
			if tree.children[2] != None: #optional max/min
				eval_tree(tree.children[2])
			eval_tree(tree.children[3])
	elif tree.data == "num_word":
		if len(tree.children) == 3:
			#eval_tree(tree.children[0]) left parenthesis
			eval_tree(tree.children[1])
			#eval_tree(tree.children[2]) right parenthesis
		elif len(tree.children) == 1:
			eval_tree(tree.children[0])
	elif tree.data == "from_clause":
		apply(tree.children[0])
		eval_tree(tree.children[1])
	elif tree.data == "column_table_commalist":
		eval_tree(tree.children[0])
	elif tree.data == "column_table":
		eval_tree(tree.children[0])
	elif tree.data == "char_num":
		for x in range(len(tree.children)):
			apply(tree.children[x])
	elif tree.data == "options":
		eval_tree(tree.children[0])
	elif tree.data == "where_clause":
		apply(tree.children[0]) #WHERE
		eval_tree(tree.children[1]) # search condition
	elif tree.data == "order_by_clause":
		apply(tree.children[0]) #ORDER
		apply(tree.children[1]) #BY
		eval_tree(tree.children[2]) # order_by_commalist
	elif tree.data == "search_condition":
		eval_tree(tree.children[0])
	elif tree.data == "predicate":
		eval_tree(tree.children[0])
	elif tree.data == "comparison_predicate":
		eval_tree(tree.children[0]) #num_word
		eval_tree(tree.children[1]) #comparison
		eval_tree(tree.children[2])
	elif tree.data == "comparison":
		apply(tree.children[0])
	elif tree.data == "between_predicate":
		eval_tree(tree.children[0])
		apply(tree.children[1])
		eval_tree(tree.children[2])
		apply(tree.children[3])
		eval_tree(tree.children[4])
	elif tree.data == "order_by_commalist":
		#tree.children[1] 	comma
		if len(tree.children) == 1:
			eval_tree(tree.children[0])
		else:
			eval_tree(tree.children[0])
			eval_tree(tree.children[2])
	elif tree.data == "order_by":
		eval_tree(tree.children[0]) #column_table
		eval_tree(tree.children[1]) #asc_desc
	elif tree.data == "asc_desc":
		apply(tree.children[0])
	elif tree.data == "max_min":
		apply(tree.children[0])
	elif tree.data == "item":
		apply(tree.children[0]) # quote
		eval_tree(tree.children[1])
		apply(tree.children[2]) # quote
	else:
		raise SyntaxError('unrecognized tree')

def examples(option):
	sql_statement = ""

	# all
	if option == 1:
		sql_statement = """
			SELECT *
			FROM people;
		"""
	# specific
	elif option == 2:
		sql_statement = """
			SELECT team, standing, year_founded
			FROM sports;
		"""
	# where
	elif option == 3:
		sql_statement = """
			SELECT last_name, first_name, age, day, month, year
			FROM people
			WHERE age > 40;
		"""
	# max
	elif option == 4:
		sql_statement = """
			SELECT first_name, last_name, MAX(age)
			FROM people;
		"""
	# min
	elif option == 5:
		sql_statement = """
			SELECT team, MIN(standing)
			FROM sports;
		"""
	# order asc
	elif option == 6:
		sql_statement = """
			SELECT first_name
			FROM people
			ORDER BY
				first_name ASC;
		"""
	# order desc
	elif option == 7:
		sql_statement = """
			SELECT team, year_founded
			FROM sports
			ORDER BY
				year_founded DESC;
		"""
	# all between
	elif option == 8:
		sql_statement = """
			SELECT first_name, last_name, age
			FROM people
			WHERE age BETWEEN 35 AND 40;
		"""


	parse_tree = parser.parse(sql_statement)
	print(parse_tree)
	eval_tree(parse_tree)

if __name__ == '__main__':
	parser = Lark(sql_grammar)
	code = input('Type \'sql\' to create own query or \'example\' to use some given example queries: \n')
	if code == "sql":
		sql = input('> ')
		parse_tree = parser.parse(sql)
		eval_tree(parse_tree)
	elif code == "example":
		example = input('Choose from given example by typing a name below: \n all \n specific \n where \n max \n min \n order asc \n order desc \n all between \n >')
		if example == "all":
			examples(1)
		elif example == "specific":
			examples(2)
		elif example == "where":
			examples(3)
		elif example == "max":
			examples(4)
		elif example == "min":
			examples(5)
		elif example == "order asc":
			examples(6)
		elif example == "order desc":
			examples(7)
		elif example == "all between":
			examples(8)
	