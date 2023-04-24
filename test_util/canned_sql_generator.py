with open('test_util/canned_sql.txt', 'w') as file:
    file.write('CREATE TABLE rela_i_i_1000(key int PRIMARY KEY, val int);\n')
    # CREATE TABLE tst (id int PRIMARY KEY,num int);
    for i in range(1000):
        file.write('INSERT INTO rela_i_i_1000(key, val) VALUES ('+str(i+1)+','+str(i+1)+');\n')
        # INSERT INTO orders(id, customer_id) VALUES (1,1);
