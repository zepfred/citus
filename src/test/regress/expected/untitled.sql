CREATE TABLE target_table(col_1 int primary key, col_2 int);
SELECT create_distributed_table('target_table','col_1');