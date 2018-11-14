setup
{	
	CREATE TABLE target_table(col_1 int primary key, col_2 int);
	SELECT create_distributed_table('target_table','col_1');
	INSERT INTO target_table VALUES(1,2),(2,3),(3,4),(4,5),(5,6);
	
	CREATE TABLE source_table(col_1 int, col_2 int, col_3 int);
	SELECT create_distributed_table('source_table','col_1');
	INSERT INTO source_table VALUES(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);
}

teardown
{
	DROP TABLE target_table, source_table;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-insert-into-select-conflict-update"
{
	INSERT INTO target_table
	SELECT 
		col_1, col_2
	FROM (
		SELECT 
			col_1, col_2, col_3 
		FROM
			source_table_1
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *;
}

step "s1-insert-into-select-conflict-do-nothing"
{
	INSERT INTO target_table
	SELECT 
		col_1, col_2
	FROM (
		SELECT 
			col_1, col_2, col_3 
		FROM
			source_table_1
		LIMIT 5
	) as foo
	ON CONFLICT DO NOTHING;
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-insert-into-select-conflict-update"
{
	INSERT INTO target_table
	SELECT 
		col_1, col_2
	FROM (
		SELECT 
			col_1, col_2, col_3 
		FROM
			source_table_1
		LIMIT 5
	) as foo
	ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2 RETURNING *;
}

step "s2-insert-into-select-conflict-do-nothing"
{
	INSERT INTO target_table
	SELECT 
		col_1, col_2
	FROM (
		SELECT 
			col_1, col_2, col_3 
		FROM
			source_table_1
		LIMIT 5
	) as foo
	ON CONFLICT DO NOTHING;
}

step "s2-update"
{
	UPDATE target_table SET col_2 = 5;
}

step "s2-delete"
{
	DELETE FROM target_table;
}

step "s2-commit"
{
	COMMIT;
}

permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update-ref-table" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-update-ref-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
