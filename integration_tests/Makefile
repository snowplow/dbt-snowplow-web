test-default:
	dbt seed --target default --full-refresh
	dbt run --target default --full-refresh --vars 'teardown_all: true'
	dbt run --target default
	dbt run --target default
	dbt run --target default
	dbt test --target default

test-bigquery:
	dbt seed --target bigquery --full-refresh
	dbt run --target bigquery --full-refresh --vars 'teardown_all: true'
	dbt run --target bigquery
	dbt run --target bigquery
	dbt run --target bigquery
	dbt test --target bigquery

test-snowflake:
	dbt seed --target snowflake --full-refresh
	dbt run --target snowflake --full-refresh --vars 'teardown_all: true'
	dbt run --target snowflake
	dbt run --target snowflake
	dbt run --target snowflake
	dbt test --target snowflake

test-all: test-default test-bigquery test-snowflake
	echo "Completed successfully"
