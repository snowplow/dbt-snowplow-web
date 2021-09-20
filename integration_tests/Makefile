test-redshift:
	dbt seed --target redshift --full-refresh
	dbt run --target redshift --full-refresh --vars 'teardown_all: true'
	dbt run --target redshift
	dbt run --target redshift
	dbt run --target redshift
	dbt test --target redshift

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

test-all: test-redshift test-bigquery test-snowflake
	echo "Completed successfully"
