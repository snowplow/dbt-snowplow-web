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

test-all: test-redshift test-bigquery
	echo "Completed successfully"
