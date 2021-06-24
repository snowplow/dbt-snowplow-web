test-redshift:
	dbt seed --target redshift --full-refresh
	dbt run --target redshift --full-refresh --vars 'teardown_all: true'
	dbt run --target redshift
	dbt run --target redshift
	dbt run --target redshift
	dbt test --target redshift

test-all: test-redshift
	echo "Completed successfully"
