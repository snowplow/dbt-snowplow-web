#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

echo "Snowplow web integration tests: Seeding data"

eval dbt seed --target $DATABASE --full-refresh

echo "Snowplow web integration tests: Execute models - run 1/4"

eval "dbt run --target $DATABASE --full-refresh --vars '{\"snowplow_teardown_all\":\"true\"}'"

for i in {2..4}
do
	echo "Snowplow web integration tests: Execute models - run $i/4"

  eval dbt run --target $DATABASE
done

echo "Snowplow web integration tests: Test models"

eval dbt test --target $DATABASE

echo "Snowplow web integration tests: All tests passed"
