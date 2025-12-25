#!/bin/bash

exit_code=0

cd sqllogic

echo "Running tests against SereneDB"
./run.sh \
  --host serenedb-single \
  --single-port 7777 \
  --test "sdb/**/*.test" \
  --junit "tests-serenedb" \
  --protocol both \
  --runner=/sqllogictest-rs || exit_code=$?

echo "Running tests against PostgreSQL"
./run.sh \
  --host postgres \
  --single-port 5432 \
  --test "pg/**/*.test" \
  --junit "tests-postgres" \
  --protocol both \
  --database postgres \
  --runner=/sqllogictest-rs || exit_code=$?

exit $exit_code
