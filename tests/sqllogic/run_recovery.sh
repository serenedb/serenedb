#!/bin/bash

cd sqllogic

./run.sh \
  --host serenedb-single \
  --single-port 7777 \
  --test "recovery/**/*.test" \
  --junit "tests-serenedb-recovery" \
  --protocol simple \
  --runner=/sqllogictest-rs

exit_code=$?

if [[ $exit_code != 0 ]]; then
  echo "SereneDB recovery tests are failed"
  exit $exit_code
fi
