#!/bin/bash

SERENED_PORT=${SERENED_PORT:-7777}

cd sqllogic

declare -a test_files=()
while IFS= read -r -d '' file; do
  test_files+=("${file#./}")
done < <(find recovery/ -name "*.test" -type f -print0 | sort -z)

if [[ ${#test_files[@]} -eq 0 ]]; then
  echo "No test files found in recovery/ directory"
  exit 1
fi

echo "Found ${#test_files[@]} recovery test(s)"

final_exit_code=0

for test_file in "${test_files[@]}"; do
  echo "Running recovery test: $test_file"
  
  ./run.sh \
    --host serenedb-single \
    --single-port "$SERENED_PORT" \
    --test "$test_file" \
    --junit "tests-serenedb-recovery" \
    --protocol simple \
    --runner=/sqllogictest-rs
  
  exit_code=$?
  
  if [[ $exit_code != 0 ]]; then
    echo "SereneDB recovery test failed: $test_file"
    final_exit_code=$exit_code
  fi
done

if [[ $final_exit_code != 0 ]]; then
  echo "SereneDB recovery tests failed"
  exit $final_exit_code
fi

echo "All recovery tests passed"
exit 0
