#!/bin/bash

if ! docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-v "${WORKSPACE}:/serenedb" \
	-v /mnt/data/.ccache:/.ccache \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    {
      cmake -S tests/re2 -B build_re2 -GNinja \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_C_COMPILER=/usr/local/bin/clang \
        -DCMAKE_CXX_COMPILER=/usr/local/bin/clang++ \
        -DCMAKE_EXE_LINKER_FLAGS=-fuse-ld=lld &&
        cmake --build build_re2 &&
        ctest --test-dir build_re2 -j "$(nproc)" --output-on-failure \
          --output-junit /serenedb/out/test-results/re2-tests.xml
    } 2>&1 | tee -a /serenedb/out/logs/re2-tests.log
  '; then
	echo "RE2_TESTS=FAILED"
	exit 123
fi
echo "RE2_TESTS=PASSED"
