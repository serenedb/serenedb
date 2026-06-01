#!/bin/bash
# Sets sanitizer options based on environment variables and writes them to san_options.env.

set -e

SAN_COMMON_OPTIONS="log_exe_name=true:print_suppressions=0"

# LeakSanitizer (LSan) or AddressSanitizer (ASan)
if [[ "$SANITIZERS" =~ "Leak" || "$SANITIZERS" =~ "Address" ]]; then
	LSAN_OPTIONS="${SAN_COMMON_OPTIONS}:log_path=/serenedb/out/sanitizers/leak/log"
	if [ -f "resources/suppressions/lsan.txt" ]; then
		LSAN_OPTIONS="${LSAN_OPTIONS}:suppressions=/serenedb/resources/suppressions/lsan.txt"
	fi
	echo "LSAN_OPTIONS=${LSAN_OPTIONS}" >>san_options.env
fi

# UndefinedBehaviorSanitizer (UBSan)
if [[ "$SANITIZERS" =~ "Undefined" ]]; then
	UBSAN_OPTIONS="${SAN_COMMON_OPTIONS}:log_path=/serenedb/out/sanitizers/undefined/log:print_stacktrace=1"
	if [ -f "resources/suppressions/ubsan.txt" ]; then
		UBSAN_OPTIONS="${UBSAN_OPTIONS}:suppressions=/serenedb/resources/suppressions/ubsan.txt"
	fi
	echo "UBSAN_OPTIONS=${UBSAN_OPTIONS}" >>san_options.env
fi

# AddressSanitizer (ASan)
if [[ "$SANITIZERS" =~ "Address" ]]; then
	# TODO(mbkkt) fix iresearch to enable detect_container_overflow
	ASAN_OPTIONS="${SAN_COMMON_OPTIONS}:log_path=/serenedb/out/sanitizers/address/log:handle_ioctl=true:check_initialization_order=true:detect_odr_violation=1:strict_init_order=true"
	if [ -f "resources/suppressions/asan.txt" ]; then
		ASAN_OPTIONS="${ASAN_OPTIONS}:suppressions=/serenedb/resources/suppressions/asan.txt"
	fi
	echo "ASAN_OPTIONS=${ASAN_OPTIONS}" >>san_options.env
fi

# MemorySanitizer (MSan)
if [[ "$SANITIZERS" =~ "Memory" ]]; then
	MSAN_OPTIONS="${SAN_COMMON_OPTIONS}:log_path=/serenedb/out/sanitizers/memory/log:poison_in_dtor=1"
	if [ -f "resources/suppressions/msan.txt" ]; then
		MSAN_OPTIONS="${MSAN_OPTIONS}:suppressions=/serenedb/resources/suppressions/msan.txt"
	fi
	echo "MSAN_OPTIONS=${MSAN_OPTIONS}" >>san_options.env
fi

# ThreadSanitizer (TSan)
if [[ "$SANITIZERS" =~ "Thread" ]]; then
	TSAN_OPTIONS="${SAN_COMMON_OPTIONS}:log_path=/serenedb/out/sanitizers/thread/log:detect_deadlocks=true:second_deadlock_stack=1:history_size=0"
	if [ -f "resources/suppressions/tsan.txt" ]; then
		TSAN_OPTIONS="${TSAN_OPTIONS}:suppressions=/serenedb/resources/suppressions/tsan.txt"
	fi
	echo "TSAN_OPTIONS=${TSAN_OPTIONS}" >>san_options.env
fi

# Code Coverage
if [[ "$USE_COVERAGE" == "SourceBased" || "$USE_COVERAGE" == "SourceBasedMCDC" ]]; then
	echo "LLVM_PROFILE_FILE=/serenedb/out/coverage/profiles/sdb.%m.%p.profraw" >>san_options.env
fi
