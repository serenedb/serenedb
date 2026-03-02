// Stubs for AWS SDK symbols that may be missing in the monolithic build.
// Following ClickHouse's approach of providing minimal stubs for unused
// functionality.

// We don't use OpenTelemetry integration - stub out the factory function
// that sdk-core's HttpClientFactory may reference.
#include <aws/core/http/HttpClientFactory.h>

// Prevent linker issues with missing platform-specific symbols.
// This file is intentionally minimal - add stubs here only when
// the linker reports undefined symbols from unused code paths.
