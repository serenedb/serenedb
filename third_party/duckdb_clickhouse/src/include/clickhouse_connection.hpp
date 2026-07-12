//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <clickhouse/client.h>

#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {

struct ClickHouseConnectionParams {
	std::string host;
	uint16_t port = 9000;
	std::string user = "default";
	std::string password;
	std::string database = "default";
	bool secure = false;
	clickhouse::CompressionMethod compression = clickhouse::CompressionMethod::LZ4;

	static ClickHouseConnectionParams FromConnectionString(const std::string &connection_string);
	clickhouse::ClientOptions ToClientOptions() const;
};

class ClickHouseConnection {
public:
	ClickHouseConnection() = default;
	~ClickHouseConnection() = default;
	// disable copy constructors
	ClickHouseConnection(const ClickHouseConnection &other) = delete;
	ClickHouseConnection &operator=(const ClickHouseConnection &) = delete;
	//! enable move constructors
	ClickHouseConnection(ClickHouseConnection &&other) noexcept = default;
	ClickHouseConnection &operator=(ClickHouseConnection &&) noexcept = default;

public:
	static ClickHouseConnection Open(const ClickHouseConnectionParams &params);

	clickhouse::Client &GetClient();

	//! Wrap `sql` in a clickhouse::Query carrying the session's per-query settings:
	//! ch_statement_timeout_millis -> SETTINGS max_execution_time (ClickHouse's
	//! bound is in fractional seconds). The postgres analog is the SET
	//! statement_timeout applied to scan connections. Used on the scan and execute
	//! paths (not catalog-metadata queries, matching postgres).
	static clickhouse::Query MakeQuery(duckdb::ClientContext &context, const std::string &sql);

	//! Print `sql` to stdout when ch_debug_show_queries is enabled. Called at every
	//! query-emit site (the postgres pg_debug_show_queries analog; there is no single
	//! choke point because callers drive clickhouse::Client directly).
	static void LogQuery(const std::string &sql);
	//! Uniform error translation: throw an IOException naming the failed operation and
	//! carrying the SQL that failed (so errors are debuggable without re-running under
	//! ch_debug_show_queries).
	[[noreturn]] static void ThrowError(const char *op, const std::string &sql, const std::exception &error);
	static void DebugSetPrintQueries(bool print);

	//! Whether idle connections are cached for reuse across transactions/scans
	//! (ch_connection_cache; on by default). When off, every lease opens a fresh
	//! connection and returns drop immediately.
	static void SetConnectionCache(bool enabled);
	static bool ConnectionCacheEnabled();

	bool IsOpen() const {
		return client != nullptr;
	}

private:
	explicit ClickHouseConnection(std::unique_ptr<clickhouse::Client> client_p);

	std::unique_ptr<clickhouse::Client> client;
};

} // namespace duckdb
