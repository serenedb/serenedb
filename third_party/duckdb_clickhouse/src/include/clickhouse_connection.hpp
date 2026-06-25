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

	bool IsOpen() const {
		return client != nullptr;
	}

private:
	explicit ClickHouseConnection(std::unique_ptr<clickhouse::Client> client_p);

	std::unique_ptr<clickhouse::Client> client;
};

} // namespace duckdb
