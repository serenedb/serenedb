#include "storage/clickhouse_connection_pool.hpp"

#include <chrono>

#include "storage/clickhouse_catalog.hpp"

namespace duckdb {

static dbconnector::pool::ConnectionPoolConfig CreateConfig(ClientContext &ctx) {
	dbconnector::pool::ConnectionPoolConfig config;

	{
		Value mode_val;
		if (ctx.TryGetCurrentSetting("ch_pool_acquire_mode", mode_val)) {
			config.acquire_mode = dbconnector::pool::AcquireModeHelpers::FromString(mode_val.ToString());
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("ch_pool_max_connections", val) && !val.IsNull()) {
			config.max_connections = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("ch_pool_wait_timeout_millis", val) && !val.IsNull()) {
			config.wait_timeout_millis = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("ch_pool_enable_thread_local_cache", val) && !val.IsNull()) {
			config.tl_cache_enabled = BooleanValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("ch_pool_max_lifetime_millis", val) && !val.IsNull()) {
			config.max_lifetime_millis = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("ch_pool_idle_timeout_millis", val) && !val.IsNull()) {
			config.idle_timeout_millis = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("ch_pool_enable_reaper_thread", val) && !val.IsNull()) {
			config.start_reaper_thread = BooleanValue::Get(val);
		}
	}

	return config;
}

ClickHouseConnectionPool::ClickHouseConnectionPool(ClickHouseCatalog &clickhouse_catalog, ClientContext &context)
    : dbconnector::pool::ConnectionPool<ClickHouseConnection>(CreateConfig(context)),
      clickhouse_catalog(clickhouse_catalog) {
}

ClickHousePoolConnection ClickHouseConnectionPool::GetConnection() {
	if (!ClickHouseConnection::ConnectionCacheEnabled()) {
		// Detached (null pool): released connections just close.
		return ClickHousePoolConnection(nullptr, CreateNewConnection(), std::chrono::steady_clock::now());
	}
	return Acquire();
}

std::unique_ptr<ClickHouseConnection> ClickHouseConnectionPool::CreateNewConnection() {
	auto conn = ClickHouseConnection::Open(clickhouse_catalog.GetConnectionParams());
	return make_uniq<ClickHouseConnection>(std::move(conn));
}

bool ClickHouseConnectionPool::CheckConnectionHealthy(ClickHouseConnection &conn) {
	if (!conn.IsOpen()) {
		return false;
	}
	try {
		conn.GetClient().Ping();
		return true;
	} catch (...) {
		// A dead or protocol-desynced connection: discard, the pool opens a fresh one.
		return false;
	}
}

void ClickHouseConnectionPool::ResetConnection(ClickHouseConnection &conn) {
	// No session state to reset between discrete native-protocol queries.
}

void ClickHouseConnectionPool::ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter) {
	dbconnector::pool::AcquireMode mode = dbconnector::pool::AcquireModeHelpers::FromString(parameter.ToString());
	if (mode != dbconnector::pool::AcquireMode::FORCE) {
		Value pool_size_val;
		if (context.TryGetCurrentSetting("ch_pool_max_connections", pool_size_val)) {
			auto pool_size = pool_size_val.GetValue<uint64_t>();
			if (pool_size == 0) {
				std::string mode_str = dbconnector::pool::AcquireModeHelpers::ToString(mode);
				throw InvalidInputException(
				    "ch_pool_acquire_mode='%s' requires ch_pool_max_connections > 0 (pooling enabled)", mode_str);
			}
		}
	}
}

} // namespace duckdb
