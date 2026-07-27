#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include "clickhouse_scanner.hpp"
#include "clickhouse_scanner_extension.hpp"
#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_secrets.hpp"
#include "clickhouse_storage.hpp"
#include "storage/clickhouse_connection_pool.hpp"
#include "storage/clickhouse_optimizer.hpp"

using namespace duckdb;

// Auto-recover from remote schema drift: a COLUMN_NOT_FOUND binder error against
// a stale cached ClickHouse catalog clears the caches and retries once, so a
// column added/dropped on the server doesn't force a manual clickhouse_clear_cache.
// Mirrors PostgresExtensionState / PostgresExtensionCallback.
class ClickHouseExtensionState : public ClientContextState {
public:
	bool CanRequestRebind() override {
		return true;
	}
	RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) override {
		if (error.Type() != ExceptionType::BINDER) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		auto &extra_info = error.ExtraInfo();
		auto entry = extra_info.find("error_subtype");
		if (entry == extra_info.end() || entry->second != "COLUMN_NOT_FOUND") {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		ClickHouseClearCacheFunction::ClearClickHouseCaches(context);
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
};

class ClickHouseExtensionCallback : public ExtensionCallback {
public:
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("clickhouse_extension",
		                                 make_shared_ptr<ClickHouseExtensionState>());
	}
};

static void SetClickHouseDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	ClickHouseConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void SetClickHouseConnectionCache(ClientContext &context, SetScope scope, Value &parameter) {
	ClickHouseConnection::SetConnectionCache(BooleanValue::Get(parameter));
}

static void LoadInternal(ExtensionLoader &loader) {
	ClickHouseScanFunction clickhouse_fun;
	loader.RegisterFunction(clickhouse_fun);

	ClickHouseScanFunctionFilterPushdown clickhouse_fun_filter_pushdown;
	loader.RegisterFunction(clickhouse_fun_filter_pushdown);

	ClickHouseQueryFunction query_func;
	loader.RegisterFunction(query_func);

	ClickHouseExecuteFunction execute_func;
	loader.RegisterFunction(execute_func);

	ClickHouseClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	ClickHouseConfigurePoolFunction configure_pool_func;
	loader.RegisterFunction(configure_pool_func);

	loader.RegisterSecretType(ClickHouseSecrets::CreateType());

	CreateSecretFunction secret_fn = {"clickhouse", "config", ClickHouseSecrets::CreateFunction};
	ClickHouseSecrets::SetSecretParameters(secret_fn);
	loader.RegisterFunction(secret_fn);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "clickhouse", make_shared_ptr<ClickHouseStorageExtension>());

	config.AddExtensionOption("ch_debug_show_queries", "DEBUG SETTING: print all queries sent to ClickHouse to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetClickHouseDebugQueryPrint);

	config.AddExtensionOption("ch_connection_cache",
	                          "Whether to reuse (pool) ClickHouse connections across statements; setting this to "
	                          "false bypasses the connection pool entirely (dynamic kill switch)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), SetClickHouseConnectionCache);

	// The pg_pool_* analogs, read when a catalog's pool is created at ATTACH; a live
	// pool is reconfigured via clickhouse_configure_pool(catalog_name := ..., ...).
	dbconnector::pool::ConnectionPoolConfig default_pool_config;
	config.AddExtensionOption(
	    "ch_pool_acquire_mode",
	    "How to acquire connections from the pool: 'force' (always connect, ignore pool limit), "
	    "'wait' (block until available), 'try' (fail immediately if unavailable) (default: force)",
	    LogicalType::VARCHAR, Value(dbconnector::pool::AcquireModeHelpers::ToString(default_pool_config.acquire_mode)),
	    ClickHouseConnectionPool::ValidatePoolAcquireMode, SetScope::GLOBAL);
	config.AddExtensionOption("ch_pool_max_connections",
	                          "Maximum number of connections that are allowed to be cached in a connection pool for "
	                          "each attached ClickHouse database",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_connections), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("ch_pool_wait_timeout_millis",
	                          "Maximum number of milliseconds to wait when acquiring a connection from a pool where "
	                          "all available connections are already taken",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.wait_timeout_millis), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("ch_pool_enable_thread_local_cache",
	                          "Whether to enable the connection caching in thread-local cache. Such connections are "
	                          "pinned to their threads and are not made available to other threads, while still "
	                          "taking a place in the pool",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(default_pool_config.tl_cache_enabled), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("ch_pool_max_lifetime_millis",
	                          "Maximum number of milliseconds a connection can be kept open; checked on lease/return "
	                          "and, with the reaper thread enabled, in the background",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_lifetime_millis), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("ch_pool_idle_timeout_millis",
	                          "Maximum number of milliseconds a connection can sit idle in the pool; checked on lease "
	                          "and, with the reaper thread enabled, in the background",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.idle_timeout_millis), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("ch_pool_enable_reaper_thread",
	                          "Whether to run the pool reaper thread that periodically closes connections exceeding "
	                          "'ch_pool_max_lifetime_millis' or 'ch_pool_idle_timeout_millis'",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(default_pool_config.start_reaper_thread), nullptr,
	                          SetScope::GLOBAL);

	// Read from the ClientContext at describe/catalog-load time; changing it clears
	// the attached catalogs' cached column types (the pg_array_as_varchar pattern),
	// so both ad-hoc scans and attached tables re-describe with the new mapping.
	config.AddExtensionOption("ch_binary_as_blob",
	                          "Read ClickHouse String/FixedString columns as BLOB instead of VARCHAR",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false),
	                          ClickHouseClearCacheFunction::ClearCacheOnSetting);

	// Read by the shared OrderByAndLimitOptimizer config (the pg_order_pushdown analog).
	config.AddExtensionOption("ch_order_pushdown",
	                          "Push ORDER BY and LIMIT clauses to ClickHouse (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	// Read at GetScanFunction time (the pg_experimental_filter_pushdown analog).
	config.AddExtensionOption("ch_experimental_filter_pushdown",
	                          "Whether or not to use filter pushdown", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));

	// Per-query max execution time applied to scan + execute (the pg_statement_
	// timeout_millis analog). NULL/0 = unbounded. Rendered as ClickHouse's
	// SETTINGS max_execution_time (seconds) by ClickHouseConnection::MakeQuery.
	config.AddExtensionOption("ch_statement_timeout_millis",
	                          "Maximum milliseconds a ClickHouse scan/execute query may run (0 = unlimited)",
	                          LogicalType::UBIGINT, Value());

	// Shared dbconnector ORDER BY / LIMIT / TOP_N pushdown (with CH safety vetoes).
	OptimizerExtension clickhouse_optimizer;
	clickhouse_optimizer.optimize_function = ClickHouseOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(clickhouse_optimizer));

	// Rebind-on-stale-schema recovery (registers ClickHouseExtensionState per connection).
	ExtensionCallback::Register(config, make_shared_ptr<ClickHouseExtensionCallback>());
}

void ClickhouseScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(clickhouse_scanner, loader) {
	LoadInternal(loader);
}
}
