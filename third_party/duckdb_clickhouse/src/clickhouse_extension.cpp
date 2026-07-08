#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include "clickhouse_scanner.hpp"
#include "clickhouse_scanner_extension.hpp"
#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_secrets.hpp"
#include "clickhouse_storage.hpp"
#include "storage/clickhouse_optimizer.hpp"

using namespace duckdb;

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

	loader.RegisterSecretType(ClickHouseSecrets::CreateType());

	CreateSecretFunction secret_fn = {"clickhouse", "config", ClickHouseSecrets::CreateFunction};
	ClickHouseSecrets::SetSecretParameters(secret_fn);
	loader.RegisterFunction(secret_fn);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "clickhouse", make_shared_ptr<ClickHouseStorageExtension>());

	config.AddExtensionOption("ch_debug_show_queries", "DEBUG SETTING: print all queries sent to ClickHouse to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetClickHouseDebugQueryPrint);

	config.AddExtensionOption("ch_connection_cache",
	                          "Whether to reuse (pool) ClickHouse connections across statements",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), SetClickHouseConnectionCache);

	// Session-scoped (no static callback): read from ClientContext at describe time so it
	// is isolated per connection, not shared process-wide (which would change column types
	// under other sessions).
	config.AddExtensionOption("ch_binary_as_blob",
	                          "Read ClickHouse String/FixedString columns as BLOB instead of VARCHAR",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Read by the shared OrderByAndLimitOptimizer config (the pg_order_pushdown analog).
	config.AddExtensionOption("ch_order_pushdown",
	                          "Push ORDER BY and LIMIT clauses to ClickHouse (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	// Read at GetScanFunction time (the pg_experimental_filter_pushdown analog).
	config.AddExtensionOption("ch_experimental_filter_pushdown",
	                          "Whether or not to use filter pushdown", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));

	// Shared dbconnector ORDER BY / LIMIT / TOP_N pushdown (with CH safety vetoes).
	OptimizerExtension clickhouse_optimizer;
	clickhouse_optimizer.optimize_function = ClickHouseOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(clickhouse_optimizer));
}

void ClickhouseScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(clickhouse_scanner, loader) {
	LoadInternal(loader);
}
}
