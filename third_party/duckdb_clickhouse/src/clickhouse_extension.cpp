#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include <clickhouse/client.h>

#include "clickhouse_scanner.hpp"
#include "clickhouse_scanner_extension.hpp"
#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_secrets.hpp"
#include "clickhouse_storage.hpp"

using namespace duckdb;

static std::string ClickHouseDefaultHost() {
	clickhouse::ClientOptions options = clickhouse::ClientOptions().SetHost("localhost");
	return options.host;
}

static void LoadInternal(ExtensionLoader &loader) {
	ClickHouseScanFunction clickhouse_fun;
	loader.RegisterFunction(clickhouse_fun);

	ClickHouseScanFunctionFilterPushdown clickhouse_fun_filter_pushdown;
	loader.RegisterFunction(clickhouse_fun_filter_pushdown);

	ClickHouseQueryFunction query_func;
	loader.RegisterFunction(query_func);

	loader.RegisterSecretType(ClickHouseSecrets::CreateType());

	CreateSecretFunction secret_fn = {"clickhouse", "config", ClickHouseSecrets::CreateFunction};
	ClickHouseSecrets::SetSecretParameters(secret_fn);
	loader.RegisterFunction(secret_fn);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "clickhouse", make_shared_ptr<ClickHouseStorageExtension>());

	config.AddExtensionOption("clickhouse_default_host", "Default ClickHouse host", LogicalType::VARCHAR,
	                          Value(ClickHouseDefaultHost()));
}

void ClickhouseScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(clickhouse_scanner, loader) {
	LoadInternal(loader);
}
}
