#include "duckdb.hpp"

#include "duckdb/common/string_util.hpp"

#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>

#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"
#include "clickhouse_scanner.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_transaction.hpp"

namespace duckdb {

void ClickHouseDiscoverColumns(ClickHouseConnection &connection, const std::string &describe_sql,
                               vector<LogicalType> &return_types, vector<std::string> &names, bool binary_as_blob,
                               vector<bool> &stringified, vector<std::string> &clickhouse_types);

static unique_ptr<FunctionData> ClickHouseQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<std::string> &names) {
	auto bind_data = make_uniq<ClickHouseBindData>();

	auto connection_string = input.inputs[0].GetValue<string>();
	// Accept an attached-database alias (like postgres_query) or a raw
	// key=value connection string.
	auto db = DatabaseManager::Get(context).GetDatabase(context, Identifier(connection_string));
	if (db && db->GetCatalog().GetCatalogType() == "clickhouse") {
		auto &ch_catalog = db->GetCatalog().Cast<ClickHouseCatalog>();
		bind_data->params = ch_catalog.GetConnectionParams();
		bind_data->catalog_alias = connection_string;
	} else {
		bind_data->params = ClickHouseConnectionParams::FromConnectionString(connection_string);
	}
	auto sql = input.inputs[1].GetValue<string>();
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}

	// schema_query: an optional cheaper query with the SAME result schema, used
	// only for DESCRIBE. A caller re-issuing a large-literal query per batch
	// (e.g. the external-lookup index source) passes a tiny constant variant so
	// ClickHouse does not parse the full statement twice.
	auto schema_sql = sql;
	auto schema_it = input.named_parameters.find("schema_query");
	if (schema_it != input.named_parameters.end() && !schema_it->second.IsNull()) {
		schema_sql = schema_it->second.GetValue<string>();
	}
	// external: a STRUCT of equal-length LISTs shipped with the query as the
	// native-binary temporary table `__sdb_keys` -- typed key batches with no
	// SQL-text rendering.
	auto external_it = input.named_parameters.find("external");
	if (external_it != input.named_parameters.end() && !external_it->second.IsNull()) {
		if (external_it->second.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("clickhouse_query external data must be a STRUCT of LISTs");
		}
		bind_data->external = external_it->second;
	}
	auto describe_sql = StringUtil::Format("DESCRIBE (%s)", schema_sql);

	Value binary_setting;
	bool binary_as_blob =
	    context.TryGetCurrentSetting("ch_binary_as_blob", binary_setting) && BooleanValue::Get(binary_setting);
	try {
		if (!bind_data->catalog_alias.empty()) {
			// The describe result is cached in the catalog (same trust model as its
			// table-metadata cache): a per-batch caller with a constant schema_query
			// pays the describe round trip once.
			auto &ch_catalog = db->GetCatalog().Cast<ClickHouseCatalog>();
			// Lease the transaction connection HERE, on the binding thread, even on
			// a describe-cache hit: the pool's thread-local cache then serves every
			// per-batch lease/release from one thread. Leaving the first lease to
			// the scan (a rotating executor thread) scatters connections across
			// thread-local caches and stalls the shared pool.
			auto &transaction = ClickHouseTransaction::Get(context, db->GetCatalog());
			auto &connection = transaction.GetConnection();
			const auto cache_key = StringUtil::Format("%d:%s", binary_as_blob ? 1 : 0, describe_sql);
			ClickHouseCatalog::DescribeCacheEntry cached;
			if (ch_catalog.TryGetDescribe(cache_key, cached)) {
				names = cached.names;
				return_types = cached.types;
				bind_data->stringified = std::move(cached.stringified);
				bind_data->clickhouse_types = std::move(cached.clickhouse_types);
			} else {
				ClickHouseDiscoverColumns(connection, describe_sql, return_types, names, binary_as_blob,
				                          bind_data->stringified, bind_data->clickhouse_types);
				ch_catalog.StoreDescribe(cache_key, ClickHouseCatalog::DescribeCacheEntry {
				                                        names, return_types, bind_data->stringified,
				                                        bind_data->clickhouse_types});
			}
		} else {
			auto connection = ClickHouseConnection::Open(bind_data->params);
			ClickHouseDiscoverColumns(connection, describe_sql, return_types, names, binary_as_blob,
			                          bind_data->stringified, bind_data->clickhouse_types);
		}
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("describing query", describe_sql, e);
	}

	bind_data->names = names;
	bind_data->types = return_types;
	bind_data->sql = std::move(sql);
	bind_data->from_query = true;
	return std::move(bind_data);
}

ClickHouseQueryFunction::ClickHouseQueryFunction()
    : TableFunction("clickhouse_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, ClickHouseQueryBind) {
	named_parameters["schema_query"] = LogicalType::VARCHAR;
	named_parameters["external"] = LogicalType::ANY;
	ClickHouseScanFunction scan_function;
	init_global = scan_function.init_global;
	function = scan_function.function;
	serialize = scan_function.serialize;
	deserialize = scan_function.deserialize;
	to_string = scan_function.to_string;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// clickhouse_execute
//===--------------------------------------------------------------------===//
struct ClickHouseExecuteBindData : public TableFunctionData {
	ClickHouseConnectionParams params;
	std::string sql;
};

static unique_ptr<FunctionData> ClickHouseExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<std::string> &names) {
	auto bind_data = make_uniq<ClickHouseExecuteBindData>();
	bind_data->params = ClickHouseConnectionParams::FromConnectionString(input.inputs[0].GetValue<string>());
	auto sql = input.inputs[1].GetValue<string>();
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}
	bind_data->sql = std::move(sql);
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(bind_data);
}

struct ClickHouseExecuteGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<GlobalTableFunctionState> ClickHouseExecuteInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<ClickHouseExecuteGlobalState>();
}

static void ClickHouseExecuteFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ClickHouseExecuteGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<ClickHouseExecuteBindData>();
	try {
		auto connection = ClickHouseConnection::Open(bind_data.params);
		ClickHouseConnection::LogQuery(bind_data.sql);
		connection.GetClient().Execute(ClickHouseConnection::MakeQuery(context, bind_data.sql));
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("executing statement", bind_data.sql, e);
	}
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	gstate.done = true;
}

ClickHouseExecuteFunction::ClickHouseExecuteFunction()
    : TableFunction("clickhouse_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ClickHouseExecuteFunc,
                    ClickHouseExecuteBind, ClickHouseExecuteInit) {
}

//===--------------------------------------------------------------------===//
// clickhouse_clear_cache
//===--------------------------------------------------------------------===//
struct ClickHouseClearCacheData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> ClickHouseClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<std::string> &names) {
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return make_uniq<ClickHouseClearCacheData>();
}

void ClickHouseClearCacheFunction::ClearClickHouseCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "clickhouse") {
			continue;
		}
		catalog.Cast<ClickHouseCatalog>().ClearCache();
	}
}

void ClickHouseClearCacheFunction::ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ClickHouseClearCacheFunction::ClearClickHouseCaches(context);
}

static void ClickHouseClearCacheFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClickHouseClearCacheData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	ClickHouseClearCacheFunction::ClearClickHouseCaches(context);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	data.finished = true;
}

ClickHouseClearCacheFunction::ClickHouseClearCacheFunction()
    : TableFunction("clickhouse_clear_cache", {}, ClickHouseClearCacheFunc, ClickHouseClearCacheBind) {
}

} // namespace duckdb
