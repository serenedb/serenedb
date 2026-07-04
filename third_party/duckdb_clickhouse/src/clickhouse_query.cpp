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

namespace duckdb {

void ClickHouseDiscoverColumns(ClickHouseConnection &connection, const std::string &describe_sql,
                               vector<LogicalType> &return_types, vector<std::string> &names, bool binary_as_blob,
                               vector<bool> &stringified, vector<std::string> &clickhouse_types);

static unique_ptr<FunctionData> ClickHouseQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<std::string> &names) {
	auto bind_data = make_uniq<ClickHouseBindData>();

	auto connection_string = input.inputs[0].GetValue<string>();
	bind_data->params = ClickHouseConnectionParams::FromConnectionString(connection_string);
	auto sql = input.inputs[1].GetValue<string>();
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}

	auto describe_sql = StringUtil::Format("DESCRIBE (%s)", sql);

	Value binary_setting;
	bool binary_as_blob =
	    context.TryGetCurrentSetting("ch_binary_as_blob", binary_setting) && BooleanValue::Get(binary_setting);
	try {
		auto connection = ClickHouseConnection::Open(bind_data->params);
		ClickHouseDiscoverColumns(connection, describe_sql, return_types, names, binary_as_blob,
		                          bind_data->stringified, bind_data->clickhouse_types);
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
		connection.GetClient().Execute(bind_data.sql);
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

static void ClickHouseClearCacheFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClickHouseClearCacheData>();
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "clickhouse") {
			continue;
		}
		catalog.Cast<ClickHouseCatalog>().ClearCache();
	}
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	data.finished = true;
}

ClickHouseClearCacheFunction::ClickHouseClearCacheFunction()
    : TableFunction("clickhouse_clear_cache", {}, ClickHouseClearCacheFunc, ClickHouseClearCacheBind) {
}

} // namespace duckdb
