#include "duckdb.hpp"

#include "duckdb/common/string_util.hpp"

#include <clickhouse/exceptions.h>

#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"
#include "clickhouse_scanner.hpp"

namespace duckdb {

void ClickHouseDiscoverColumns(ClickHouseConnection &connection, const std::string &describe_sql,
                               vector<LogicalType> &return_types, vector<std::string> &names);

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

	try {
		auto connection = ClickHouseConnection::Open(bind_data->params);
		ClickHouseDiscoverColumns(connection, describe_sql, return_types, names);
	} catch (const clickhouse::ServerException &e) {
		throw IOException("ClickHouse error describing query: %s", e.what());
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error describing query: %s", e.what());
	}

	bind_data->names = names;
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
	projection_pushdown = true;
}

} // namespace duckdb
