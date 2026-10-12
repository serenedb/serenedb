#include "clickhouse_logging.hpp"

#include "duckdb/common/types/value.hpp"

namespace duckdb {

constexpr LogLevel ClickHouseQueryLogType::LEVEL;

string ClickHouseQueryLogType::ConstructLogMessage(const string &query, int64_t duration) {
	child_list_t<Value> child_list = {
	    {"query", query},
	    {"duration_ms", duration},
	};
	return Value::STRUCT(std::move(child_list)).ToString();
}

LogicalType ClickHouseQueryLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"query", LogicalType::VARCHAR},
	    {"duration_ms", LogicalType::BIGINT},
	};
	return LogicalType::STRUCT(child_list);
}

} // namespace duckdb
