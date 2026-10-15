#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"

namespace duckdb {

class ClickHouseQueryLogType : public LogType {
public:
	static constexpr const char *NAME = "ClickHouseQueryLog";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	ClickHouseQueryLogType() : LogType(NAME, LEVEL, GetLogType()) {};

	static string ConstructLogMessage(const string &query, int64_t duration);
	static LogicalType GetLogType();
};

} // namespace duckdb
