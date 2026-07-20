//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class ClickHouseOptimizer {
public:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
