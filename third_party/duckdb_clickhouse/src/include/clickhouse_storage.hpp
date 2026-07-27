//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class ClickHouseStorageExtension : public StorageExtension {
public:
	ClickHouseStorageExtension();
};

} // namespace duckdb
