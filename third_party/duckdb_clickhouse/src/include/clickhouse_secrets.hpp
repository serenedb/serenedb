//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_secrets.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

struct ClickHouseSecrets {
	static SecretType CreateType();

	static unique_ptr<BaseSecret> CreateFunction(ClientContext &context, CreateSecretInput &input);

	static void SetSecretParameters(CreateSecretFunction &function);
};

} // namespace duckdb
