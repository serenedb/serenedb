//===----------------------------------------------------------------------===//
//                         DuckDB
//
// clickhouse_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include <clickhouse/columns/column.h>
#include <clickhouse/types/types.h>

#include <string>

namespace duckdb {

LogicalType ClickHouseToLogicalType(const clickhouse::Type &type);

LogicalType ClickHouseTypeStringToLogicalType(const std::string &type_str);

void ClickHouseColumnToVector(const clickhouse::Column &col, Vector &out, idx_t src_offset, idx_t count);

//! Quote a ClickHouse identifier with backticks, doubling any embedded backtick. Behaviour is
//! identical to raw `name` wrapping for normal names; it only matters for names containing a
//! backtick. Used wherever a column/table/PK name is interpolated into generated SQL.
std::string ClickHouseQuoteIdentifier(const std::string &name);

//! A fully-quoted ClickHouse string literal: wraps `value` in single quotes and
//! backslash-escapes embedded ' and \. For value literals (filter pushdown, UPDATE).
std::string ClickHouseStringLiteral(const std::string &value);

//! Render BLOB bytes as a ClickHouse unhex('HEX') literal so raw binary
//! round-trips exactly. Shared by filter pushdown and UPDATE literal rendering.
std::string ClickHouseBlobLiteral(const string_t &bytes);

//! Render a scalar DuckDB Value as an EXACT ClickHouse SQL literal: NULL -> NULL;
//! HUGEINT/UHUGEINT/DECIMAL via to*() casts (ClickHouse parses their bare literals as
//! Float64, losing precision); other scalar types as a quoted string ClickHouse casts
//! to the target type. Shared by filter pushdown, UPDATE assignments and DEFAULT
//! rendering so their exactness rules cannot drift apart. Throws
//! NotImplementedException for nested types.
std::string ClickHouseValueLiteral(const Value &value);

//! Reverse type map: a DuckDB LogicalType rendered as a ClickHouse column type for DDL
//! (CREATE TABLE / CREATE TABLE AS). When `nullable` is set the result is wrapped in
//! Nullable(...). Throws NotImplementedException for types with no ClickHouse equivalent.
std::string LogicalTypeToClickHouseType(const LogicalType &type, bool nullable);

//! Build a ClickHouse column of the given (server-declared) type string and append the
//! first `count` rows of `vec` (which must already be flattened and carry the matching
//! DuckDB type). Used by the INSERT sink to turn a DataChunk into a clickhouse::Block.
clickhouse::ColumnRef ClickHouseColumnFromVector(const std::string &ch_type, Vector &vec, idx_t count);

} // namespace duckdb
