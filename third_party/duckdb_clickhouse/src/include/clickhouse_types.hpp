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

//! Escape embedded single quotes (doubled) WITHOUT surrounding quotes, for a name
//! interpolated into a metadata query the caller already quotes (... = '<here>').
std::string ClickHouseEscapeSingleQuotes(const std::string &value);

//! Reverse type map: a DuckDB LogicalType rendered as a ClickHouse column type for DDL
//! (CREATE TABLE / CREATE TABLE AS). When `nullable` is set the result is wrapped in
//! Nullable(...). Throws NotImplementedException for types with no ClickHouse equivalent.
std::string LogicalTypeToClickHouseType(const LogicalType &type, bool nullable);

//! Build a ClickHouse column of the given (server-declared) type string and append the
//! first `count` rows of `vec` (which must already be flattened and carry the matching
//! DuckDB type). Used by the INSERT sink to turn a DataChunk into a clickhouse::Block.
clickhouse::ColumnRef ClickHouseColumnFromVector(const std::string &ch_type, Vector &vec, idx_t count);

} // namespace duckdb
