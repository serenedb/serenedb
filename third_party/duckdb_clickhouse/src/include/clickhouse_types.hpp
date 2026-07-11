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

//! A fully-quoted ClickHouse string literal: single quotes, backslash-escaped
//! (delegates to the shared dbconnector QueryWriter). For value literals.
std::string ClickHouseStringLiteral(const std::string &value);

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

//! True when pushing a COMPARISON on a column of this type to ClickHouse can return a
//! different row set than DuckDB's own evaluation, so the filter must stay local:
//! FLOAT/DOUBLE (NaN semantics + Float32 literal round-trip), DATE (Date32 saturation /
//! ±infinity abort), TIMESTAMP*/TIME* (server-timezone literal parsing), UUID (internal
//! vs canonical order), and -- via `ch_type` -- Enum (ordinal vs label, unknown-label
//! abort) and IPv4/IPv6 (address vs text). `ch_type` is the server-declared type string.
bool ClickHouseComparisonUnsafe(const LogicalType &duckdb_type, const std::string &ch_type);

//! True when pushing an ORDER BY on a column of this type to ClickHouse can order rows
//! differently than DuckDB (so a TOP_N must not fold): FLOAT/DOUBLE (NaN placement),
//! UUID (internal vs canonical order), and -- via `ch_type` -- Enum and IPv4/IPv6.
//! Narrower than the comparison set: DATE/TIMESTAMP/TIME order by the stored value
//! identically in both engines (no literal parsing is involved).
bool ClickHouseOrderingUnsafe(const LogicalType &duckdb_type, const std::string &ch_type);

} // namespace duckdb
