#include "duckdb.hpp"

#include "dbconnector/query/query_writer.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/json.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/types/types.h>

#include "clickhouse_types.hpp"

#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

namespace duckdb {

static int64_t Pow10(int n) {
  int64_t result = 1;
  for (int i = 0; i < n; i++) {
    result *= 10;
  }
  return result;
}

// Floor division: negative values round toward -infinity, so a pre-epoch
// DateTime64 instant maps into the microsecond/second CONTAINING it (plain /
// truncates toward zero, mapping e.g. -0.1s to second 0 -- one unit late and
// past the pre-epoch range guards).
static int64_t FloorDiv(int64_t value, int64_t divisor) {
  return value / divisor -
         ((value % divisor != 0 && (value ^ divisor) < 0) ? 1 : 0);
}

// ClickHouse String carries raw bytes while DuckDB VARCHAR requires UTF-8.
// Both decode paths (bulk scan and per-row lookup) must agree on the same
// bytes, so both reject invalid payloads with the same escape hatch.
static void VerifyStringUtf8(const char *data, size_t size) {
  if (Utf8Proc::Analyze(data, size) == UnicodeType::INVALID) {
    throw InvalidInputException(
        "ClickHouse String value contains invalid UTF-8; set "
        "ch_binary_as_blob=true to read String columns as BLOB");
  }
}

// ClickHouse DateTime64(p) stores an integer tick count at 10^-p seconds;
// DuckDB TIMESTAMP is microseconds (10^-6). Shared by the scalar and bulk decode.
static int64_t DateTime64TicksToMicros(int64_t ticks, int precision) {
  return (precision <= 6) ? ticks * Pow10(6 - precision) : FloorDiv(ticks, Pow10(precision - 6));
}

static hugeint_t ClickHouseInt128ToHugeint(const clickhouse::Int128 &value) {
  return hugeint_t(static_cast<int64_t>(absl::Int128High64(value)), absl::Int128Low64(value));
}

static uhugeint_t ClickHouseUInt128ToUhugeint(const clickhouse::UInt128 &value) {
  return uhugeint_t(absl::Uint128High64(value), absl::Uint128Low64(value));
}

static LogicalType ClickHouseToLogicalType(const clickhouse::Type &type) {
  switch (type.GetCode()) {
  case clickhouse::Type::UInt8:
    return LogicalType::UTINYINT;
  case clickhouse::Type::UInt16:
    return LogicalType::USMALLINT;
  case clickhouse::Type::UInt32:
    return LogicalType::UINTEGER;
  case clickhouse::Type::UInt64:
    return LogicalType::UBIGINT;
  case clickhouse::Type::Int8:
    return LogicalType::TINYINT;
  case clickhouse::Type::Int16:
    return LogicalType::SMALLINT;
  case clickhouse::Type::Int32:
    return LogicalType::INTEGER;
  case clickhouse::Type::Int64:
    return LogicalType::BIGINT;
  case clickhouse::Type::Int128:
    return LogicalType::HUGEINT;
  case clickhouse::Type::UInt128:
    return LogicalType::UHUGEINT;
  case clickhouse::Type::Float32:
    return LogicalType::FLOAT;
  case clickhouse::Type::Float64:
    return LogicalType::DOUBLE;
  case clickhouse::Type::Bool:
    return LogicalType::BOOLEAN;
  case clickhouse::Type::String:
  case clickhouse::Type::FixedString:
    return LogicalType::VARCHAR;
  case clickhouse::Type::Date:
  case clickhouse::Type::Date32:
    return LogicalType::DATE;
  case clickhouse::Type::DateTime:
  case clickhouse::Type::DateTime64:
    return LogicalType::TIMESTAMP;
  case clickhouse::Type::Decimal:
  case clickhouse::Type::Decimal32:
  case clickhouse::Type::Decimal64:
  case clickhouse::Type::Decimal128: {
    auto &decimal_type = *type.As<clickhouse::DecimalType>();
    auto precision = decimal_type.GetPrecision();
    if (precision > 38) {
      // No native read: clickhouse-cpp stores decimals as Int128 and would read
      // 16 bytes/row of a 32-byte Decimal256, desyncing the whole block. The
      // scanner catches this and degrades the column to a toString() projection.
      throw NotImplementedException("ClickHouse Decimal precision %d exceeds DuckDB's maximum of 38",
                                    static_cast<int>(precision));
    }
    return LogicalType::DECIMAL(static_cast<uint8_t>(precision), static_cast<uint8_t>(decimal_type.GetScale()));
  }
  case clickhouse::Type::Enum8:
  case clickhouse::Type::Enum16:
    return LogicalType::VARCHAR;
  case clickhouse::Type::UUID:
    return LogicalType::UUID;
  case clickhouse::Type::IPv4:
  case clickhouse::Type::IPv6:
    return LogicalType::VARCHAR;
  case clickhouse::Type::Array: {
    auto &array_type = *type.As<clickhouse::ArrayType>();
    return LogicalType::LIST(ClickHouseToLogicalType(*array_type.GetItemType()));
  }
  case clickhouse::Type::Map: {
    auto &map_type = *type.As<clickhouse::MapType>();
    return LogicalType::MAP(ClickHouseToLogicalType(*map_type.GetKeyType()),
                            ClickHouseToLogicalType(*map_type.GetValueType()));
  }
  case clickhouse::Type::Tuple: {
    auto &tuple_type = *type.As<clickhouse::TupleType>();
    auto item_types = tuple_type.GetTupleType();
    auto &item_names = tuple_type.GetItemNames();
    child_list_t<LogicalType> children;
    for (idx_t i = 0; i < item_types.size(); i++) {
      std::string name = i < item_names.size() ? item_names[i] : "entry_" + std::to_string(i);
      children.push_back(make_pair(Identifier(std::move(name)), ClickHouseToLogicalType(*item_types[i])));
    }
    return LogicalType::STRUCT(std::move(children));
  }
  case clickhouse::Type::Nullable: {
    auto &nullable_type = *type.As<clickhouse::NullableType>();
    return ClickHouseToLogicalType(*nullable_type.GetNestedType());
  }
  case clickhouse::Type::LowCardinality: {
    auto &lc_type = *type.As<clickhouse::LowCardinalityType>();
    return ClickHouseToLogicalType(*lc_type.GetNestedType());
  }
  case clickhouse::Type::JSON:
    return LogicalType::VARCHAR;
  case clickhouse::Type::Void:
    return LogicalType::SQLNULL;
  default:
    throw NotImplementedException("Unsupported ClickHouse type: %s", type.GetName());
  }
}

LogicalType ClickHouseTypeStringToLogicalType(const std::string &type_str) {
  // LowCardinality is a storage-only wrapper that maps to the same LogicalType as its
  // inner type, so strip it before constructing a column. CreateColumnByType on a
  // top-level LowCardinality(Nullable(<numeric>)) throws while building its
  // null-placeholder dictionary (GetNullItemForDictionary constructs a zero-byte
  // numeric ItemView) -- a clickhouse-cpp quirk -- and would make every column of a
  // table that has such a column unreadable at bind time.
  static const std::string lc_prefix = "LowCardinality(";
  if (type_str.rfind(lc_prefix, 0) == 0 && type_str.back() == ')') {
    return ClickHouseTypeStringToLogicalType(
        type_str.substr(lc_prefix.size(), type_str.size() - lc_prefix.size() - 1));
  }
  auto column = clickhouse::CreateColumnByType(type_str);
  if (!column) {
    throw NotImplementedException("Unsupported ClickHouse type: %s", type_str);
  }
  return ClickHouseToLogicalType(*column->Type());
}

static Value ClickHouseColumnValueAt(const clickhouse::Column &col, idx_t row);

template <class CHColumn>
static Value StringValueAt(const clickhouse::Column &col, idx_t row) {
  auto view = col.As<CHColumn>()->At(row);
  VerifyStringUtf8(view.data(), view.size());
  return Value(std::string(view.data(), view.size()));
}

// NameAt() does std::map::at on the ordinal -> throws (aborting the whole scan)
// if the stored ordinal is outside the current Enum definition (e.g. after an
// ALTER). Read the raw ordinal and only resolve the name when it is defined.
template <class CHColumn>
static Value EnumValueAt(const clickhouse::Column &col, idx_t row) {
  int16_t ordinal = col.As<CHColumn>()->At(row);
  auto enum_type = col.Type()->As<clickhouse::EnumType>();
  if (enum_type && enum_type->HasEnumValue(ordinal)) {
    return Value(std::string(enum_type->GetEnumName(ordinal)));
  }
  return Value(std::to_string(ordinal));
}

static Value ClickHouseScalarValueAt(const clickhouse::Column &col, idx_t row) {
  switch (col.Type()->GetCode()) {
  case clickhouse::Type::UInt8:
    return Value::UTINYINT(col.As<clickhouse::ColumnUInt8>()->At(row));
  case clickhouse::Type::UInt16:
    return Value::USMALLINT(col.As<clickhouse::ColumnUInt16>()->At(row));
  case clickhouse::Type::UInt32:
    return Value::UINTEGER(col.As<clickhouse::ColumnUInt32>()->At(row));
  case clickhouse::Type::UInt64:
    return Value::UBIGINT(col.As<clickhouse::ColumnUInt64>()->At(row));
  case clickhouse::Type::Int8:
    return Value::TINYINT(col.As<clickhouse::ColumnInt8>()->At(row));
  case clickhouse::Type::Int16:
    return Value::SMALLINT(col.As<clickhouse::ColumnInt16>()->At(row));
  case clickhouse::Type::Int32:
    return Value::INTEGER(col.As<clickhouse::ColumnInt32>()->At(row));
  case clickhouse::Type::Int64:
    return Value::BIGINT(col.As<clickhouse::ColumnInt64>()->At(row));
  case clickhouse::Type::Int128:
    return Value::HUGEINT(ClickHouseInt128ToHugeint(col.As<clickhouse::ColumnInt128>()->At(row)));
  case clickhouse::Type::UInt128:
    return Value::UHUGEINT(ClickHouseUInt128ToUhugeint(col.As<clickhouse::ColumnUInt128>()->At(row)));
  case clickhouse::Type::Float32:
    return Value::FLOAT(col.As<clickhouse::ColumnFloat32>()->At(row));
  case clickhouse::Type::Float64:
    return Value::DOUBLE(col.As<clickhouse::ColumnFloat64>()->At(row));
  case clickhouse::Type::Bool:
    return Value::BOOLEAN(col.As<clickhouse::ColumnBool>()->At(row));
  case clickhouse::Type::String:
    return StringValueAt<clickhouse::ColumnString>(col, row);
  case clickhouse::Type::FixedString:
    return StringValueAt<clickhouse::ColumnFixedString>(col, row);
  case clickhouse::Type::Date:
    return Value::DATE(date_t(static_cast<int32_t>(col.As<clickhouse::ColumnDate>()->RawAt(row))));
  case clickhouse::Type::Date32:
    return Value::DATE(date_t(col.As<clickhouse::ColumnDate32>()->RawAt(row)));
  case clickhouse::Type::DateTime: {
    auto seconds = static_cast<int64_t>(col.As<clickhouse::ColumnDateTime>()->RawAt(row));
    return Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(seconds * 1000000));
  }
  case clickhouse::Type::DateTime64: {
    auto datetime = col.As<clickhouse::ColumnDateTime64>();
    auto ticks = datetime->At(row);
    auto precision = static_cast<int>(datetime->GetPrecision());
    return Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(DateTime64TicksToMicros(ticks, precision)));
  }
  case clickhouse::Type::Decimal:
  case clickhouse::Type::Decimal32:
  case clickhouse::Type::Decimal64:
  case clickhouse::Type::Decimal128: {
    auto decimal = col.As<clickhouse::ColumnDecimal>();
    auto unscaled = decimal->At(row);
    auto precision = decimal->GetPrecision();
    auto scale = decimal->GetScale();
    if (precision <= 18) {
      return Value::DECIMAL(static_cast<int64_t>(unscaled), static_cast<uint8_t>(precision),
                            static_cast<uint8_t>(scale));
    }
    return Value::DECIMAL(ClickHouseInt128ToHugeint(unscaled), static_cast<uint8_t>(precision),
                          static_cast<uint8_t>(scale));
  }
  case clickhouse::Type::Enum8:
    return EnumValueAt<clickhouse::ColumnEnum8>(col, row);
  case clickhouse::Type::Enum16:
    return EnumValueAt<clickhouse::ColumnEnum16>(col, row);
  case clickhouse::Type::UUID: {
    auto uuid = col.As<clickhouse::ColumnUUID>()->At(row);
    return Value::UUID(hugeint_t(static_cast<int64_t>(uuid.first ^ (static_cast<uint64_t>(1) << 63)), uuid.second));
  }
  case clickhouse::Type::IPv4:
    return Value(col.As<clickhouse::ColumnIPv4>()->AsString(row));
  case clickhouse::Type::IPv6:
    return Value(col.As<clickhouse::ColumnIPv6>()->AsString(row));
  case clickhouse::Type::JSON:
    // ColumnJSON is a sibling of ColumnString (both derive from Column), not a
    // subclass, so As<ColumnString>() would return nullptr and ->At() segfault.
    return Value(std::string(col.As<clickhouse::ColumnJSON>()->At(row)));
  case clickhouse::Type::Void:
    return Value();
  default:
    throw NotImplementedException("Unsupported ClickHouse type: %s", col.Type()->GetName());
  }
}

static Value ClickHouseColumnValueAt(const clickhouse::Column &col, idx_t row) {
  switch (col.Type()->GetCode()) {
  case clickhouse::Type::Nullable: {
    auto nullable = col.As<clickhouse::ColumnNullable>();
    auto nested = nullable->Nested();
    if (nullable->IsNull(row)) {
      return Value(ClickHouseToLogicalType(*nested->Type()));
    }
    return ClickHouseColumnValueAt(*nested, row);
  }
  case clickhouse::Type::LowCardinality: {
    auto lc = col.As<clickhouse::ColumnLowCardinality>();
    auto nested_type = lc->GetNestedType();
    bool nested_nullable = nested_type->GetCode() == clickhouse::Type::Nullable;
    if (nested_nullable) {
      nested_type = nested_type->As<clickhouse::NullableType>()->GetNestedType();
    }
    auto item = lc->GetItem(row);
    if (nested_nullable && item.type == clickhouse::Type::Void) {
      return Value(ClickHouseToLogicalType(*nested_type));
    }
    auto view = item.AsBinaryData();
    switch (nested_type->GetCode()) {
    case clickhouse::Type::String:
    case clickhouse::Type::FixedString: {
      // The NULL placeholder of an LC(Nullable(String)) is a nullptr view (an
      // empty string keeps a valid pointer).
      if (nested_nullable && view.data() == nullptr) {
        return Value(ClickHouseToLogicalType(*nested_type));
      }
      VerifyStringUtf8(view.data(), view.size());
      return Value(std::string(view.data(), view.size()));
    }
    default:
      break;
    }
    // Fixed-width payloads: clickhouse-cpp materializes the NULL placeholder of
    // an LC(Nullable(T)) as a ZERO-BYTE view (get<T>'s size validator would
    // throw on it); detect it structurally instead of catching, so a genuine
    // decode error on a non-null row still surfaces.
    if (nested_nullable && view.size() == 0) {
      return Value(ClickHouseToLogicalType(*nested_type));
    }
    switch (nested_type->GetCode()) {
    case clickhouse::Type::UInt8:
      return Value::UTINYINT(item.get<uint8_t>());
    case clickhouse::Type::UInt16:
      return Value::USMALLINT(item.get<uint16_t>());
    case clickhouse::Type::UInt32:
      return Value::UINTEGER(item.get<uint32_t>());
    case clickhouse::Type::UInt64:
      return Value::UBIGINT(item.get<uint64_t>());
    case clickhouse::Type::Int8:
      return Value::TINYINT(item.get<int8_t>());
    case clickhouse::Type::Int16:
      return Value::SMALLINT(item.get<int16_t>());
    case clickhouse::Type::Int32:
      return Value::INTEGER(item.get<int32_t>());
    case clickhouse::Type::Int64:
      return Value::BIGINT(item.get<int64_t>());
    case clickhouse::Type::Float32:
      return Value::FLOAT(item.get<float>());
    case clickhouse::Type::Float64:
      return Value::DOUBLE(item.get<double>());
    case clickhouse::Type::Date:
      return Value::DATE(date_t(static_cast<int32_t>(item.get<uint16_t>())));
    case clickhouse::Type::Date32:
      return Value::DATE(date_t(item.get<int32_t>()));
    case clickhouse::Type::DateTime:
      return Value::TIMESTAMP(
          Timestamp::FromEpochMicroSeconds(static_cast<int64_t>(item.get<uint32_t>()) * 1000000));
    default:
      throw NotImplementedException("Unsupported ClickHouse LowCardinality nested type: %s", nested_type->GetName());
    }
  }
  case clickhouse::Type::Array: {
    auto array = col.As<clickhouse::ColumnArray>();
    auto child = array->GetData();
    auto offset = array->GetOffset(row);
    auto size = array->GetSize(row);
    auto child_type = ClickHouseToLogicalType(*child->Type());
    vector<Value> values;
    values.reserve(size);
    for (idx_t i = 0; i < size; i++) {
      values.push_back(ClickHouseColumnValueAt(*child, offset + i));
    }
    return Value::LIST(child_type, std::move(values));
  }
  case clickhouse::Type::Tuple: {
    auto tuple = col.As<clickhouse::ColumnTuple>();
    auto &tuple_type = *col.Type()->As<clickhouse::TupleType>();
    auto &item_names = tuple_type.GetItemNames();
    child_list_t<Value> children;
    for (idx_t i = 0; i < tuple->TupleSize(); i++) {
      std::string name = i < item_names.size() ? item_names[i] : "entry_" + std::to_string(i);
      children.push_back(make_pair(Identifier(std::move(name)), ClickHouseColumnValueAt(*tuple->At(i), row)));
    }
    return Value::STRUCT(std::move(children));
  }
  case clickhouse::Type::Map: {
    auto map = col.As<clickhouse::ColumnMap>();
    auto &map_type = *col.Type()->As<clickhouse::MapType>();
    auto key_type = ClickHouseToLogicalType(*map_type.GetKeyType());
    auto value_type = ClickHouseToLogicalType(*map_type.GetValueType());
    auto entries = map->GetAsColumn(row);
    auto tuple = entries->As<clickhouse::ColumnTuple>();
    auto keys_col = tuple->At(0);
    auto values_col = tuple->At(1);
    vector<Value> keys;
    vector<Value> values;
    keys.reserve(keys_col->Size());
    values.reserve(values_col->Size());
    for (idx_t i = 0; i < keys_col->Size(); i++) {
      keys.push_back(ClickHouseColumnValueAt(*keys_col, i));
      values.push_back(ClickHouseColumnValueAt(*values_col, i));
    }
    return Value::MAP(key_type, value_type, std::move(keys), std::move(values));
  }
  default:
    return ClickHouseScalarValueAt(col, row);
  }
}

// Bulk-copy a fixed-width ClickHouse numeric column straight into the DuckDB
// FlatVector. ColumnVector<T> stores T contiguously and matches the FlatVector
// physical layout for these types, so a single memcpy replaces a per-cell
// dynamic_cast + Value box + SetValue.
template <class CHColumn, class T>
static void BulkCopyNumeric(const clickhouse::Column &col, Vector &out, idx_t src_offset, idx_t count) {
  auto typed = col.As<CHColumn>();
  auto data = FlatVector::GetDataMutable<T>(out);
  if (count > 0) {
    std::memcpy(data, &typed->At(src_offset), count * sizeof(T));
  }
}

// AddStringOrBlob, not AddString: with ch_binary_as_blob the target vector is
// BLOB, which AddString rejects (and raw bytes need no UTF-8 check).
template <class CHColumn>
static void BulkCopyString(const clickhouse::Column &col, Vector &out, idx_t src_offset, idx_t count) {
  auto typed = col.As<CHColumn>();
  auto data = FlatVector::GetDataMutable<string_t>(out);
  const bool is_blob = out.GetType().id() == LogicalTypeId::BLOB;
  for (idx_t row = 0; row < count; row++) {
    auto view = typed->At(src_offset + row);
    if (!is_blob) {
      VerifyStringUtf8(view.data(), view.size());
    }
    data[row] = StringVector::AddStringOrBlob(out, view.data(), view.size());
  }
}

void ClickHouseColumnToVector(const clickhouse::Column &col, Vector &out, idx_t src_offset, idx_t count) {
  switch (col.Type()->GetCode()) {
  case clickhouse::Type::Nullable: {
    auto nullable = col.As<clickhouse::ColumnNullable>();
    ClickHouseColumnToVector(*nullable->Nested(), out, src_offset, count);
    for (idx_t row = 0; row < count; row++) {
      if (nullable->IsNull(src_offset + row)) {
        FlatVector::SetNull(out, row, true);
      }
    }
    return;
  }
  // Fast paths: cast the column once and write directly into the FlatVector,
  // avoiding the per-cell dynamic_cast (col.As<>), Value boxing and SetValue
  // that dominated the scan profile.
  case clickhouse::Type::UInt8:
    return BulkCopyNumeric<clickhouse::ColumnUInt8, uint8_t>(col, out, src_offset, count);
  case clickhouse::Type::UInt16:
    return BulkCopyNumeric<clickhouse::ColumnUInt16, uint16_t>(col, out, src_offset, count);
  case clickhouse::Type::UInt32:
    return BulkCopyNumeric<clickhouse::ColumnUInt32, uint32_t>(col, out, src_offset, count);
  case clickhouse::Type::UInt64:
    return BulkCopyNumeric<clickhouse::ColumnUInt64, uint64_t>(col, out, src_offset, count);
  case clickhouse::Type::Int8:
    return BulkCopyNumeric<clickhouse::ColumnInt8, int8_t>(col, out, src_offset, count);
  case clickhouse::Type::Int16:
    return BulkCopyNumeric<clickhouse::ColumnInt16, int16_t>(col, out, src_offset, count);
  case clickhouse::Type::Int32:
    return BulkCopyNumeric<clickhouse::ColumnInt32, int32_t>(col, out, src_offset, count);
  case clickhouse::Type::Int64:
    return BulkCopyNumeric<clickhouse::ColumnInt64, int64_t>(col, out, src_offset, count);
  case clickhouse::Type::Float32:
    return BulkCopyNumeric<clickhouse::ColumnFloat32, float>(col, out, src_offset, count);
  case clickhouse::Type::Float64:
    return BulkCopyNumeric<clickhouse::ColumnFloat64, double>(col, out, src_offset, count);
  case clickhouse::Type::String:
    return BulkCopyString<clickhouse::ColumnString>(col, out, src_offset, count);
  case clickhouse::Type::FixedString:
    return BulkCopyString<clickhouse::ColumnFixedString>(col, out, src_offset, count);
  case clickhouse::Type::Date: {
    auto typed = col.As<clickhouse::ColumnDate>();
    auto data = FlatVector::GetDataMutable<date_t>(out);
    for (idx_t row = 0; row < count; row++) {
      data[row] = date_t(static_cast<int32_t>(typed->RawAt(src_offset + row)));
    }
    return;
  }
  case clickhouse::Type::Date32: {
    auto typed = col.As<clickhouse::ColumnDate32>();
    auto data = FlatVector::GetDataMutable<date_t>(out);
    for (idx_t row = 0; row < count; row++) {
      data[row] = date_t(typed->RawAt(src_offset + row));
    }
    return;
  }
  case clickhouse::Type::DateTime: {
    auto typed = col.As<clickhouse::ColumnDateTime>();
    auto data = FlatVector::GetDataMutable<timestamp_t>(out);
    for (idx_t row = 0; row < count; row++) {
      data[row] = Timestamp::FromEpochMicroSeconds(static_cast<int64_t>(typed->RawAt(src_offset + row)) * 1000000);
    }
    return;
  }
  case clickhouse::Type::DateTime64: {
    auto typed = col.As<clickhouse::ColumnDateTime64>();
    auto precision = static_cast<int>(typed->GetPrecision());
    auto data = FlatVector::GetDataMutable<timestamp_t>(out);
    for (idx_t row = 0; row < count; row++) {
      auto ticks = typed->At(src_offset + row);
      data[row] = Timestamp::FromEpochMicroSeconds(DateTime64TicksToMicros(ticks, precision));
    }
    return;
  }
  default:
    // Complex / long-tail types (Decimal, UUID, Enum, IP, Array, Map, Tuple,
    // LowCardinality, Int128/UInt128, ...) keep the per-cell Value path.
    for (idx_t row = 0; row < count; row++) {
      out.SetValue(row, ClickHouseColumnValueAt(col, src_offset + row));
    }
    return;
  }
}

//===--------------------------------------------------------------------===//
// Reverse map: DuckDB -> ClickHouse
//===--------------------------------------------------------------------===//
// One escaping style everywhere, via the shared dbconnector renderer: backtick
// identifiers and single-quoted string literals, both backslash-escaped -- the
// exact configs the shared filter pushdown and order-by optimizer already use,
// so the SELECT list, WHERE clause and DDL render an identifier identically.
std::string ClickHouseQuoteIdentifier(const std::string &name) {
  auto config = dbconnector::query::QueryWriter::CreateConfig(
      '`', dbconnector::query::QuoteEscapeStyle::BACKSLASH);
  return dbconnector::query::QueryWriter::WriteQuotedAndEscaped(config, name);
}

std::string ClickHouseStringLiteral(const std::string &value) {
  auto config = dbconnector::query::QueryWriter::CreateConfig(
      '\'', dbconnector::query::QuoteEscapeStyle::BACKSLASH);
  return dbconnector::query::QueryWriter::WriteQuotedAndEscaped(config, value);
}

std::string ClickHouseValueLiteral(const Value &value) {
  if (value.IsNull()) {
    return "NULL";
  }
  switch (value.type().id()) {
  case LogicalTypeId::BOOLEAN:
    return value.GetValue<bool>() ? "1" : "0";
  case LogicalTypeId::TINYINT:
  case LogicalTypeId::SMALLINT:
  case LogicalTypeId::INTEGER:
  case LogicalTypeId::BIGINT:
  case LogicalTypeId::UTINYINT:
  case LogicalTypeId::USMALLINT:
  case LogicalTypeId::UINTEGER:
  case LogicalTypeId::UBIGINT:
  case LogicalTypeId::FLOAT:
  case LogicalTypeId::DOUBLE:
    return value.ToString();
  case LogicalTypeId::HUGEINT:
  case LogicalTypeId::UHUGEINT:
  case LogicalTypeId::DECIMAL:
  case LogicalTypeId::BLOB: {
    // The shared writer's ClickHouse dialect renders these exactly: (U)HugeInt and
    // Decimal via toInt128/toUInt128/toDecimal128 casts (a bare wide literal parses
    // as Float64, losing precision) and BLOB as unhex('HEX').
    auto config = dbconnector::query::QueryWriter::CreateConfig(
        '\'', dbconnector::query::QuoteEscapeStyle::BACKSLASH, "unhex('", ")",
        dbconnector::query::Dialect::ClickHouse);
    return dbconnector::query::QueryWriter::WriteConstant(config, value);
  }
  case LogicalTypeId::DATE: {
    // Typed casts, not bare quoted strings: comparisons coerce strings, but multiIf /
    // assignment contexts have no String<->Date/DateTime/UUID supertype (NO_COMMON_TYPE).
    // Numeric epoch days, not a date string: toDate32('...') silently SATURATES
    // outside 1900..2299, and the write path rejects that range loudly.
    auto days = value.GetValue<date_t>().days;
    if (days < -25567 || days > 120529) {
      throw InvalidInputException(
          "Date value out of range for a ClickHouse Date32 cast (representable: 1900-01-01..2299-12-31)");
    }
    return "toDate32(" + std::to_string(days) + ")";
  }
  case LogicalTypeId::TIMESTAMP_SEC:
  case LogicalTypeId::TIMESTAMP_MS:
  case LogicalTypeId::TIMESTAMP:
  case LogicalTypeId::TIMESTAMP_NS:
  case LogicalTypeId::TIMESTAMP_TZ: {
    // Epoch micros, not a wall-time string: toDateTime64('...') parses in the
    // SERVER time zone, so the same value would store a different instant than
    // the INSERT block path (which sends raw epoch) whenever the server is not
    // UTC. fromUnixTimestamp64Micro is time-zone independent.
    auto ts = value.DefaultCastAs(LogicalType::TIMESTAMP).GetValue<timestamp_t>();
    if (!ts.IsFinite()) {
      throw InvalidInputException("Infinite TIMESTAMP is not representable as a ClickHouse DateTime64");
    }
    return "fromUnixTimestamp64Micro(" + std::to_string(ts.value) + ")";
  }
  case LogicalTypeId::UUID:
    return "toUUID(" + ClickHouseStringLiteral(value.ToString()) + ")";
  case LogicalTypeId::LIST:
  case LogicalTypeId::ARRAY:
  case LogicalTypeId::STRUCT:
  case LogicalTypeId::MAP:
    // DuckDB's nested-value text is not valid ClickHouse literal syntax.
    throw NotImplementedException("Cannot render a %s value as a ClickHouse literal", value.type().ToString());
  default:
    // VARCHAR, DATE/TIMESTAMP, UUID, ENUM labels, ...: a quoted string literal that
    // ClickHouse casts to the column / comparison type.
    return ClickHouseStringLiteral(value.ToString());
  }
}

// ClickHouse forbids Nullable around compound types (Array/Tuple/Map); only
// scalar children get the Nullable wrap.
static bool ChildTypeNullable(const LogicalType &child) {
  return child.id() != LogicalTypeId::LIST && child.id() != LogicalTypeId::STRUCT &&
         child.id() != LogicalTypeId::MAP;
}

std::string LogicalTypeToClickHouseType(const LogicalType &type, bool nullable) {
  std::string base;
  switch (type.id()) {
  case LogicalTypeId::BOOLEAN:
    base = "Bool";
    break;
  case LogicalTypeId::TINYINT:
    base = "Int8";
    break;
  case LogicalTypeId::SMALLINT:
    base = "Int16";
    break;
  case LogicalTypeId::INTEGER:
    base = "Int32";
    break;
  case LogicalTypeId::BIGINT:
    base = "Int64";
    break;
  case LogicalTypeId::HUGEINT:
    base = "Int128";
    break;
  case LogicalTypeId::UTINYINT:
    base = "UInt8";
    break;
  case LogicalTypeId::USMALLINT:
    base = "UInt16";
    break;
  case LogicalTypeId::UINTEGER:
    base = "UInt32";
    break;
  case LogicalTypeId::UBIGINT:
    base = "UInt64";
    break;
  case LogicalTypeId::UHUGEINT:
    base = "UInt128";
    break;
  case LogicalTypeId::FLOAT:
    base = "Float32";
    break;
  case LogicalTypeId::DOUBLE:
    base = "Float64";
    break;
  case LogicalTypeId::VARCHAR:
  case LogicalTypeId::BLOB:
    base = "String";
    break;
  case LogicalTypeId::DATE:
    base = "Date32";
    break;
  case LogicalTypeId::TIMESTAMP_SEC:
    base = "DateTime64(0)";
    break;
  case LogicalTypeId::TIMESTAMP_MS:
    base = "DateTime64(3)";
    break;
  case LogicalTypeId::TIMESTAMP:
    base = "DateTime64(6)";
    break;
  case LogicalTypeId::TIMESTAMP_NS:
    base = "DateTime64(9)";
    break;
  case LogicalTypeId::UUID:
    base = "UUID";
    break;
  case LogicalTypeId::DECIMAL:
    base = "Decimal(" + std::to_string(DecimalType::GetWidth(type)) + ", " +
           std::to_string(DecimalType::GetScale(type)) + ")";
    break;
  case LogicalTypeId::ENUM: {
    // ClickHouse Enum8 values are int8 (fits <=127 positive ordinals), Enum16 are
    // int16. Members get 1-based ordinals; INSERT and read map by NAME (see the
    // Enum cases in AppendScalarColumn / ClickHouseColumnValueAt), so the numeric
    // ordinals are internal and need only be distinct + in range.
    auto size = EnumType::GetSize(type);
    // Enum8 fits 127 one-based ordinals; anything larger renders as Enum16. A member
    // count beyond Int16 is delegated to ClickHouse, which rejects the DDL loudly on
    // the first out-of-range ordinal.
    base = size <= 127 ? "Enum8(" : "Enum16(";
    for (idx_t i = 0; i < size; i++) {
      if (i > 0) {
        base += ", ";
      }
      base += ClickHouseStringLiteral(EnumType::GetString(type, i).GetString()) + " = " + std::to_string(i + 1);
    }
    base += ")";
    break;
  }
  case LogicalTypeId::LIST: {
    // ClickHouse forbids Nullable(Array(...)), so the array is never wrapped
    // (returned directly, bypassing the Nullable wrap below). Elements can be
    // NULL at runtime, so a scalar element type is made Nullable; nested element
    // types (Array/Map/Tuple) cannot be, and are left bare.
    auto &child = ListType::GetChildType(type);
    return "Array(" + LogicalTypeToClickHouseType(child, ChildTypeNullable(child)) + ")";
  }
  case LogicalTypeId::STRUCT: {
    // DuckDB STRUCT -> ClickHouse named Tuple. Like arrays, ClickHouse forbids
    // Nullable(Tuple(...)), so the tuple is returned directly; scalar fields are
    // made Nullable (nested Array/Struct/Map fields cannot be).
    std::string s = "Tuple(";
    auto field_count = StructType::GetChildCount(type);
    for (idx_t i = 0; i < field_count; i++) {
      if (i > 0) {
        s += ", ";
      }
      auto &field = StructType::GetChildType(type, i);
      s += ClickHouseQuoteIdentifier(StructType::GetChildName(type, i).GetIdentifierName()) + " " +
           LogicalTypeToClickHouseType(field, ChildTypeNullable(field));
    }
    s += ")";
    return s;
  }
  case LogicalTypeId::MAP: {
    // DuckDB MAP -> ClickHouse Map(K, V). ClickHouse forbids Nullable(Map) and a
    // Nullable Map key, so neither is wrapped; the value may be Nullable when scalar.
    auto &key = MapType::KeyType(type);
    auto &value = MapType::ValueType(type);
    return "Map(" + LogicalTypeToClickHouseType(key, false) + ", " +
           LogicalTypeToClickHouseType(value, ChildTypeNullable(value)) + ")";
  }
  default:
    throw NotImplementedException("Cannot map DuckDB type %s to a ClickHouse type", type.ToString());
  }
  return nullable ? "Nullable(" + base + ")" : base;
}

static clickhouse::Int128 HugeintToCH(const hugeint_t &value) {
  return absl::MakeInt128(value.upper, value.lower);
}

// NULL rows append T{} rather than data[row]: the Nullable wrapper records the
// flag, but the placeholder still goes over the wire and the vector buffer may
// hold arbitrary stale bytes at masked positions.
template <class CHColumn, class T>
static void AppendNumericColumn(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  auto typed = col->As<CHColumn>();
  auto data = FlatVector::GetData<T>(vec);
  auto &validity = FlatVector::Validity(vec);
  for (idx_t row = 0; row < count; row++) {
    typed->Append(validity.RowIsValid(row) ? data[row] : T{});
  }
}

// Append `count` rows into an existing ClickHouse column, transparently handling a
// Nullable wrapper (defined below ClickHouseColumnFromVector). Forward-declared here
// because the Tuple/Map cases of AppendScalarColumn recurse into it per field.
static void AppendColumnFromVector(const clickhouse::ColumnRef &col, Vector &vec, idx_t count);

template <class CHColumn>
static void AppendStringColumn(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  auto typed = col->As<CHColumn>();
  auto data = FlatVector::GetData<string_t>(vec);
  auto &validity = FlatVector::Validity(vec);
  for (idx_t row = 0; row < count; row++) {
    if (!validity.RowIsValid(row)) {
      typed->Append(std::string_view());
    } else {
      typed->Append(std::string_view(data[row].GetData(), data[row].GetSize()));
    }
  }
}

template <class PHYS>
static void AppendDecimalColumn(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  auto typed = col->As<clickhouse::ColumnDecimal>();
  auto data = FlatVector::GetData<PHYS>(vec);
  auto &validity = FlatVector::Validity(vec);
  for (idx_t row = 0; row < count; row++) {
    typed->Append(validity.RowIsValid(row) ? clickhouse::Int128(data[row]) : clickhouse::Int128(0));
  }
}

// The connector exposes ClickHouse Enum columns to DuckDB as VARCHAR, so the
// input is the label string. clickhouse-cpp resolves labels with std::map::at,
// whose raw std::out_of_range would escape the connector's error handling --
// validate against the enum definition first. Null rows append a raw
// placeholder ordinal (unchecked; the Nullable wrapper flags them).
template <class CHColumn, class ORDINAL>
static void AppendEnumColumn(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  auto typed = col->As<CHColumn>();
  auto enum_type = col->Type()->As<clickhouse::EnumType>();
  auto data = FlatVector::GetData<string_t>(vec);
  auto &validity = FlatVector::Validity(vec);
  for (idx_t row = 0; row < count; row++) {
    if (!validity.RowIsValid(row)) {
      typed->Append(static_cast<ORDINAL>(0), /*checkValue=*/false);
      continue;
    }
    std::string label(data[row].GetData(), data[row].GetSize());
    if (!enum_type || !enum_type->HasEnumName(label)) {
      throw InvalidInputException("Unknown label \"%s\" for a ClickHouse Enum column", label);
    }
    typed->Append(label);
  }
}

// Append `count` rows of a flattened DuckDB vector into a (non-Nullable) ClickHouse column.
// Null rows are written as a type default; the surrounding Nullable wrapper records the flag.
static void AppendScalarColumn(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  auto &validity = FlatVector::Validity(vec);
  switch (col->Type()->GetCode()) {
  case clickhouse::Type::UInt8:
    return AppendNumericColumn<clickhouse::ColumnUInt8, uint8_t>(col, vec, count);
  case clickhouse::Type::UInt16:
    return AppendNumericColumn<clickhouse::ColumnUInt16, uint16_t>(col, vec, count);
  case clickhouse::Type::UInt32:
    return AppendNumericColumn<clickhouse::ColumnUInt32, uint32_t>(col, vec, count);
  case clickhouse::Type::UInt64:
    return AppendNumericColumn<clickhouse::ColumnUInt64, uint64_t>(col, vec, count);
  case clickhouse::Type::Int8:
    return AppendNumericColumn<clickhouse::ColumnInt8, int8_t>(col, vec, count);
  case clickhouse::Type::Int16:
    return AppendNumericColumn<clickhouse::ColumnInt16, int16_t>(col, vec, count);
  case clickhouse::Type::Int32:
    return AppendNumericColumn<clickhouse::ColumnInt32, int32_t>(col, vec, count);
  case clickhouse::Type::Int64:
    return AppendNumericColumn<clickhouse::ColumnInt64, int64_t>(col, vec, count);
  case clickhouse::Type::Float32:
    return AppendNumericColumn<clickhouse::ColumnFloat32, float>(col, vec, count);
  case clickhouse::Type::Float64:
    return AppendNumericColumn<clickhouse::ColumnFloat64, double>(col, vec, count);
  case clickhouse::Type::Bool:
    return AppendNumericColumn<clickhouse::ColumnBool, bool>(col, vec, count);
  case clickhouse::Type::Int128: {
    auto typed = col->As<clickhouse::ColumnInt128>();
    auto data = FlatVector::GetData<hugeint_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      typed->Append(validity.RowIsValid(row) ? HugeintToCH(data[row]) : clickhouse::Int128(0));
    }
    return;
  }
  case clickhouse::Type::UInt128: {
    auto typed = col->As<clickhouse::ColumnUInt128>();
    auto data = FlatVector::GetData<uhugeint_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      typed->Append(validity.RowIsValid(row) ? absl::MakeUint128(data[row].upper, data[row].lower)
                                             : clickhouse::UInt128(0));
    }
    return;
  }
  case clickhouse::Type::UUID: {
    auto typed = col->As<clickhouse::ColumnUUID>();
    auto data = FlatVector::GetData<hugeint_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->Append(clickhouse::UUID {0, 0});
        continue;
      }
      // Inverse of the read decode: DuckDB stores a UUID as hugeint with the sign bit
      // of the high half flipped for ordering.
      typed->Append(clickhouse::UUID {static_cast<uint64_t>(data[row].upper) ^ (static_cast<uint64_t>(1) << 63),
                                      data[row].lower});
    }
    return;
  }
  case clickhouse::Type::String:
    return AppendStringColumn<clickhouse::ColumnString>(col, vec, count);
  case clickhouse::Type::FixedString:
    return AppendStringColumn<clickhouse::ColumnFixedString>(col, vec, count);
  case clickhouse::Type::Date: {
    // ClickHouse Date is an unsigned 16-bit day count: 1970-01-01..2149-06-06.
    // date_t.days is a signed int32 epoch day; reject out-of-range rather than
    // silently wrapping modulo 65536 (use Date32 for the wider range).
    auto typed = col->As<clickhouse::ColumnDate>();
    auto data = FlatVector::GetData<date_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->AppendRaw(0);
        continue;
      }
      auto days = data[row].days;
      if (days < 0 || days > 65535) {
        throw InvalidInputException(
            "Date value out of range for a ClickHouse Date column (representable: 1970-01-01..2149-06-06); "
            "use a Date32 column for wider dates");
      }
      typed->AppendRaw(static_cast<uint16_t>(days));
    }
    return;
  }
  case clickhouse::Type::Date32: {
    auto typed = col->As<clickhouse::ColumnDate32>();
    auto data = FlatVector::GetData<date_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->AppendRaw(0);
        continue;
      }
      // ClickHouse Date32 covers 1900-01-01..2299-12-31 (epoch days -25567..
      // 120529); the native protocol does not validate, so an out-of-range raw
      // value would be stored and rendered as garbage server-side. Infinities
      // land outside the range too.
      auto days = data[row].days;
      if (days < -25567 || days > 120529) {
        throw InvalidInputException(
            "Date value out of range for a ClickHouse Date32 column (representable: 1900-01-01..2299-12-31)");
      }
      typed->AppendRaw(days);
    }
    return;
  }
  case clickhouse::Type::DateTime: {
    // ClickHouse DateTime is unsigned 32-bit epoch seconds: 1970..2106. Reject
    // negative (pre-1970) or > UINT32_MAX rather than wrapping (use DateTime64).
    auto typed = col->As<clickhouse::ColumnDateTime>();
    auto data = FlatVector::GetData<timestamp_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->AppendRaw(0);
        continue;
      }
      int64_t seconds = FloorDiv(data[row].value, 1000000);
      if (seconds < 0 || seconds > static_cast<int64_t>(UINT32_MAX)) {
        throw InvalidInputException(
            "Timestamp out of range for a ClickHouse DateTime column (representable: 1970..2106); "
            "use a DateTime64 column for wider timestamps");
      }
      typed->AppendRaw(static_cast<uint32_t>(seconds));
    }
    return;
  }
  case clickhouse::Type::DateTime64: {
    auto typed = col->As<clickhouse::ColumnDateTime64>();
    auto precision = static_cast<int>(typed->GetPrecision());
    auto data = FlatVector::GetData<timestamp_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->Append(0);
        continue;
      }
      if (!data[row].IsFinite()) {
        // DuckDB's -infinity sentinel is -INT64_MAX, not INT64_MIN, so an explicit
        // INT64_MIN check misses it; IsFinite() rejects both infinities correctly.
        throw InvalidInputException(
            "Infinite TIMESTAMP is not representable in a ClickHouse "
            "DateTime64 column");
      }
      int64_t micros = data[row].value;
      // ClickHouse DateTime64 covers 1900-01-01..2299-12-31; like Date32, the
      // native protocol stores out-of-range ticks verbatim and the server then
      // renders garbage.
      int64_t seconds = FloorDiv(micros, 1000000);
      if (seconds < -2208988800 || seconds > 10413791999) {
        throw InvalidInputException(
            "Timestamp out of range for a ClickHouse DateTime64 column (representable: 1900..2299)");
      }
      int64_t ticks;
      if (precision <= 6) {
        ticks = FloorDiv(micros, Pow10(6 - precision));
      } else if (__builtin_mul_overflow(micros, Pow10(precision - 6), &ticks)) {
        throw InvalidInputException(
            "Timestamp out of range for a ClickHouse DateTime64(%d) column (tick count overflows int64)",
            precision);
      }
      typed->Append(ticks);
    }
    return;
  }
  case clickhouse::Type::Decimal:
  case clickhouse::Type::Decimal32:
  case clickhouse::Type::Decimal64:
  case clickhouse::Type::Decimal128: {
    switch (vec.GetType().InternalType()) {
    case PhysicalType::INT16:
      return AppendDecimalColumn<int16_t>(col, vec, count);
    case PhysicalType::INT32:
      return AppendDecimalColumn<int32_t>(col, vec, count);
    case PhysicalType::INT64:
      return AppendDecimalColumn<int64_t>(col, vec, count);
    case PhysicalType::INT128: {
      auto typed = col->As<clickhouse::ColumnDecimal>();
      auto data = FlatVector::GetData<hugeint_t>(vec);
      for (idx_t row = 0; row < count; row++) {
        typed->Append(validity.RowIsValid(row) ? HugeintToCH(data[row]) : clickhouse::Int128(0));
      }
      return;
    }
    default:
      throw NotImplementedException("Unsupported DuckDB decimal storage for ClickHouse INSERT");
    }
  }
  case clickhouse::Type::Enum8:
    return AppendEnumColumn<clickhouse::ColumnEnum8, int8_t>(col, vec, count);
  case clickhouse::Type::Enum16:
    return AppendEnumColumn<clickhouse::ColumnEnum16, int16_t>(col, vec, count);
  case clickhouse::Type::Array: {
    // vec is a flattened LIST vector. Build a ClickHouse sub-column for each
    // row's slice of the child vector and append it as one array element. The
    // element type string comes from the array's own (empty) data column, so
    // nested and Nullable element types recurse through ClickHouseColumnFromVector
    // unchanged.
    auto array_col = col->As<clickhouse::ColumnArray>();
    // Per-row sub-columns are cheap empty clones of the array's own (empty)
    // data column -- no per-row type-string parse (same trick as the Map case).
    auto element_prototype = array_col->GetData();
    auto list_entries = FlatVector::GetData<list_entry_t>(vec);
    auto &child_vec = ListVector::GetChild(vec);
    for (idx_t row = 0; row < count; row++) {
      auto &entry = list_entries[row];
      Vector slice(child_vec, entry.offset, entry.offset + entry.length);
      auto element_column = element_prototype->CloneEmpty();
      AppendColumnFromVector(element_column, slice, entry.length);
      array_col->AppendAsColumn(element_column);
    }
    return;
  }
  case clickhouse::Type::Tuple: {
    // DuckDB STRUCT -> ClickHouse Tuple: append each struct field into the tuple's
    // corresponding child column (fields may themselves be Nullable/nested).
    auto tuple_col = col->As<clickhouse::ColumnTuple>();
    auto &entries = StructVector::GetEntries(vec);
    for (idx_t f = 0; f < tuple_col->TupleSize(); f++) {
      AppendColumnFromVector(tuple_col->At(f), entries[f], count);
    }
    return;
  }
  case clickhouse::Type::Map: {
    // DuckDB MAP (a LIST of key/value pairs) -> ClickHouse Map(K,V), which is
    // physically Array(Tuple(K,V)). Build that array one map-row at a time from
    // the key/value child vectors, then wrap it in a ColumnMap and append.
    auto map_type = col->Type()->As<clickhouse::MapType>();
    std::string tuple_type =
        "Tuple(" + map_type->GetKeyType()->GetName() + ", " + map_type->GetValueType()->GetName() + ")";
    auto array_col = clickhouse::CreateColumnByType("Array(" + tuple_type + ")");
    // Parse the tuple type string once; per-row sub-columns are cheap empty clones.
    auto tuple_prototype = clickhouse::CreateColumnByType(tuple_type);
    auto list_entries = FlatVector::GetData<list_entry_t>(vec);
    auto &keys = MapVector::GetKeys(vec);
    auto &values = MapVector::GetValues(vec);
    for (idx_t row = 0; row < count; row++) {
      auto &entry = list_entries[row];
      Vector key_slice(keys, entry.offset, entry.offset + entry.length);
      Vector value_slice(values, entry.offset, entry.offset + entry.length);
      auto tuple_sub = tuple_prototype->CloneEmpty();
      auto tuple_typed = tuple_sub->As<clickhouse::ColumnTuple>();
      AppendColumnFromVector(tuple_typed->At(0), key_slice, entry.length);
      AppendColumnFromVector(tuple_typed->At(1), value_slice, entry.length);
      array_col->As<clickhouse::ColumnArray>()->AppendAsColumn(tuple_sub);
    }
    auto map_batch = std::make_shared<clickhouse::ColumnMap>(array_col);
    col->As<clickhouse::ColumnMap>()->Append(map_batch);
    return;
  }
  default:
    throw NotImplementedException("INSERT into ClickHouse column of type %s is not yet supported",
                                  col->Type()->GetName());
  }
}

static void AppendColumnFromVector(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  vec.Flatten(count);
  col->Reserve(count);
  if (col->Type()->GetCode() == clickhouse::Type::Nullable) {
    auto nullable = col->As<clickhouse::ColumnNullable>();
    AppendScalarColumn(nullable->Nested(), vec, count);
    auto &validity = FlatVector::Validity(vec);
    for (idx_t row = 0; row < count; row++) {
      nullable->Append(!validity.RowIsValid(row));
    }
    return;
  }
  auto &validity = FlatVector::Validity(vec);
  if (!validity.AllValid()) {
    throw InvalidInputException("Cannot insert NULL into non-Nullable ClickHouse column of type %s",
                                col->Type()->GetName());
  }
  AppendScalarColumn(col, vec, count);
}

clickhouse::ColumnRef ClickHouseColumnFromVector(const std::string &ch_type, Vector &vec, idx_t count) {
  auto col = clickhouse::CreateColumnByType(ch_type);
  if (!col) {
    throw NotImplementedException("Unsupported ClickHouse column type for INSERT: %s", ch_type);
  }
  AppendColumnFromVector(col, vec, count);
  return col;
}

// CH types whose VARCHAR mapping makes text comparison/ordering diverge from the
// server's own: Enum (compares by ordinal, aborts on an unknown label), IPv4/IPv6
// (compares as an address), and the schemaless JSON family. Matched by substring
// so wrappers -- Nullable(Enum8(...)), LowCardinality(...), Array(IPv4) -- are
// caught too; a plain String/FixedString type string contains none of these.
static bool ClickHouseTypeTextDiverges(const std::string &ch_type) {
  return ch_type.find("Enum8(") != std::string::npos || ch_type.find("Enum16(") != std::string::npos ||
         ch_type.find("IPv4") != std::string::npos || ch_type.find("IPv6") != std::string::npos ||
         ch_type.find("JSON") != std::string::npos || ch_type.find("Variant(") != std::string::npos ||
         ch_type.find("Dynamic") != std::string::npos;
}

// The type-id switch below only sees the TOP-LEVEL DuckDB type, so a divergent
// scalar nested inside a compound column (Tuple/Array/Map/Nested -> STRUCT/LIST/
// MAP) would slip through: tupleElement(col,'f') comparisons on a Float or
// DateTime field push as exact while the remote evaluates them differently.
// Matched by substring on the original CH type string: floats (NaN placement),
// UUID (half-swapped byte order) and the date family ("Date" also covers
// Date32/DateTime/DateTime64: literals parse in the server's time zone).
static bool ClickHouseInnerComparisonDiverges(const std::string &ch_type) {
  return ch_type.find("Float32") != std::string::npos || ch_type.find("Float64") != std::string::npos ||
         ch_type.find("UUID") != std::string::npos || ch_type.find("Date") != std::string::npos;
}

static bool IsCompoundType(const LogicalType &duckdb_type) {
  switch (duckdb_type.id()) {
  case LogicalTypeId::STRUCT:
  case LogicalTypeId::LIST:
  case LogicalTypeId::ARRAY:
  case LogicalTypeId::MAP:
    return true;
  default:
    return false;
  }
}

bool ClickHouseComparisonUnsafe(const LogicalType &duckdb_type, const std::string &ch_type) {
  switch (duckdb_type.id()) {
  case LogicalTypeId::FLOAT:
  case LogicalTypeId::DOUBLE:
  case LogicalTypeId::DATE:
  case LogicalTypeId::TIMESTAMP:
  case LogicalTypeId::TIMESTAMP_TZ:
  case LogicalTypeId::TIMESTAMP_NS:
  case LogicalTypeId::TIMESTAMP_MS:
  case LogicalTypeId::TIMESTAMP_SEC:
  case LogicalTypeId::TIME:
  case LogicalTypeId::TIME_TZ:
  case LogicalTypeId::UUID:
    return true;
  default:
    return (IsCompoundType(duckdb_type) && ClickHouseInnerComparisonDiverges(ch_type)) ||
           ClickHouseTypeTextDiverges(ch_type);
  }
}

} // namespace duckdb
