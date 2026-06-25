#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/types/types.h>

#include "clickhouse_types.hpp"

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

static std::string RenderDecimalString(clickhouse::Int128 unscaled, size_t scale) {
  bool negative = unscaled < 0;
  clickhouse::UInt128 magnitude = negative ? clickhouse::UInt128(-unscaled) : clickhouse::UInt128(unscaled);
  std::string digits;
  if (magnitude == 0) {
    digits = "0";
  } else {
    while (magnitude > 0) {
      auto digit = static_cast<int>(magnitude % 10);
      digits.push_back(static_cast<char>('0' + digit));
      magnitude /= 10;
    }
    std::reverse(digits.begin(), digits.end());
  }
  std::string result;
  if (scale == 0) {
    result = digits;
  } else {
    while (digits.size() <= scale) {
      digits.insert(digits.begin(), '0');
    }
    result = digits.substr(0, digits.size() - scale) + "." + digits.substr(digits.size() - scale);
  }
  if (negative) {
    result.insert(result.begin(), '-');
  }
  return result;
}

static hugeint_t ClickHouseInt128ToHugeint(const clickhouse::Int128 &value) {
  return hugeint_t(static_cast<int64_t>(absl::Int128High64(value)), absl::Int128Low64(value));
}

static uhugeint_t ClickHouseUInt128ToUhugeint(const clickhouse::UInt128 &value) {
  return uhugeint_t(absl::Uint128High64(value), absl::Uint128Low64(value));
}

LogicalType ClickHouseToLogicalType(const clickhouse::Type &type) {
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
      return LogicalType::VARCHAR;
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
      children.push_back(make_pair(std::move(name), ClickHouseToLogicalType(*item_types[i])));
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
  auto column = clickhouse::CreateColumnByType(type_str);
  if (!column) {
    throw NotImplementedException("Unsupported ClickHouse type: %s", type_str);
  }
  return ClickHouseToLogicalType(*column->Type());
}

static Value ClickHouseColumnValueAt(const clickhouse::Column &col, idx_t row);

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
  case clickhouse::Type::String: {
    auto view = col.As<clickhouse::ColumnString>()->At(row);
    return Value(std::string(view.data(), view.size()));
  }
  case clickhouse::Type::FixedString: {
    auto fixed = col.As<clickhouse::ColumnFixedString>();
    auto view = fixed->At(row);
    return Value(std::string(view.data(), view.size()));
  }
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
    int64_t micros;
    if (precision <= 6) {
      micros = ticks * Pow10(6 - precision);
    } else {
      micros = ticks / Pow10(precision - 6);
    }
    return Value::TIMESTAMP(Timestamp::FromEpochMicroSeconds(micros));
  }
  case clickhouse::Type::Decimal:
  case clickhouse::Type::Decimal32:
  case clickhouse::Type::Decimal64:
  case clickhouse::Type::Decimal128: {
    auto decimal = col.As<clickhouse::ColumnDecimal>();
    auto unscaled = decimal->At(row);
    auto precision = decimal->GetPrecision();
    auto scale = decimal->GetScale();
    if (precision > 38) {
      return Value(RenderDecimalString(unscaled, scale));
    }
    if (precision <= 18) {
      return Value::DECIMAL(static_cast<int64_t>(unscaled), static_cast<uint8_t>(precision),
                            static_cast<uint8_t>(scale));
    }
    return Value::DECIMAL(ClickHouseInt128ToHugeint(unscaled), static_cast<uint8_t>(precision),
                          static_cast<uint8_t>(scale));
  }
  case clickhouse::Type::Enum8:
    return Value(std::string(col.As<clickhouse::ColumnEnum8>()->NameAt(row)));
  case clickhouse::Type::Enum16:
    return Value(std::string(col.As<clickhouse::ColumnEnum16>()->NameAt(row)));
  case clickhouse::Type::UUID: {
    auto uuid = col.As<clickhouse::ColumnUUID>()->At(row);
    return Value::UUID(hugeint_t(static_cast<int64_t>(uuid.first ^ (static_cast<uint64_t>(1) << 63)), uuid.second));
  }
  case clickhouse::Type::IPv4:
    return Value(col.As<clickhouse::ColumnIPv4>()->AsString(row));
  case clickhouse::Type::IPv6:
    return Value(col.As<clickhouse::ColumnIPv6>()->AsString(row));
  case clickhouse::Type::JSON:
    return Value(std::string(col.As<clickhouse::ColumnString>()->At(row)));
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
    auto child_type = ClickHouseToLogicalType(*nested->Type());
    if (nullable->IsNull(row)) {
      return Value(child_type);
    }
    return ClickHouseColumnValueAt(*nested, row);
  }
  case clickhouse::Type::LowCardinality: {
    auto lc = col.As<clickhouse::ColumnLowCardinality>();
    auto item = lc->GetItem(row);
    auto nested_type = lc->GetNestedType();
    bool nested_nullable = nested_type->GetCode() == clickhouse::Type::Nullable;
    if (nested_nullable) {
      nested_type = nested_type->As<clickhouse::NullableType>()->GetNestedType();
    }
    switch (nested_type->GetCode()) {
    case clickhouse::Type::String:
    case clickhouse::Type::FixedString: {
      auto child_type = ClickHouseToLogicalType(*nested_type);
      auto view = item.AsBinaryData();
      if (nested_nullable && view.data() == nullptr) {
        return Value(child_type);
      }
      return Value(std::string(view.data(), view.size()));
    }
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
      children.push_back(make_pair(std::move(name), ClickHouseColumnValueAt(*tuple->At(i), row)));
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

void ClickHouseColumnToVector(const clickhouse::Column &col, Vector &out, idx_t src_offset, idx_t count) {
  switch (col.Type()->GetCode()) {
  case clickhouse::Type::Nullable: {
    auto nullable = col.As<clickhouse::ColumnNullable>();
    ClickHouseColumnToVector(*nullable->Nested(), out, src_offset, count);
    auto nulls = nullable->Nulls()->As<clickhouse::ColumnUInt8>();
    for (idx_t row = 0; row < count; row++) {
      if (nulls->At(src_offset + row) != 0) {
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
  case clickhouse::Type::String: {
    auto typed = col.As<clickhouse::ColumnString>();
    auto data = FlatVector::GetDataMutable<string_t>(out);
    for (idx_t row = 0; row < count; row++) {
      auto view = typed->At(src_offset + row);
      data[row] = StringVector::AddString(out, view.data(), view.size());
    }
    return;
  }
  case clickhouse::Type::FixedString: {
    auto typed = col.As<clickhouse::ColumnFixedString>();
    auto data = FlatVector::GetDataMutable<string_t>(out);
    for (idx_t row = 0; row < count; row++) {
      auto view = typed->At(src_offset + row);
      data[row] = StringVector::AddString(out, view.data(), view.size());
    }
    return;
  }
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
      int64_t micros = (precision <= 6) ? ticks * Pow10(6 - precision) : ticks / Pow10(precision - 6);
      data[row] = Timestamp::FromEpochMicroSeconds(micros);
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
std::string ClickHouseQuoteIdentifier(const std::string &name) {
  std::string result = "`";
  for (auto c : name) {
    if (c == '\\') {
      result += "\\\\";
    } else if (c == '`') {
      result += "``";
    } else {
      result += c;
    }
  }
  result += "`";
  return result;
}

std::string ClickHouseStringLiteral(const std::string &value) {
  std::string result = "'";
  for (auto c : value) {
    if (c == '\'' || c == '\\') {
      result += '\\';
    }
    result += c;
  }
  result += "'";
  return result;
}

std::string ClickHouseEscapeSingleQuotes(const std::string &value) {
  std::string result;
  result.reserve(value.size());
  for (auto c : value) {
    if (c == '\'') {
      result += "''";
    } else {
      result += c;
    }
  }
  return result;
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
  default:
    throw NotImplementedException("Cannot map DuckDB type %s to a ClickHouse type", type.ToString());
  }
  return nullable ? "Nullable(" + base + ")" : base;
}

static clickhouse::Int128 HugeintToCH(const hugeint_t &value) {
  return absl::MakeInt128(value.upper, value.lower);
}

template <class CHColumn, class T>
static void AppendNumericColumn(const clickhouse::ColumnRef &col, Vector &vec, idx_t count) {
  auto typed = col->As<CHColumn>();
  auto data = FlatVector::GetData<T>(vec);
  for (idx_t row = 0; row < count; row++) {
    typed->Append(data[row]);
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
  case clickhouse::Type::Bool: {
    auto typed = col->As<clickhouse::ColumnBool>();
    auto data = FlatVector::GetData<bool>(vec);
    for (idx_t row = 0; row < count; row++) {
      typed->Append(data[row]);
    }
    return;
  }
  case clickhouse::Type::Int128: {
    auto typed = col->As<clickhouse::ColumnInt128>();
    auto data = FlatVector::GetData<hugeint_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      typed->Append(HugeintToCH(data[row]));
    }
    return;
  }
  case clickhouse::Type::UInt128: {
    auto typed = col->As<clickhouse::ColumnUInt128>();
    auto data = FlatVector::GetData<uhugeint_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      typed->Append(absl::MakeUint128(data[row].upper, data[row].lower));
    }
    return;
  }
  case clickhouse::Type::String: {
    auto typed = col->As<clickhouse::ColumnString>();
    auto data = FlatVector::GetData<string_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->Append(std::string_view());
      } else {
        typed->Append(std::string_view(data[row].GetData(), data[row].GetSize()));
      }
    }
    return;
  }
  case clickhouse::Type::FixedString: {
    auto typed = col->As<clickhouse::ColumnFixedString>();
    auto data = FlatVector::GetData<string_t>(vec);
    for (idx_t row = 0; row < count; row++) {
      if (!validity.RowIsValid(row)) {
        typed->Append(std::string_view());
      } else {
        typed->Append(std::string_view(data[row].GetData(), data[row].GetSize()));
      }
    }
    return;
  }
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
      if (!data[row].IsFinite()) {
        throw InvalidInputException(
            "Infinite DATE is not representable in a ClickHouse Date32 column");
      }
      typed->AppendRaw(static_cast<int32_t>(data[row].days));
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
      int64_t seconds = data[row].value / 1000000;
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
      int64_t micros = data[row].value;
      if (micros == INT64_MAX || micros == INT64_MIN) {
        throw InvalidInputException(
            "Infinite TIMESTAMP is not representable in a ClickHouse "
            "DateTime64 column");
      }
      int64_t ticks;
      if (precision <= 6) {
        ticks = micros / Pow10(6 - precision);
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
    auto typed = col->As<clickhouse::ColumnDecimal>();
    switch (vec.GetType().InternalType()) {
    case PhysicalType::INT16: {
      auto data = FlatVector::GetData<int16_t>(vec);
      for (idx_t row = 0; row < count; row++) {
        typed->Append(clickhouse::Int128(data[row]));
      }
      return;
    }
    case PhysicalType::INT32: {
      auto data = FlatVector::GetData<int32_t>(vec);
      for (idx_t row = 0; row < count; row++) {
        typed->Append(clickhouse::Int128(data[row]));
      }
      return;
    }
    case PhysicalType::INT64: {
      auto data = FlatVector::GetData<int64_t>(vec);
      for (idx_t row = 0; row < count; row++) {
        typed->Append(clickhouse::Int128(data[row]));
      }
      return;
    }
    case PhysicalType::INT128: {
      auto data = FlatVector::GetData<hugeint_t>(vec);
      for (idx_t row = 0; row < count; row++) {
        typed->Append(HugeintToCH(data[row]));
      }
      return;
    }
    default:
      throw NotImplementedException("Unsupported DuckDB decimal storage for ClickHouse INSERT");
    }
  }
  default:
    throw NotImplementedException("INSERT into ClickHouse column of type %s is not yet supported",
                                  col->Type()->GetName());
  }
}

clickhouse::ColumnRef ClickHouseColumnFromVector(const std::string &ch_type, Vector &vec, idx_t count) {
  auto col = clickhouse::CreateColumnByType(ch_type);
  if (!col) {
    throw NotImplementedException("Unsupported ClickHouse column type for INSERT: %s", ch_type);
  }
  vec.Flatten(count);
  col->Reserve(count);
  if (col->Type()->GetCode() == clickhouse::Type::Nullable) {
    auto nullable = col->As<clickhouse::ColumnNullable>();
    AppendScalarColumn(nullable->Nested(), vec, count);
    auto &validity = FlatVector::Validity(vec);
    for (idx_t row = 0; row < count; row++) {
      nullable->Append(!validity.RowIsValid(row));
    }
    return col;
  }
  auto &validity = FlatVector::Validity(vec);
  if (!validity.AllValid()) {
    throw InvalidInputException("Cannot insert NULL into non-Nullable ClickHouse column of type %s",
                                col->Type()->GetName());
  }
  AppendScalarColumn(col, vec, count);
  return col;
}

} // namespace duckdb
