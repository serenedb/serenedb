////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string_view>
#include <utility>

#include "connector/pg_logical_types.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/format_type.h"
#include "pg/catalog/lookup.h"

namespace sdb::pg {
namespace {

using TypmodFn = std::optional<int32_t> (*)(int64_t, int32_t);

template<TypmodFn Fn>
void TypmodFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, int32_t>(
    args, result,
    [](int64_t typid, int32_t typmod) { return Fn(typid, typmod); });
}

void IntervalTypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                          duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int32_t>(
    args, result,
    [&](int64_t typid, int32_t typmod) -> duckdb::optional<duckdb::string_t> {
      const auto type = IntervalType(typid, typmod);
      if (!type) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(result, *type);
    });
}

template<typename R>
duckdb::optional<int32_t> Position(const R& attnums, int16_t column) {
  const auto it = absl::c_find(attnums, column);
  if (it == attnums.end()) {
    return duckdb::nullopt;
  }
  return static_cast<int32_t>(it - attnums.begin()) + 1;
}

duckdb::optional<int32_t> IndexPosition(const Session& session, int64_t oid,
                                        int16_t column) {
  if (auto entry = EntryByOid(session, static_cast<uint64_t>(oid))) {
    if (entry->type != duckdb::CatalogType::INDEX_ENTRY) {
      return duckdb::nullopt;
    }
    const auto& index = entry->Cast<duckdb::IndexCatalogEntry>();
    auto relation = index.GetRelation(*session.transaction);
    if (!relation) {
      return duckdb::nullopt;
    }
    return Position(IndexAttnums(*session.context, index, *relation), column);
  }
  if (const auto key = FindKeyIndex(session, static_cast<uint64_t>(oid));
      key.table) {
    return Position(KeyAttnums(key.table->GetColumns(), *key.key_index),
                    column);
  }
  return duckdb::nullopt;
}

void IndexPositionFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, int16_t>(
    args, result, [&](int64_t oid, int16_t column) {
      return IndexPosition(session, oid, column);
    });
}

}  // namespace

void RegisterInformationSchemaFunctions(duckdb::ExtensionLoader& loader) {
  using enum duckdb::FunctionStability;
  const auto oid = OID();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto int2 = duckdb::LogicalType::SMALLINT;
  const auto int4 = duckdb::LogicalType::INTEGER;
  const std::string_view information_schema =
    irs::StaticStrings::kInformationSchema;

  for (const auto& [name, function] : {
         std::pair{"_pg_char_max_length", &TypmodFunction<CharMaxLength>},
         std::pair{"_pg_char_octet_length", &TypmodFunction<CharOctetLength>},
         std::pair{"_pg_numeric_precision", &TypmodFunction<NumericPrecision>},
         std::pair{"_pg_numeric_precision_radix",
                   &TypmodFunction<NumericPrecisionRadix>},
         std::pair{"_pg_numeric_scale", &TypmodFunction<NumericScale>},
         std::pair{"_pg_datetime_precision",
                   &TypmodFunction<DatetimePrecision>},
       }) {
    RegisterScalar(loader, information_schema, name, CONSISTENT,
                   {duckdb::ScalarFunction{{oid, int4}, int4, function}});
  }
  RegisterScalar(
    loader, information_schema, "_pg_interval_type", CONSISTENT,
    {duckdb::ScalarFunction{{oid, int4}, text, IntervalTypeFunction}});
  RegisterScalar(
    loader, information_schema, "_pg_index_position", CONSISTENT_WITHIN_QUERY,
    InitSession,
    {duckdb::ScalarFunction{{oid, int2}, int4, IndexPositionFunction}});
}

}  // namespace sdb::pg
