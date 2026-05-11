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

#include "connector/functions/cast.h"

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>

#include "pg/parse_array.h"

namespace sdb::connector {
namespace {

// Cast VARCHAR -> LIST(VARCHAR) by parsing PG array literal format.
// Input:  '{1,2,3}' or '{hello,world,NULL}'
// Output: ['1','2','3'] or ['hello','world',NULL]
bool PgArrayTextToList(duckdb::Vector& source, duckdb::Vector& result,
                       duckdb::idx_t count,
                       duckdb::CastParameters& parameters) {
  // DuckDB may pass non-flat (constant/dictionary) vectors
  duckdb::UnifiedVectorFormat source_data;
  source.ToUnifiedFormat(count, source_data);
  auto source_strings =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(source_data);

  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  duckdb::idx_t total_elements = 0;

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = source_data.sel->get_index(i);
    if (!source_data.validity.RowIsValid(src_idx)) {
      result_validity.SetInvalid(i);
      continue;
    }

    auto input_sv = source_strings[src_idx].GetString();

    auto list_offset = total_elements;

    pg::ParsePgTextArray(
      input_sv,
      [&](std::string_view token, bool is_null) {
        duckdb::ListVector::Reserve(result, total_elements + 1);
        auto& entry = duckdb::ListVector::GetEntry(result);
        if (is_null) {
          duckdb::FlatVector::ValidityMutable(entry).SetInvalid(total_elements);
        } else {
          duckdb::FlatVector::GetDataMutable<duckdb::string_t>(
            entry)[total_elements] =
            duckdb::StringVector::AddString(entry, token.data(), token.size());
        }
        total_elements++;
      },
      [&](std::string_view msg) {
        throw duckdb::InvalidInputException("Failed to parse array literal: %s",
                                            std::string{msg});
      });

    duckdb::ListVector::GetData(result)[i] =
      duckdb::list_entry_t{list_offset, total_elements - list_offset};
  }

  duckdb::ListVector::SetListSize(result, total_elements);
  return true;
}

// Bind function: VARCHAR -> LIST(T)
// Chains: VARCHAR -> LIST(VARCHAR) [our parser] -> LIST(T) [DuckDB native cast]
duckdb::BoundCastInfo PgArrayCastBind(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType& source,
                                      const duckdb::LogicalType& target) {
  auto child_type = duckdb::ListType::GetChildType(target);

  if (child_type.id() == duckdb::LogicalTypeId::VARCHAR) {
    // VARCHAR -> LIST(VARCHAR): direct parse, no chain needed
    return duckdb::BoundCastInfo(PgArrayTextToList);
  }

  // VARCHAR -> LIST(T): parse to LIST(VARCHAR),
  // then cast LIST(VARCHAR) -> LIST(T)
  auto varchar_list = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  duckdb::GetCastFunctionInput get_input(input.context);
  auto child_cast =
    input.function_set.GetCastFunction(varchar_list, target, get_input);

  struct ChainCastData : public duckdb::BoundCastData {
    duckdb::BoundCastInfo child_cast;
    explicit ChainCastData(duckdb::BoundCastInfo child_cast)
      : child_cast(std::move(child_cast)) {}
    duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
      return duckdb::make_uniq<ChainCastData>(child_cast.Copy());
    }
  };

  auto cast_data = duckdb::make_uniq<ChainCastData>(std::move(child_cast));

  auto chain_fn = [](duckdb::Vector& source, duckdb::Vector& result,
                     duckdb::idx_t count,
                     duckdb::CastParameters& parameters) -> bool {
    auto& data = parameters.cast_data->Cast<ChainCastData>();

    // Step 1: VARCHAR -> LIST(VARCHAR)
    duckdb::Vector intermediate(
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR), count);
    PgArrayTextToList(source, intermediate, count, parameters);

    // Step 2: LIST(VARCHAR) -> LIST(T) via DuckDB native cast
    duckdb::CastParameters child_params(
      parameters, data.child_cast.GetCastData().get(), nullptr);
    return data.child_cast.GetFunction()(intermediate, result, count,
                                         child_params);
  };

  return duckdb::BoundCastInfo(chain_fn, std::move(cast_data));
}

}  // namespace

void RegisterPgCasts(duckdb::DatabaseInstance& db) {
  auto& config = duckdb::DBConfig::GetConfig(db);
  auto& casts = config.GetCastFunctions();

  // Register PG-compatible VARCHAR -> LIST cast so that '{1,2,3}'::int[] works.
  // Parses PostgreSQL text-format array literals into DuckDB lists.
  // Register bind function for VARCHAR -> LIST(ANY)
  casts.RegisterCastFunction(
    duckdb::LogicalType::VARCHAR,
    duckdb::LogicalType::LIST(duckdb::LogicalType::ANY), PgArrayCastBind, 100);
}

}  // namespace sdb::connector
