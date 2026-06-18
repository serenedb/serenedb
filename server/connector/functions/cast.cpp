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

#include <absl/strings/ascii.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <string>
#include <string_view>

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

// Append one element to a PG array literal, applying PG quoting rules.
void AppendPgArrayElement(std::string& out, std::string_view value) {
  bool needs_quotes = value.empty() || absl::EqualsIgnoreCase(value, "NULL");
  if (!needs_quotes) {
    for (char c : value) {
      if (c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' ||
          absl::ascii_isspace(static_cast<unsigned char>(c))) {
        needs_quotes = true;
        break;
      }
    }
  }

  if (!needs_quotes) {
    out += value;
    return;
  }

  out += '"';
  for (char c : value) {
    if (c == '"' || c == '\\') {
      out += '\\';
    }
    out += c;
  }
  out += '"';
}

struct PgArrayToTextCastData : public duckdb::BoundCastData {
  PgArrayToTextCastData(bool child_is_nested, duckdb::BoundCastInfo child_cast)
    : child_is_nested(child_is_nested), child_cast(std::move(child_cast)) {}
  bool child_is_nested;
  duckdb::BoundCastInfo child_cast;
  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<PgArrayToTextCastData>(child_is_nested,
                                                    child_cast.Copy());
  }
};

// Emit a LIST(VARCHAR) row in PG array literal format into out.
// When child_is_nested, child strings are already rendered nested arrays
// ('{..}') and are emitted verbatim (PG does not quote nested array elements).
void RenderPgArrayRow(std::string& out,
                      const duckdb::UnifiedVectorFormat& child_data,
                      const duckdb::string_t* child_strings,
                      duckdb::list_entry_t entry, bool child_is_nested) {
  out += '{';
  for (duckdb::idx_t k = 0; k < entry.length; k++) {
    if (k > 0) {
      out += ',';
    }
    auto child_idx = child_data.sel->get_index(entry.offset + k);
    if (!child_data.validity.RowIsValid(child_idx)) {
      out += "NULL";
    } else if (child_is_nested) {
      out += child_strings[child_idx].GetString();
    } else {
      AppendPgArrayElement(out, child_strings[child_idx].GetString());
    }
  }
  out += '}';
}

// Cast LIST(VARCHAR) -> VARCHAR by emitting PG array literal format.
// Input:  ['1','2','3'] or ['hello',NULL]
// Output: '{1,2,3}' or '{hello,NULL}'
bool PgListToArrayText(duckdb::Vector& source, duckdb::Vector& result,
                       duckdb::idx_t count,
                       duckdb::CastParameters& parameters) {
  bool child_is_nested = false;
  if (parameters.cast_data) {
    child_is_nested =
      parameters.cast_data->Cast<PgArrayToTextCastData>().child_is_nested;
  }

  duckdb::UnifiedVectorFormat list_data;
  source.ToUnifiedFormat(count, list_data);
  auto list_entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(list_data);

  auto& child = duckdb::ListVector::GetEntry(source);
  auto child_size = duckdb::ListVector::GetListSize(source);
  duckdb::UnifiedVectorFormat child_data;
  child.ToUnifiedFormat(child_size, child_data);
  auto child_strings =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_data);

  auto result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto list_idx = list_data.sel->get_index(i);
    if (!list_data.validity.RowIsValid(list_idx)) {
      result_validity.SetInvalid(i);
      continue;
    }

    std::string out;
    RenderPgArrayRow(out, child_data, child_strings, list_entries[list_idx],
                     child_is_nested);
    result_data[i] = duckdb::StringVector::AddString(result, out);
  }

  if (source.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  }
  return true;
}

// Bind function: LIST(T) -> VARCHAR
// Chains: LIST(T) -> LIST(VARCHAR) [DuckDB native cast] -> VARCHAR [our format]
duckdb::BoundCastInfo PgArrayToTextCastBind(duckdb::BindCastInput& input,
                                            const duckdb::LogicalType& source,
                                            const duckdb::LogicalType& target) {
  auto child_type = duckdb::ListType::GetChildType(source);
  const bool child_is_nested = child_type.id() == duckdb::LogicalTypeId::LIST ||
                               child_type.id() == duckdb::LogicalTypeId::ARRAY;

  // Chain LIST(T) -> LIST(VARCHAR) (a no-op NopCast when T is already VARCHAR)
  // -> our PG formatter.
  auto varchar_list = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  duckdb::GetCastFunctionInput get_input(input.context);
  auto cast_data = duckdb::make_uniq<PgArrayToTextCastData>(
    child_is_nested,
    input.function_set.GetCastFunction(source, varchar_list, get_input));

  auto chain_fn = [](duckdb::Vector& source, duckdb::Vector& result,
                     duckdb::idx_t count,
                     duckdb::CastParameters& parameters) -> bool {
    auto& data = parameters.cast_data->Cast<PgArrayToTextCastData>();

    // Step 1: LIST(T) -> LIST(VARCHAR)
    duckdb::Vector intermediate(
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR), count);
    duckdb::CastParameters child_params(
      parameters, data.child_cast.GetCastData().get(), nullptr);
    if (!data.child_cast.GetFunction()(source, intermediate, count,
                                       child_params)) {
      return false;
    }

    // Step 2: LIST(VARCHAR) -> VARCHAR via our PG formatter
    return PgListToArrayText(intermediate, result, count, parameters);
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

  // Register PG-compatible LIST -> VARCHAR cast so that array::text renders in
  // PostgreSQL brace format '{a,b,c}' instead of DuckDB's '[a, b, c]'.
  casts.RegisterCastFunction(
    duckdb::LogicalType::LIST(duckdb::LogicalType::ANY),
    duckdb::LogicalType::VARCHAR, PgArrayToTextCastBind, 100);
  casts.RegisterCastFunction(
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    duckdb::LogicalType::VARCHAR, PgArrayToTextCastBind, 100);
}

}  // namespace sdb::connector
