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

#include "connector/functions/minhash.h"

#include <absl/base/internal/endian.h>

#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/minhash_utils.hpp>
#include <iterator>

#include "basics/wyhash.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

constexpr uint64_t kHashSeed = 0xdeadbeef;

struct MinHashBindData final : public duckdb::FunctionData {
  uint32_t num_hashes = 1;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<MinHashBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return num_hashes == other.Cast<MinHashBindData>().num_hashes;
  }
};

struct MinHashLocalState final : public duckdb::FunctionLocalState {
  explicit MinHashLocalState(uint32_t num_hashes) : sketch{num_hashes} {}

  irs::MinHash sketch;
};

duckdb::unique_ptr<duckdb::FunctionData> MinHashBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& args = input.GetArguments();
  if (!args[1]->IsFoldable()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("minhash: num_hashes must be a constant"));
  }
  const auto value =
    duckdb::ExpressionExecutor::EvaluateScalar(input.GetClientContext(), *args[1]);
  if (value.IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("minhash: num_hashes must not be NULL"));
  }
  const auto num_hashes = duckdb::IntegerValue::Get(value);
  if (num_hashes < 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("minhash: num_hashes must be >= 1, got ", num_hashes));
  }
  auto bind = duckdb::make_uniq<MinHashBindData>();
  bind->num_hashes = static_cast<uint32_t>(num_hashes);
  return bind;
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitMinHashLocalState(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<MinHashLocalState>(
    bind_data->Cast<MinHashBindData>().num_hashes);
}

void MinHashFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  const auto count = args.size();
  auto& sketch = duckdb::ExecuteFunctionState::GetFunctionState(state)
                   ->Cast<MinHashLocalState>()
                   .sketch;

  duckdb::UnifiedVectorFormat lists;
  args.data[0].ToUnifiedFormat(lists);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(lists);
  duckdb::UnifiedVectorFormat elements;
  duckdb::ListVector::GetChild(args.data[0]).ToUnifiedFormat(elements);
  const auto* tokens =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(elements);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* out_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  auto& signatures = duckdb::ListVector::GetChildMutable(result);
  duckdb::idx_t offset = 0;

  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto idx = lists.sel->get_index(row);
    if (!lists.validity.RowIsValid(idx)) {
      validity.SetInvalid(row);
      out_entries[row] = {offset, 0};
      continue;
    }
    sketch.Clear();
    const auto entry = entries[idx];
    for (duckdb::idx_t k = 0; k < entry.length; ++k) {
      const auto token_idx = elements.sel->get_index(entry.offset + k);
      if (!elements.validity.RowIsValid(token_idx)) {
        continue;
      }
      const auto& token = tokens[token_idx];
      sketch.Insert(
        sdb::basics::WyHash(token.GetData(), token.GetSize(), kHashSeed));
    }
    const auto row_offset = offset;
    const auto produced =
      static_cast<duckdb::idx_t>(std::distance(sketch.begin(), sketch.end()));
    duckdb::ListVector::SetListSize(result, offset);
    duckdb::ListVector::Reserve(result, offset + produced);
    auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(signatures);
    for (const auto hash : sketch) {
      const auto value = absl::little_endian::FromHost(hash);
      data[offset++] = duckdb::StringVector::AddStringOrBlob(
        signatures, reinterpret_cast<const char*>(&value), sizeof value);
    }
    out_entries[row] = {row_offset, offset - row_offset};
  }
  duckdb::ListVector::SetListSize(result, offset);
}

}  // namespace

void RegisterMinHash(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{"minhash"};
  for (const auto& element :
       {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB}) {
    duckdb::ScalarFunction f{
      {duckdb::LogicalType::LIST(element), duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::LIST(duckdb::LogicalType::BLOB),
      MinHashFunction,
      MinHashBind,
      nullptr,
      InitMinHashLocalState,
    };
    f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    set.AddFunction(std::move(f));
  }
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
