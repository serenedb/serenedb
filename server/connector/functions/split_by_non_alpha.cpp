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

#include "connector/functions/split_by_non_alpha.h"

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <iresearch/analysis/split_by_non_alpha.hpp>
#include <string>
#include <string_view>

#include "connector/common.h"

namespace sdb::connector {
namespace {

// Appends VARCHAR tokens to the child of a LIST result vector, growing it as
// needed. The child reference is stable across Reserve (only its data buffer
// moves), so the data pointer is re-fetched on every Push.
class ListSink {
 public:
  explicit ListSink(duckdb::Vector& result_list)
    : _result_list(result_list),
      _result_child(duckdb::ListVector::GetEntry(result_list)) {}
  ~ListSink() { duckdb::ListVector::SetListSize(_result_list, _offset); }

  duckdb::idx_t Offset() const noexcept { return _offset; }

  void Push(std::string_view token) {
    if (_offset >= duckdb::ListVector::GetListCapacity(_result_list)) {
      duckdb::ListVector::SetListSize(_result_list, _offset);
      duckdb::ListVector::Reserve(
        _result_list, duckdb::ListVector::GetListCapacity(_result_list) * 2);
    }
    auto* data =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(_result_child);
    data[_offset] = duckdb::StringVector::AddStringOrBlob(
      _result_child, token.data(), token.size());
    ++_offset;
  }

 private:
  duckdb::Vector& _result_list;
  duckdb::Vector& _result_child;
  duckdb::idx_t _offset = 0;
};

void SplitByNonAlphaFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& /*state*/,
                             duckdb::Vector& result) {
  const auto count = args.size();
  const bool has_to_lower = args.ColumnCount() >= 2;

  duckdb::UnifiedVectorFormat text_format;
  args.data[0].ToUnifiedFormat(count, text_format);
  auto* text_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  duckdb::UnifiedVectorFormat tl_format;
  const bool* tl_data = nullptr;
  if (has_to_lower) {
    args.data[1].ToUnifiedFormat(count, tl_format);
    tl_data = duckdb::UnifiedVectorFormat::GetData<bool>(tl_format);
  }

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  ListSink sink{result};
  std::string buf;

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto text_idx = text_format.sel->get_index(i);
    if (!text_format.validity.RowIsValid(text_idx)) {
      result_validity.SetInvalid(i);
      list_entries[i] = {sink.Offset(), 0};
      continue;
    }
    bool to_lower = false;
    if (has_to_lower) {
      auto tl_idx = tl_format.sel->get_index(i);
      to_lower = tl_format.validity.RowIsValid(tl_idx) && tl_data[tl_idx];
    }
    const auto row_offset = sink.Offset();
    irs::analysis::SplitByNonAlpha(
      AsView(text_data[text_idx]), to_lower, buf,
      [&](std::string_view token) { sink.Push(token); });
    list_entries[i] = {row_offset, sink.Offset() - row_offset};
  }
}

duckdb::ScalarFunction MakeFn(duckdb::vector<duckdb::LogicalType> args) {
  duckdb::ScalarFunction f{
    std::move(args),
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    SplitByNonAlphaFunction,
  };
  f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  return f;
}

}  // namespace

void RegisterSplitByNonAlpha(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{"split_by_non_alpha"};
  set.AddFunction(MakeFn({duckdb::LogicalType::VARCHAR}));
  set.AddFunction(
    MakeFn({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN}));
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
