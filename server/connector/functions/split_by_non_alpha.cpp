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

#include <absl/strings/ascii.h>

#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/analysis/split_by_non_alpha.hpp>
#include <string_view>

#include "connector/common.h"

namespace sdb::connector {
namespace {

class ListSink {
 public:
  explicit ListSink(duckdb::Vector& result_list)
    : _result_list(result_list),
      _result_child(duckdb::ListVector::GetEntry(result_list)) {}
  ~ListSink() { duckdb::ListVector::SetListSize(_result_list, _offset); }

  duckdb::idx_t Offset() const noexcept { return _offset; }

  template<bool ToLower>
  void Push(std::string_view token) {
    if (_offset >= duckdb::ListVector::GetListCapacity(_result_list)) {
      duckdb::ListVector::SetListSize(_result_list, _offset);
      duckdb::ListVector::Reserve(
        _result_list, duckdb::ListVector::GetListCapacity(_result_list) * 2);
    }
    auto* data =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(_result_child);
    if constexpr (ToLower) {
      auto str = duckdb::StringVector::EmptyString(_result_child, token.size());
      absl::ascii_internal::AsciiStrToLower(str.GetDataWriteable(),
                                            token.data(), token.size());
      str.Finalize();
      data[_offset] = str;
    } else {
      data[_offset] = duckdb::StringVector::AddStringOrBlob(
        _result_child, token.data(), token.size());
    }
    ++_offset;
  }

 private:
  duckdb::Vector& _result_list;
  duckdb::Vector& _result_child;
  duckdb::idx_t _offset = 0;
};

template<typename PushRow>
void Split(duckdb::DataChunk& args, duckdb::Vector& result, PushRow push_row) {
  const auto row_count = args.size();
  duckdb::UnifiedVectorFormat text_format;
  args.data[0].ToUnifiedFormat(row_count, text_format);
  auto* text_values =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  ListSink sink{result};

  for (duckdb::idx_t row = 0; row < row_count; row++) {
    auto text_row = text_format.sel->get_index(row);
    if (!text_format.validity.RowIsValid(text_row)) {
      result_validity.SetInvalid(row);
      list_entries[row] = {sink.Offset(), 0};
      continue;
    }
    const auto row_offset = sink.Offset();
    push_row(sink, row, AsView(text_values[text_row]));
    list_entries[row] = {row_offset, sink.Offset() - row_offset};
  }
}

template<bool ToLower>
void SplitConstant(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  Split(args, result, [](ListSink& sink, duckdb::idx_t, std::string_view text) {
    irs::analysis::SplitByNonAlpha(
      text, [&](std::string_view token) { sink.Push<ToLower>(token); });
  });
}

void SplitDynamic(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::UnifiedVectorFormat to_lower_format;
  args.data[1].ToUnifiedFormat(args.size(), to_lower_format);
  auto* to_lower_values =
    duckdb::UnifiedVectorFormat::GetData<bool>(to_lower_format);

  Split(args, result,
        [&](ListSink& sink, duckdb::idx_t row, std::string_view text) {
          auto to_lower_row = to_lower_format.sel->get_index(row);
          const bool to_lower =
            to_lower_format.validity.RowIsValid(to_lower_row) &&
            to_lower_values[to_lower_row];
          irs::analysis::SplitByNonAlpha(text, [&](std::string_view token) {
            if (to_lower) {
              sink.Push<true>(token);
            } else {
              sink.Push<false>(token);
            }
          });
        });
}

duckdb::unique_ptr<duckdb::FunctionData> SplitBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& args = input.GetArguments();
  if (args[1]->IsFoldable()) {
    auto val = duckdb::ExpressionExecutor::EvaluateScalar(
      input.GetClientContext(), *args[1]);
    const bool to_lower = !val.IsNull() && duckdb::BooleanValue::Get(val);
    input.GetBoundFunction().SetFunctionCallback(
      to_lower ? SplitConstant<true> : SplitConstant<false>);
  }
  return nullptr;
}

duckdb::ScalarFunction MakeFn(duckdb::vector<duckdb::LogicalType> args,
                              duckdb::scalar_function_t fn,
                              duckdb::bind_scalar_function_t bind = nullptr) {
  duckdb::ScalarFunction f{
    std::move(args), duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    fn, bind};
  f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  return f;
}

}  // namespace

void RegisterSplitByNonAlpha(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{"split_by_non_alpha"};
  set.AddFunction(MakeFn({duckdb::LogicalType::VARCHAR}, SplitConstant<false>));
  set.AddFunction(
    MakeFn({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN},
           SplitDynamic, SplitBind));
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
