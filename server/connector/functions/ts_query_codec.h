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

#pragma once

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace duckdb {

class ClientContext;
class Expression;

}  // namespace duckdb
namespace sdb::connector {

inline constexpr duckdb::idx_t kTSQueryTextChild = 0;
inline constexpr duckdb::idx_t kTSQueryTokenizerChild = 1;
inline constexpr duckdb::idx_t kTSQueryBoostChild = 2;
inline constexpr duckdb::idx_t kTSQuerySlopChild = 3;
inline constexpr duckdb::idx_t kTSQueryScorerChild = 4;
inline constexpr duckdb::idx_t kTSQueryMergeChild = 5;

bool IsTSQueryStructType(const duckdb::LogicalType& type);

enum class TSQueryMerge : uint8_t {
  Default = 0,
  Sum,
  Max,
};

}  // namespace sdb::connector
namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<sdb::connector::TSQueryMerge>(
  sdb::connector::TSQueryMerge value) noexcept {
  using Merge = sdb::connector::TSQueryMerge;
  switch (value) {
    case Merge::Default:
      return "default";
    case Merge::Sum:
      return "sum";
    case Merge::Max:
      return "max";
  }
  return invalid_tag;
}

}  // namespace magic_enum
namespace sdb::connector {

template<typename Str>
struct TSQueryFields {
  Str text;
  Str tokenizer;
  Str scorer;
  int64_t slop = 0;
  float boost = 1.0f;
  TSQueryMerge merge = TSQueryMerge::Default;
};

using TSQueryParts = TSQueryFields<std::string>;
using TSQueryRowView = TSQueryFields<std::string_view>;

std::optional<TSQueryParts> TryGetTSQueryParts(const duckdb::Value& value);

TSQueryParts TSQueryPartsForType(const duckdb::LogicalType& type,
                                 std::string_view text);

duckdb::Value MakeTSQueryValue(const duckdb::LogicalType& type,
                               std::string_view text);

std::string RenderTSQueryPartsSQL(const TSQueryParts& parts);

std::string RenderTSQueryValueText(const TSQueryParts& parts);

std::optional<duckdb::Value> TryFoldTSQueryCall(
  duckdb::ClientContext& context, std::string_view name,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> children);

duckdb::unique_ptr<duckdb::Expression> TryParseStructuredTSQueryText(
  std::string_view text, duckdb::ClientContext& context);

}  // namespace sdb::connector
