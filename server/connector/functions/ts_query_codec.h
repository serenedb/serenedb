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

bool IsTSQueryStructType(const duckdb::LogicalType& type);

struct TSQueryParts {
  std::string text;
  std::string tokenizer;
  float boost = 1.0f;
};

std::optional<TSQueryParts> TryGetTSQueryParts(const duckdb::Value& value);

duckdb::Value MakeTSQueryValue(const duckdb::LogicalType& type,
                               std::string_view text);

std::string RenderTSQuery(std::string_view text, std::string_view tokenizer,
                          float boost);

std::optional<duckdb::Value> TryFoldTSQueryCall(
  duckdb::ClientContext& context, std::string_view name,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> children);

duckdb::unique_ptr<duckdb::Expression> TryParseStructuredTSQueryText(
  std::string_view text, duckdb::ClientContext& context);

}  // namespace sdb::connector
