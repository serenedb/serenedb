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
#include <functional>
#include <memory>
#include <vector>

#include "iresearch/types.hpp"

namespace duckdb {

class DataChunk;
class Vector;

}  // namespace duckdb
namespace irs {

class TextExpression {
 public:
  virtual ~TextExpression() = default;

  virtual duckdb::Vector& Evaluate(duckdb::DataChunk& columns) = 0;
};

struct TextSource {
  using Expression = std::function<std::unique_ptr<TextExpression>()>;

  std::vector<field_id> columns;
  std::vector<duckdb::LogicalType> types;
  Expression expression;

  bool operator==(const TextSource& rhs) const noexcept {
    return columns == rhs.columns && types == rhs.types &&
           static_cast<bool>(expression) == static_cast<bool>(rhs.expression);
  }
};

}  // namespace irs
