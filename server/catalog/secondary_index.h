////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <optional>
#include <vector>

#include "basics/down_cast.h"
#include "catalog/index.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

class SecondaryIndex : public Index {
 public:
  SecondaryIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                 ObjectId relation_id, std::string name,
                 std::vector<Column::Id> columns,
                 std::vector<ExpressionData> expressions, bool unique);

  static std::shared_ptr<SecondaryIndex> Deserialize(duckdb::Deserializer& src,
                                                     ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;
  bool IsUnique() const noexcept { return _unique; }

  // Positional ART key list, in source order. A Column::kInvalidId slot is an
  // expression key whose payload is the next unconsumed entry of Expressions().
  // Order (and column/expression interleaving) is the ART prefix order.
  const std::vector<Column::Id>& Columns() const noexcept { return _columns; }
  const std::vector<ExpressionData>& Expressions() const noexcept {
    return _expressions;
  }

 private:
  std::vector<Column::Id> _columns;
  std::vector<ExpressionData> _expressions;
  bool _unique;
};

}  // namespace sdb::catalog
