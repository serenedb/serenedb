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

#include <memory>
#include <optional>
#include <vector>

#include "catalog/index.h"
#include "catalog/persistence/secondary_index.h"

namespace sdb::catalog {

// One secondary (ART) index, in the form a catalog entry is built from.
class CreateSecondaryIndexInfo final : public CreateIndexInfoBase {
 public:
  CreateSecondaryIndexInfo(ObjectId schema_id, ObjectId id,
                           ObjectId relation_id,
                           persistence::SecondaryIndexData data);

  persistence::SecondaryIndexData ToData() const;
  void Serialize(duckdb::Serializer& sink) const final;
  void WriteJson(basics::JsonSink& sink) const final;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static std::shared_ptr<CreateSecondaryIndexInfo> Deserialize(
    duckdb::Deserializer& src, ObjectId schema_id, ObjectId id,
    ObjectId relation_id);

  bool IsUnique() const noexcept { return _unique; }

  // Positional ART key list, in source order. A kInvalidColumnId slot is an
  // expression key whose payload is the next unconsumed entry of Expressions().
  // Order (and column/expression interleaving) is the ART prefix order.
  const std::vector<ColumnId>& Columns() const noexcept { return _key_columns; }
  const std::vector<ExpressionData>& Expressions() const noexcept {
    return _expressions;
  }

 private:
  std::vector<ColumnId> _key_columns;
  std::vector<ExpressionData> _expressions;
  bool _unique;
};

using SecondaryIndexInfoRef = std::shared_ptr<const CreateSecondaryIndexInfo>;

}  // namespace sdb::catalog
