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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/index/field_meta.hpp>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"

namespace sdb::connector {

// The dictionary a constraint key's term is written under.
inline constexpr irs::DictRef kConstraintKeyDict{
  .layout = irs::DictLayout::String, .member = irs::DictMember::Strings};

struct KeyRead {
  ColumnId column = 0;
  duckdb::LogicalType type;
};

class ConstraintKeyEncoder {
 public:
  ConstraintKeyEncoder(duckdb::ClientContext& context, duckdb::idx_t table_id,
                       const catalog::ConstraintKey& key);

  const catalog::ConstraintKey& Key() const noexcept { return _key; }

  std::vector<KeyRead> ExpressionReads() const;

  void Encode(duckdb::DataChunk& chunk, std::span<const ColumnId> column_ids,
              duckdb::Vector& keys) const;

 private:
  duckdb::ClientContext* _context;
  duckdb::idx_t _table_id;
  catalog::ConstraintKey _key;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _expressions;
};

// The key's values as PostgreSQL shows them in a DETAIL: `(a, b)=(1, x)`.
std::string ConstraintKeyText(duckdb::ClientContext& context,
                              std::string_view key,
                              std::span<const std::string> names,
                              std::span<const duckdb::LogicalType> types);

}  // namespace sdb::connector
