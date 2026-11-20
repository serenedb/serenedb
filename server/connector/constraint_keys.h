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

#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/index/field_meta.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"
#include "search/key_spec.h"

namespace sdb::connector {

// The dictionary a constraint key's term is written under.
inline constexpr irs::DictRef kConstraintKeyDict{
  .layout = irs::DictLayout::String, .member = irs::DictMember::Strings};

struct KeyRead {
  ColumnId column = 0;
  duckdb::LogicalType type;
};

void CollectReads(const duckdb::Expression& expr, std::vector<KeyRead>& reads);

class KeyEncoder {
 public:
  KeyEncoder(duckdb::Allocator& allocator,
             const duckdb::vector<duckdb::LogicalType>& types);

  void Encode(duckdb::DataChunk& input, duckdb::Vector& keys);

 private:
  std::vector<size_t> _intervals;
  duckdb::DataChunk _normalized;
  duckdb::vector<duckdb::OrderModifiers> _modifiers;
  duckdb::UnifiedVectorFormat _format;
};

class ConstraintKeyEncoder {
 public:
  ConstraintKeyEncoder(duckdb::ClientContext& context, duckdb::idx_t table_id,
                       const catalog::ConstraintKey& key);

  const catalog::ConstraintKey& Key() const noexcept { return _key; }

  std::vector<KeyRead> ExpressionReads() const;

  const duckdb::Vector& Encode(duckdb::DataChunk& chunk,
                               std::span<const ColumnId> column_ids);

 private:
  struct Prepared {
    explicit Prepared(duckdb::ClientContext& context) : executor{context} {}

    std::vector<duckdb::unique_ptr<duckdb::Expression>> parts;
    duckdb::ExpressionExecutor executor;
    duckdb::DataChunk input;
    std::optional<KeyEncoder> encoder;
    duckdb::DataChunk keys;
  };

  void Prepare(const duckdb::DataChunk& chunk,
               std::span<const ColumnId> column_ids);

  duckdb::ClientContext* _context;
  duckdb::idx_t _table_id;
  catalog::ConstraintKey _key;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _expressions;
  std::unique_ptr<Prepared> _prepared;
};

// The key's values as PostgreSQL shows them in a DETAIL: `(a, b)=(1, x)`, to a
// role that may read every key column of `table`.
std::optional<std::string> ConstraintKeyText(
  duckdb::ClientContext& context, const duckdb::RoleAccess& role,
  const duckdb::TableCatalogEntry& table, const search::KeySpec& spec,
  std::string_view key);

std::string EncodeKeyValue(const duckdb::Value& value);

std::string KeyValueText(std::string_view key, const duckdb::LogicalType& type);

}  // namespace sdb::connector
