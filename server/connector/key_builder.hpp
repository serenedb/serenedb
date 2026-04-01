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

#include <velox/type/Type.h>
#include <velox/type/Variant.h>

#include <span>

#include "basics/fwd.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/secondary_sink_writer.hpp"

namespace sdb::connector {

class PrimaryKeyBuilder {
 public:
  PrimaryKeyBuilder(ObjectId table_id, velox::RowTypePtr pk_type)
    : _table_id{table_id}, _pk_type{std::move(pk_type)} {}

  void BuildFullKey(std::string& key, catalog::Column::Id column_id,
                    std::span<const velox::variant> values) const {
    key_utils::AppendTableKey(key, _table_id);
    key_utils::AppendColumnKey(key, column_id);
    primary_key::Create(values, *_pk_type, key);
  }

  ObjectId _table_id;
  velox::RowTypePtr _pk_type;
};

class SecondaryKeyBuilder {
 public:
  SecondaryKeyBuilder(ObjectId shard_id, velox::RowTypePtr sk_type)
    : _shard_id{shard_id}, _sk_type{std::move(sk_type)} {}

  void BuildFullKey(std::string& key, catalog::Column::Id column_id,
                    std::span<const velox::variant> values) const {
    secondary_key::AppendShardPrefix(key, _shard_id);
    secondary_key::AppendDummyColumnId(key);
    for (size_t i = 0; i < values.size(); ++i) {
      if (values[i].isNull()) {
        secondary_key::AppendNullMarker(key);
      } else {
        secondary_key::AppendNotNullMarker(key);
        primary_key::AppendVariantValue(key, values[i], _sk_type->childAt(i));
      }
    }
  }

  ObjectId _shard_id;
  velox::RowTypePtr _sk_type;
};

}  // namespace sdb::connector
