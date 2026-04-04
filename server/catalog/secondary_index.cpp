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

#include "catalog/secondary_index.h"

#include <vpack/serializer.h>

namespace sdb::catalog {

SecondaryIndex::SecondaryIndex(ObjectId database_id, ObjectId schema_id,
                               ObjectId id, ObjectId relation_id,
                               std::string name,
                               std::vector<Column::Id> column_ids, bool unique)
  : Index(database_id, schema_id, id, relation_id, std::move(name),
          std::move(column_ids), IndexType::Secondary),
    _unique{unique} {}

std::shared_ptr<SecondaryIndex> SecondaryIndex::ReadInternal(
  vpack::Slice slice, ReadContext ctx) {
  auto name_slice = slice.get("name");
  if (!name_slice.isString()) {
    return nullptr;
  }

  struct BaseOpts {
    std::vector<Column::Id> column_ids;
  };
  BaseOpts base;
  if (auto r = vpack::ReadTupleNothrow(slice.get("base"), base); !r.ok()) {
    return nullptr;
  }

  struct ImplOpts {
    bool unique = false;
  };
  ImplOpts impl;
  if (auto r = vpack::ReadTupleNothrow(slice.get("impl"), impl); !r.ok()) {
    return nullptr;
  }

  return std::make_shared<SecondaryIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::string{name_slice.stringView()}, std::move(base.column_ids),
    impl.unique);
}

void SecondaryIndex::WriteInternal(vpack::Builder& b) const {
  WriteObject(b, [&](vpack::Builder& b) {
    struct BaseOpts {
      IndexType type;
      std::span<const Column::Id> column_ids;
    };
    b.add("base");
    vpack::WriteTuple(b, BaseOpts{.type = GetIndexType(), .column_ids = _column_ids});
    struct ImplOpts {
      bool unique;
    };
    b.add("impl");
    vpack::WriteTuple(b, ImplOpts{.unique = _unique});
  });
}

}  // namespace sdb::catalog
