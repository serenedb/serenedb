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

#include "catalog/index.h"

namespace sdb::catalog {

SecondaryIndex::SecondaryIndex(IndexOptions<SecondaryIndexOptions> options,
                               ObjectId database_id)
  : Index(std::move(options.base), database_id),
    _columns{std::move(options.impl.columns)},
    _unique{options.impl.unique} {}

void SecondaryIndex::WriteInternal(vpack::Builder& builder) const {
  IndexOptions<SecondaryIndexOptions> options{
    .base =
      {
        .id = GetId(),
        .relation_id = GetRelationId(),
        .name = std::string{GetName()},
        .type = GetIndexType(),
      },
    .impl =
      {
        .columns = std::vector<uint16_t>{_columns.begin(), _columns.end()},
        .unique = _unique,
      },
  };

  vpack::WriteTuple(builder, options);
}

}  // namespace sdb::catalog
