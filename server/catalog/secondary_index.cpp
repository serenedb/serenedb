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

SecondaryIndex::SecondaryIndex(IndexOptions<SecondaryIndexOptions> options)
  : Index(std::move(options.base)), _unique{options.impl.unique} {}

void SecondaryIndex::WriteInternal(vpack::Builder& builder) const {
  Index::WriteInternal(builder);
  SecondaryIndexOptions options{
    .unique = _unique,
  };

  vpack::WriteTuple(builder, options);
}

}  // namespace sdb::catalog
