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

#include <vector>

#include "catalog/index.h"

namespace sdb::catalog {

struct SecondaryIndexOptions {
  std::vector<uint16_t> columns;
  bool unique = false;
};

class SecondaryIndex : public Index {
 public:
  using Options = SecondaryIndexOptions;

  SecondaryIndex(IndexOptions<SecondaryIndexOptions> options,
                 ObjectId database_id);

  void WriteInternal(vpack::Builder& builder) const;
  std::span<const uint16_t> GetColumns() const noexcept { return _columns; }
  bool IsUnique() const noexcept { return _unique; }

 private:
  std::vector<uint16_t> _columns;
  bool _unique;
};

}  // namespace sdb::catalog
