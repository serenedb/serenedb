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

#include "catalog/index.h"

namespace sdb::catalog {

struct SecondaryIndexOptions {
  bool unique = false;
  std::vector<std::string> columns;  // TODO: should be a ref to table column
};

class SecondaryIndex : public Index {
 public:
  SecondaryIndex(SecondaryIndexOptions options, IndexOptions index_options,
                 ObjectId database_id);

  auto& GetOptions() const noexcept { return _options; }

 private:
  SecondaryIndexOptions _options;
};

}  // namespace sdb::catalog
