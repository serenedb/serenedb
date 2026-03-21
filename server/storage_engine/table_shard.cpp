////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "table_shard.h"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/iterator.h>
#include <vpack/slice.h>

#include <atomic>
#include <yaclib/async/make.hpp>

#include "catalog/object.h"
#include "catalog/table.h"

namespace sdb {

TableShard::TableShard(ObjectId id, ObjectId table_id,
                       const catalog::TableStats& stats)
  : catalog::Object{ObjectId{0}, id, "", catalog::ObjectType::TableShard},
    _table_id{table_id},
    _num_rows{stats.num_rows} {}

TableShard::TableShard(ObjectId table_id, const catalog::TableStats& stats)
  : catalog::Object{ObjectId{0}, ObjectId{0}, "",
                    catalog::ObjectType::TableShard},
    _table_id{table_id},
    _num_rows{stats.num_rows} {}

}  // namespace sdb
