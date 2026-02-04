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

#include <algorithm>
#include <atomic>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/read_locker.h"
#include "basics/recursive_locker.h"
#include "basics/result.h"
#include "basics/static_strings.h"
#include "basics/write_locker.h"
#include "catalog/table.h"
#include "database/ticks.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/indexes_snapshot.h"
#include "utils/query_cache.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "indexes/index.h"
#include "transaction/helpers.h"
#endif

namespace sdb {

catalog::TableMeta MakeTableMeta(const catalog::Table& c) {
  return {
    .database = c.GetDatabaseId(),
    .schema = c.GetSchemaId(),
    .id = c.GetId(),
    .plan_id = c.planId(),
    .plan_db = c.planDb(),
    .from = c.from(),
    .to = c.to(),
    .name = std::string{c.GetName()},
  };
}

TableShard::TableShard(catalog::TableMeta collection_meta,
                       const catalog::TableStats& stats)
  : _collection_meta{std::move(collection_meta)}, _num_rows{stats.num_rows} {}

}  // namespace sdb
