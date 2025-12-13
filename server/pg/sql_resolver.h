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

#include "basics/containers/flat_hash_set.h"
#include "pg/sql_collector.h"
#include "query/config.h"

namespace sdb::pg {

using Disallowed = containers::FlatHashSet<Objects::ObjectName>;

void ResolveQueryView(ObjectId database,
                      std::span<const std::string> search_path,
                      Objects& objects, Disallowed& disallowed,
                      const Objects& query);

void ResolveFunction(ObjectId database,
                     std::span<const std::string> search_path, Objects& objects,
                     Disallowed& disallowed, const Objects& query);

void Resolve(ObjectId database, Objects& objects, const Config& config);

}  // namespace sdb::pg
