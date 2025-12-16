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

#include <velox/type/HugeInt.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/type/Type.h>

#include "basics/fwd.h"

namespace sdb::aql {

// TODO: move aql to a separate header

velox::TypePtr COLLECTION();

bool IsCollection(const velox::TypePtr& type);

}  // namespace sdb::aql

namespace sdb::pg {

velox::TypePtr VOID();
bool IsVoid(const velox::TypePtr& type);

velox::TypePtr PROCEDURE();
bool IsProcedure(const velox::TypePtr& type);

velox::TypePtr INTERVAL();
bool IsInterval(const velox::TypePtr& type);
bool IsInterval(const velox::Type& type);

struct IntervalTrait {
  using type = velox::int128_t;
  static constexpr const char* typeName = "PG_INTERVAL";  // NOLINT
};
using Interval = velox::CustomType<IntervalTrait>;

void RegisterTypes();

}  // namespace sdb::pg
