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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <optional>
#include <span>
#include <vector>

#include "basics/serializer.h"
#include "catalog/column_id.h"
#include "catalog/entry.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"

namespace sdb::catalog {

// Which engine owns the table's row data. Both kinds are first-class and
// coexist: Transactional tables live as store tables in the engine's
// single-file database; Search is the eventually-consistent iresearch-only
// table engine.
enum class TableEngine : uint8_t {
  Transactional = 0,
  Search = 1,
};

}  // namespace sdb::catalog

// ObjectFormat (JSON) renders for the types whose tuple format goes through
// duckdb member Serialize: named fields. The binary format is untouched
// (overloads are constrained to ObjectFormat).
namespace duckdb {

template<typename Context>
  requires std::is_same_v<typename Context::Format, sdb::basics::ObjectFormat>
void SerdeWrite(Context ctx, const LogicalType& type) {
  sdb::basics::detail::WriteString(ctx.io(), type.ToString());
}

}  // namespace duckdb
