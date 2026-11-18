////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"

namespace sdb::search {

enum class KeyKind : uint8_t {
  Unique,
  Reference,
};

// A key a table enforces: the rows' sort key over `key.columns`, written as a
// term under `key.field`. A unique key refuses a second live row holding a
// value; a reference key needs the referenced table to hold every value.
struct KeySpec {
  KeyKind kind = KeyKind::Unique;
  catalog::ConstraintKey key;
  std::string constraint;
  std::vector<std::string> names;
  // The types the key's values are encoded as.
  std::vector<duckdb::LogicalType> types;
  bool deferred = false;
  // Reference only: the referenced table and the field of its unique key the
  // values must be held in. The key's columns follow that unique key's order.
  duckdb::idx_t referenced_table = 0;
  bool self_reference = false;
  duckdb::Identifier referenced;
  irs::field_id referenced_field = 0;
};

// A foreign key of table `table` whose key `field` must be held in the
// referenced table's unique key `referenced`. Every catalog version of the
// referencing table holds its own, and the referenced table's store tracks
// them while any is alive, so a version that no longer declares the key may
// still be counted.
struct Referencing {
  duckdb::idx_t table = 0;
  irs::field_id field = 0;
  irs::field_id referenced = 0;
};

struct TableKeys {
  std::vector<KeySpec> specs;
  std::vector<std::shared_ptr<const Referencing>> referencing;
};

}  // namespace sdb::search
