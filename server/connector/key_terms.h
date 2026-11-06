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

#include <iresearch/index/field_meta.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <vector>

namespace sdb::connector {

// One component of a row key, as the index holds it: which of the key column's
// member dictionaries the value lives in, and its term there. A field's
// postings are partitioned by `DictRef` and only then by term, so a lookup
// answers to both; a boolean's value is the member alone, its term being
// empty, and a numeric lives under a member no string ever reaches.
struct KeyTerm {
  irs::DictRef dict;
  irs::bstring term;

  auto operator<=>(const KeyTerm&) const = default;
};

// A whole primary key: one component per key column, in key-column order. Each
// is what a query for that value would look up, so a key built here compares
// against what the index holds.
using KeyTerms = std::vector<KeyTerm>;

}  // namespace sdb::connector
