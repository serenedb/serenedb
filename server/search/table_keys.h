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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types.hpp>
#include <iresearch/formats/term_reader.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/index_reader.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "search/hot_keys.h"
#include "search/table.h"

namespace sdb::catalog {
class SearchTableEntry;
}  // namespace sdb::catalog

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
  // Reference only: the referenced table and the field of its unique key the
  // values must be held in. The key's columns follow that unique key's order.
  duckdb::Identifier referenced_schema;
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

TableKeys ResolveTableKeys(const catalog::SearchTableEntry& table,
                           duckdb::CatalogTransaction transaction);

// The table reference key `spec` of `table` references, as `transaction`
// sees it.
const catalog::SearchTableEntry* ReferencedTable(
  const catalog::SearchTableEntry& table, const KeySpec& spec,
  duckdb::CatalogTransaction transaction);

struct ReferencingKey {
  catalog::SearchTableEntry* table = nullptr;
  const KeySpec* spec = nullptr;
};

// The foreign keys among `referencing` that reference unique key `field` of
// `table` and are still declared, as `transaction` sees them.
std::vector<ReferencingKey> LiveReferencing(
  const catalog::SearchTableEntry& table,
  std::span<const std::shared_ptr<const Referencing>> referencing,
  irs::field_id field, duckdb::CatalogTransaction transaction);

// The unique key of index `name` on `columns` of `table`, written under
// `field`.
KeySpec UniqueIndexKey(const catalog::SearchTableEntry& table,
                       irs::field_id field, std::string name,
                       std::span<const duckdb::Identifier> columns);

// The unique key `key` of `table` if it is an index's, committed or not: a
// table's store writes and checks the key from the start of the index's
// build.
std::optional<KeySpec> IndexKeySpec(const catalog::SearchTableEntry& table,
                                    const catalog::ConstraintKey& key,
                                    duckdb::CatalogTransaction transaction);

// Refuses rows of `reader` sharing unique key `spec`, as a new index on it.
void VerifyUniqueKey(const irs::DirectoryReader& reader, const KeySpec& spec);

// Whether a row is gone: deleted by a commit, or removed by the transaction
// asking.
struct RowMasks {
  const irs::DocumentMask* deleted = nullptr;
  const irs::DocumentMaskBuilder* removed = nullptr;

  bool Contains(irs::doc_id_t doc) const noexcept {
    return (deleted && deleted->Contains(doc)) ||
           (removed && removed->Contains(doc));
  }
};

// A row holding a key: its unit and its document there.
struct KeyHolder {
  uint64_t unit = 0;
  irs::doc_id_t doc = 0;
};

// The live rows holding a key, read from units written as segments through
// the term dictionary of the key's field, and from hot units and buffered rows
// through their key sets.
class KeyProbe {
 public:
  struct Segment {
    uint64_t unit = 0;
    const irs::SubReader* sub = nullptr;
    RowMasks masks;
    // Rows before `first` are not answered.
    irs::doc_id_t first = 0;
  };

  struct Hot {
    uint64_t unit = 0;
    const HotKeys* keys = nullptr;
    // Rows in [first, docs) are the ones answered.
    irs::doc_id_t first = 0;
    irs::doc_id_t docs = 0;
    RowMasks masks;
  };

  explicit KeyProbe(irs::field_id field) : _field{field} {}

  void Add(const Segment& segment);
  void Add(const Hot& hot) { _hots.push_back(hot); }
  // The units of `live`; with `since`, only the rows `since` lacks: all of a
  // unit it does not hold, and the rows a hot unit gained after it.
  void AddLive(const LiveUnits& live, const LiveUnits* since = nullptr);

  bool Empty() const noexcept { return _segments.empty() && _hots.empty(); }

  // Visits the live rows holding `key` until `visit` returns false.
  void ForEach(std::string_view key,
               absl::FunctionRef<bool(const KeyHolder&)> visit);
  bool Exists(std::string_view key);

 private:
  struct Source {
    uint64_t unit = 0;
    const irs::TermReader* field = nullptr;
    irs::DocumentMask::Iterator mask;
    RowMasks masks;
    irs::doc_id_t first = 0;
  };

  irs::field_id _field;
  std::vector<Source> _segments;
  std::vector<Hot> _hots;
};

// The keys of the live rows of `segments` under `field`, in ascending order:
// `visit` sees each key with the number of live rows holding it, and stops the
// walk by returning false.
void ForEachSegmentKey(
  std::span<const KeyProbe::Segment> segments, irs::field_id field,
  absl::FunctionRef<bool(std::string_view key, size_t rows)> visit);

}  // namespace sdb::search
