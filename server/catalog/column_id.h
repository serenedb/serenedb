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

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <limits>
#include <string>
#include <string_view>

#include "catalog/identifiers/object_id.h"
#include "query/utils.h"

namespace sdb::catalog {

// A column's identity, stable across a drop of the column before it: the same
// number duckdb's ColumnDefinition::HostId() carries and pg_attribute keys on.
// Not a definition of its own -- a column is a member of the relation entry --
// so the id and the reserved ids below stand on their own here, reachable
// without the relation.
using ColumnId = ObjectId;

// Ids above this belong to the synthetic columns below; a real column never
// reaches it.
inline constexpr uint64_t kMaxRealColumnIdValue =
  std::numeric_limits<uint64_t>::max() - 1'000'000;

// The synthetic primary key of a relation that declares none.
inline constexpr ColumnId kGeneratedPKId{kMaxRealColumnIdValue + 1};

// The columns an inverted-index scan produces rather than reads: the match
// score, term offsets, and the per-term dictionary projections.
inline constexpr ColumnId kInvertedIndexScoreId{kMaxRealColumnIdValue + 2};
inline constexpr ColumnId kInvertedIndexOffsetsId{kMaxRealColumnIdValue + 3};
inline constexpr ColumnId kInvertedIndexTermId{kMaxRealColumnIdValue + 4};
inline constexpr ColumnId kInvertedIndexTermCountId{kMaxRealColumnIdValue + 5};
inline constexpr ColumnId kInvertedIndexTermFreqId{kMaxRealColumnIdValue + 6};
inline constexpr ColumnId kInvertedIndexTermScoreId{kMaxRealColumnIdValue + 7};
inline constexpr ColumnId kInvertedIndexTermRawId{kMaxRealColumnIdValue + 8};

// Sentinel for "no/invalid column id". numeric_limits<ColumnId>::max() does NOT
// work (ObjectId is a class wrapping uint64_t, not an arithmetic type).
inline constexpr ColumnId kInvalidColumnId{
  std::numeric_limits<uint64_t>::max()};

inline constexpr std::string_view kScoreName = "sdb_inverted_index_score";
inline constexpr std::string_view kTermName = "sdb_inverted_index_term$";
inline constexpr std::string_view kTermRawName = "sdb_inverted_index_term_raw$";
inline constexpr std::string_view kTermCountName =
  "sdb_inverted_index_term_count$";
inline constexpr std::string_view kTermFreqName =
  "sdb_inverted_index_term_freq$";
inline constexpr std::string_view kTermScoreName =
  "sdb_inverted_index_term_score$";
// Prefix used in virtual offsets column names. Ends with kReservedSymbol so it
// can never collide with a user-defined column name.
inline constexpr std::string_view kOffsetsNamePrefix =
  "sdb_inverted_index_offsets$";

inline std::string MakeOffsetsName(ColumnId column_id) {
  static_assert(kOffsetsNamePrefix.ends_with(query::kReservedSymbol));
  return absl::StrCat(kOffsetsNamePrefix, column_id.id());
}

// LIST(INTEGER) -- flat offsets column: interleaved start,end pairs.
inline duckdb::LogicalType MakeOffsetsType() {
  return duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
}

}  // namespace sdb::catalog
