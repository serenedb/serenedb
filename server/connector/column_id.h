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

#include <duckdb/common/constants.hpp>
#include <duckdb/common/types.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <string>
#include <string_view>

namespace sdb::connector {

// A column's identity inside an inverted index or a search table: the
// iresearch field/column id its terms and values are written under.
using ColumnId = irs::field_id;

inline constexpr ColumnId kInvalidColumnId = irs::field_limits::invalid();

// Real column ids are duckdb catalog oids and view column positions, all far
// below the threshold; every id above it names a synthetic column no relation
// can ever hold. The values are freshly chosen -- no on-disk format depends on
// them -- and the fifteen slots directly above the threshold are deliberately
// left unnamed, for sentinels that only need an id nothing else can claim.
inline constexpr ColumnId kMaxRealColumnIdValue = 0xFFFF'FFFF;
inline constexpr ColumnId kFirstSyntheticColumnId =
  kMaxRealColumnIdValue + 0x10;

inline constexpr ColumnId kGeneratedPKId = kFirstSyntheticColumnId + 0;
inline constexpr ColumnId kInvertedIndexScoreId = kFirstSyntheticColumnId + 1;
inline constexpr ColumnId kInvertedIndexOffsetsId = kFirstSyntheticColumnId + 2;
inline constexpr ColumnId kInvertedIndexTermId = kFirstSyntheticColumnId + 3;
inline constexpr ColumnId kInvertedIndexTermRawId = kFirstSyntheticColumnId + 4;
inline constexpr ColumnId kInvertedIndexTermFreqId =
  kFirstSyntheticColumnId + 5;
inline constexpr ColumnId kInvertedIndexTermCountId =
  kFirstSyntheticColumnId + 6;
inline constexpr ColumnId kInvertedIndexTermScoreId =
  kFirstSyntheticColumnId + 7;

// The SQL-visible names of those synthetic columns. The score is a whole
// column name; the rest are prefixes a column name is appended to after a '$',
// because one scan can expose the same kind for several indexed columns.
inline constexpr std::string_view kScoreName = "sdb_inverted_index_score";
inline constexpr std::string_view kOffsetsPrefix =
  "sdb_inverted_index_offsets$";
inline constexpr std::string_view kTermName = "sdb_inverted_index_term$";
inline constexpr std::string_view kTermRawName = "sdb_inverted_index_term_raw$";
inline constexpr std::string_view kTermFreqName =
  "sdb_inverted_index_term_freq$";
inline constexpr std::string_view kTermCountName =
  "sdb_inverted_index_term_count$";
inline constexpr std::string_view kTermScoreName =
  "sdb_inverted_index_term_score$";

// Projection-pushdown identifiers for columns no relation stores, continuing
// duckdb's virtual-column space: it begins at VIRTUAL_COLUMN_START (2^63) and
// MultiFileReader::COLUMN_IDENTIFIER_* claims the first three slots. Bound
// plans carry these; nothing persists them.
inline constexpr duckdb::column_t kColumnIdentifierTableOid =
  UINT64_C(9223372036854775811);
inline constexpr duckdb::column_t kColumnIdentifierGeneratedPk =
  UINT64_C(9223372036854775812);
inline constexpr duckdb::column_t kColumnIdentifierPkRowNumber =
  UINT64_C(9223372036854775813);
// One slot per primary-key column, in key order: what a search table answers
// GetRowIdColumns() with, since duckdb identifies a row by virtual columns and
// a stored key column cannot name itself.
inline constexpr duckdb::column_t kColumnIdentifierPrimaryKeyBase =
  UINT64_C(9223372036854775814);

// Offsets come back as one flat list of start/end pairs per row, which is the
// return type ts_offsets is registered with.
inline duckdb::LogicalType MakeOffsetsType() {
  return duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
}

inline std::string MakeOffsetsName(ColumnId column_id) {
  return absl::StrCat(kOffsetsPrefix, column_id);
}

}  // namespace sdb::connector
