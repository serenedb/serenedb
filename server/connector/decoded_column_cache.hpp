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
#include <duckdb/planner/expression.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <memory>
#include <optional>
#include <vector>

namespace sdb::connector {

// One numeric column of one segment decoded into a plain array. A predicate
// over every row of the segment is then a compare per value instead of a walk
// through the codec's blocks, and a caller that asks about single rows can
// look them up. Integers are widened to int64, FLOAT and DOUBLE kept as
// double; a row that is NULL is invalid and passes no predicate.
struct DecodedColumn {
  std::vector<int64_t> ints;
  std::vector<double> reals;
  // Empty when every row is valid.
  std::vector<uint64_t> valid;
  // The valid rows ordered by value: a range predicate finds its rows by two
  // binary searches instead of a compare per row.
  std::vector<uint32_t> order;
  uint64_t rows = 0;

  bool Real() const noexcept { return !reals.empty(); }

  bool Valid(uint64_t row) const noexcept {
    return valid.empty() || ((valid[row / 64] >> (row % 64)) & 1U) != 0;
  }

  size_t Bytes() const noexcept {
    return ints.size() * sizeof(int64_t) + reals.size() * sizeof(double) +
           valid.size() * sizeof(uint64_t) + order.size() * sizeof(uint32_t);
  }
};

// Decoded columns of live segments, least recently used out past the budget.
// A segment's columns are keyed by its name and version, so a segment that
// was compacted away is never served from its predecessor's entry; the entry
// itself lingers until the budget evicts it.
class DecodedColumnCache {
 public:
  static DecodedColumnCache& Instance();

  // The column decoded, from the cache or decoded now; nullptr when the
  // column's type is not a plain number.
  std::shared_ptr<const DecodedColumn> Get(const irs::SubReader& segment,
                                           const irs::ColReader& columns,
                                           irs::field_id field,
                                           size_t budget_bytes);

  size_t Bytes() const noexcept;

 private:
  struct Entry {
    std::shared_ptr<const DecodedColumn> column;
    uint64_t used = 0;
  };

  class Impl;
  std::unique_ptr<Impl> _impl;

  DecodedColumnCache();
  ~DecodedColumnCache();
};

// A column predicate compiled from a pushed filter expression: one closed or
// open interval per column (a comparison, or an AND of comparisons on the
// same column), evaluated on the decoded values.
struct DecodedPredicate {
  std::shared_ptr<const DecodedColumn> column;
  // Integer bounds are inclusive after adjusting for strict comparisons; real
  // bounds keep their own inclusiveness.
  int64_t lo = std::numeric_limits<int64_t>::min();
  int64_t hi = std::numeric_limits<int64_t>::max();
  double dlo = -std::numeric_limits<double>::infinity();
  double dhi = std::numeric_limits<double>::infinity();
  bool dlo_inclusive = true;
  bool dhi_inclusive = true;

  bool Pass(uint64_t row) const noexcept {
    if (!column->Valid(row)) {
      return false;
    }
    if (column->Real()) {
      const double v = column->reals[row];
      return (dlo_inclusive ? v >= dlo : v > dlo) &&
             (dhi_inclusive ? v <= dhi : v < dhi);
    }
    const int64_t v = column->ints[row];
    return v >= lo && v <= hi;
  }

  // Clears every bit of `mask[0..words)` (bit i is row `first + i`) whose row
  // fails; returns the survivors.
  uint64_t Narrow(uint64_t first, uint64_t* mask,
                  uint32_t words) const noexcept;

  // The rows that pass, as [begin, end) into the column's value order.
  std::pair<uint32_t, uint32_t> Range() const noexcept;

  // Sets the bit of every passing row in `words` (bit i is row i).
  void Fill(uint64_t* words) const noexcept;
};

// The predicate `expr` (a pushed ExpressionFilter's expression over one
// column) compiled against `column`, or nullopt when its shape is not a plain
// interval on that column.
std::optional<DecodedPredicate> CompileDecodedPredicate(
  const duckdb::Expression& expr, std::shared_ptr<const DecodedColumn> column);

}  // namespace sdb::connector
