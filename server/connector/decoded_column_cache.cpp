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

#include "connector/decoded_column_cache.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <limits>
#include <string>

namespace sdb::connector {
namespace {

template<typename T>
void AppendInts(DecodedColumn& out, const duckdb::Vector& vec, size_t n) {
  const auto* data = duckdb::FlatVector::GetData<T>(vec);
  for (size_t i = 0; i < n; ++i) {
    out.ints.push_back(static_cast<int64_t>(data[i]));
  }
}

template<typename T>
void AppendReals(DecodedColumn& out, const duckdb::Vector& vec, size_t n) {
  const auto* data = duckdb::FlatVector::GetData<T>(vec);
  for (size_t i = 0; i < n; ++i) {
    out.reals.push_back(static_cast<double>(data[i]));
  }
}

bool PlainNumber(duckdb::PhysicalType type) noexcept {
  switch (type) {
    case duckdb::PhysicalType::INT8:
    case duckdb::PhysicalType::INT16:
    case duckdb::PhysicalType::INT32:
    case duckdb::PhysicalType::INT64:
    case duckdb::PhysicalType::UINT8:
    case duckdb::PhysicalType::UINT16:
    case duckdb::PhysicalType::UINT32:
    case duckdb::PhysicalType::FLOAT:
    case duckdb::PhysicalType::DOUBLE:
      return true;
    default:
      return false;
  }
}

std::shared_ptr<const DecodedColumn> Decode(const irs::ColReader& columns,
                                            const irs::ColumnReader& reader) {
  const auto& type = reader.Type();
  if (!PlainNumber(type.InternalType())) {
    return nullptr;
  }
  auto out = std::make_shared<DecodedColumn>();
  out->rows = reader.RowCount();
  const bool real = type.InternalType() == duckdb::PhysicalType::FLOAT ||
                    type.InternalType() == duckdb::PhysicalType::DOUBLE;
  (real ? out->reals.reserve(out->rows) : out->ints.reserve(out->rows));
  irs::ReadContext ctx{columns};
  auto scan = reader.InitScan(ctx);
  std::vector<uint64_t> valid;
  bool any_null = false;
  for (uint64_t row = 0; row < out->rows;) {
    const auto want = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, out->rows - row));
    duckdb::Vector vec{type, want};
    const auto got = reader.Scan(scan, vec, want);
    if (got == 0) {
      break;
    }
    vec.Flatten(got);
    switch (type.InternalType()) {
      case duckdb::PhysicalType::INT8:
        AppendInts<int8_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::INT16:
        AppendInts<int16_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::INT32:
        AppendInts<int32_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::INT64:
        AppendInts<int64_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::UINT8:
        AppendInts<uint8_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::UINT16:
        AppendInts<uint16_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::UINT32:
        AppendInts<uint32_t>(*out, vec, got);
        break;
      case duckdb::PhysicalType::FLOAT:
        AppendReals<float>(*out, vec, got);
        break;
      case duckdb::PhysicalType::DOUBLE:
        AppendReals<double>(*out, vec, got);
        break;
      default:
        return nullptr;
    }
    const auto& validity = duckdb::FlatVector::Validity(vec);
    if (!validity.AllValid()) {
      if (!any_null) {
        any_null = true;
        valid.assign((out->rows + 63) / 64, ~uint64_t{0});
      }
      for (duckdb::idx_t i = 0; i < got; ++i) {
        if (!validity.RowIsValid(i)) {
          const auto r = row + i;
          valid[r / 64] &= ~(uint64_t{1} << (r % 64));
        }
      }
    }
    row += got;
  }
  if (any_null) {
    // Rows past the end read as valid: a caller never asks about them.
    out->valid = std::move(valid);
  }
  const auto decoded = real ? out->reals.size() : out->ints.size();
  if (decoded != out->rows) {
    return nullptr;
  }
  out->order.reserve(out->rows);
  for (uint64_t r = 0; r < out->rows; ++r) {
    if (out->Valid(r)) {
      out->order.push_back(static_cast<uint32_t>(r));
    }
  }
  if (real) {
    const auto* v = out->reals.data();
    std::sort(out->order.begin(), out->order.end(),
              [v](uint32_t a, uint32_t b) { return v[a] < v[b]; });
  } else {
    const auto* v = out->ints.data();
    std::sort(out->order.begin(), out->order.end(),
              [v](uint32_t a, uint32_t b) { return v[a] < v[b]; });
  }
  return out;
}

}  // namespace

class DecodedColumnCache::Impl {
 public:
  std::shared_ptr<const DecodedColumn> Get(const irs::SubReader& segment,
                                           const irs::ColReader& columns,
                                           irs::field_id field,
                                           size_t budget_bytes) {
    const auto& meta = segment.Meta();
    const auto key = absl::StrCat(meta.name, "@", meta.version, "#", field);
    {
      absl::MutexLock lock{&_mutex};
      if (auto it = _entries.find(key); it != _entries.end()) {
        it->second.used = ++_tick;
        return it->second.column;
      }
    }
    // Decoded outside the lock: a second query for the same column decodes
    // it again rather than wait, and the first stored copy wins.
    const auto* reader = columns.Column(field);
    if (reader == nullptr) {
      return nullptr;
    }
    auto column = Decode(columns, *reader);
    if (!column) {
      return nullptr;
    }
    absl::MutexLock lock{&_mutex};
    if (auto it = _entries.find(key); it != _entries.end()) {
      it->second.used = ++_tick;
      return it->second.column;
    }
    const auto bytes = column->Bytes();
    if (bytes > budget_bytes) {
      // Too large for the cache: served once, never kept.
      return column;
    }
    while (_bytes + bytes > budget_bytes && !_entries.empty()) {
      auto victim = _entries.begin();
      for (auto it = _entries.begin(); it != _entries.end(); ++it) {
        if (it->second.used < victim->second.used) {
          victim = it;
        }
      }
      _bytes -= victim->second.column->Bytes();
      _entries.erase(victim);
    }
    _bytes += bytes;
    _entries.emplace(key, Entry{.column = column, .used = ++_tick});
    return column;
  }

  size_t Bytes() const noexcept {
    absl::MutexLock lock{&_mutex};
    return _bytes;
  }

 private:
  mutable absl::Mutex _mutex;
  absl::flat_hash_map<std::string, Entry> _entries;
  size_t _bytes = 0;
  uint64_t _tick = 0;
};

DecodedColumnCache::DecodedColumnCache() : _impl{std::make_unique<Impl>()} {}
DecodedColumnCache::~DecodedColumnCache() = default;

DecodedColumnCache& DecodedColumnCache::Instance() {
  static DecodedColumnCache cache;
  return cache;
}

std::shared_ptr<const DecodedColumn> DecodedColumnCache::Get(
  const irs::SubReader& segment, const irs::ColReader& columns,
  irs::field_id field, size_t budget_bytes) {
  return _impl->Get(segment, columns, field, budget_bytes);
}

size_t DecodedColumnCache::Bytes() const noexcept { return _impl->Bytes(); }

uint64_t DecodedPredicate::Narrow(uint64_t first, uint64_t* mask,
                                  uint32_t words) const noexcept {
  uint64_t total = 0;
  const auto rows = column->rows;
  for (uint32_t w = 0; w < words; ++w) {
    uint64_t m = mask[w];
    if (m == 0) {
      continue;
    }
    const uint64_t row0 = first + uint64_t{w} * 64;
    uint64_t keep = 0;
    if (column->Real() || !column->valid.empty() || row0 + 64 > rows) {
      for (uint64_t rest = m; rest != 0; rest &= rest - 1) {
        const auto bit = static_cast<unsigned>(std::countr_zero(rest));
        const auto row = row0 + bit;
        if (row < rows && Pass(row)) {
          keep |= uint64_t{1} << bit;
        }
      }
    } else {
      // A whole word of integers with no nulls: one compare per value, the
      // shape the compiler vectorises.
      const int64_t* v = column->ints.data() + row0;
      for (unsigned i = 0; i < 64; ++i) {
        keep |= static_cast<uint64_t>(v[i] >= lo && v[i] <= hi) << i;
      }
    }
    m &= keep;
    mask[w] = m;
    total += static_cast<uint64_t>(std::popcount(m));
  }
  return total;
}

std::pair<uint32_t, uint32_t> DecodedPredicate::Range() const noexcept {
  const auto& order = column->order;
  if (column->Real()) {
    const auto* v = column->reals.data();
    const auto lo_it =
      dlo_inclusive
        ? std::lower_bound(order.begin(), order.end(), dlo,
                           [v](uint32_t a, double x) { return v[a] < x; })
        : std::upper_bound(order.begin(), order.end(), dlo,
                           [v](double x, uint32_t a) { return x < v[a]; });
    const auto hi_it =
      dhi_inclusive
        ? std::upper_bound(order.begin(), order.end(), dhi,
                           [v](double x, uint32_t a) { return x < v[a]; })
        : std::lower_bound(order.begin(), order.end(), dhi,
                           [v](uint32_t a, double x) { return v[a] < x; });
    const auto b = static_cast<uint32_t>(lo_it - order.begin());
    const auto e = static_cast<uint32_t>(hi_it - order.begin());
    return {b, std::max(b, e)};
  }
  const auto* v = column->ints.data();
  const auto lo_it =
    std::lower_bound(order.begin(), order.end(), lo,
                     [v](uint32_t a, int64_t x) { return v[a] < x; });
  const auto hi_it =
    std::upper_bound(order.begin(), order.end(), hi,
                     [v](int64_t x, uint32_t a) { return x < v[a]; });
  const auto b = static_cast<uint32_t>(lo_it - order.begin());
  const auto e = static_cast<uint32_t>(hi_it - order.begin());
  return {b, std::max(b, e)};
}

void DecodedPredicate::Fill(uint64_t* words) const noexcept {
  const auto [b, e] = Range();
  const auto& order = column->order;
  for (uint32_t i = b; i < e; ++i) {
    const auto row = order[i];
    words[row / 64] |= uint64_t{1} << (row % 64);
  }
}

namespace {

bool IsColumn(const duckdb::Expression& e) noexcept {
  return e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF ||
         e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF;
}

// Tightens `p` by `column <op> constant`; false when the constant is not a
// finite number of the column's kind.
bool Bound(DecodedPredicate& p, duckdb::ExpressionType op,
           const duckdb::Value& constant) {
  if (constant.IsNull()) {
    return false;
  }
  const auto& type = constant.type();
  if (!type.IsNumeric()) {
    return false;
  }
  if (p.column->Real()) {
    double c;
    try {
      c = constant.GetValue<double>();
    } catch (...) {
      return false;
    }
    if (!std::isfinite(c)) {
      return false;
    }
    switch (op) {
      case duckdb::ExpressionType::COMPARE_EQUAL:
        p.dlo = std::max(p.dlo, c);
        p.dhi = std::min(p.dhi, c);
        return true;
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        if (c >= p.dlo) {
          p.dlo = c;
          p.dlo_inclusive = false;
        }
        return true;
      case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        if (c > p.dlo) {
          p.dlo = c;
          p.dlo_inclusive = true;
        }
        return true;
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
        if (c <= p.dhi) {
          p.dhi = c;
          p.dhi_inclusive = false;
        }
        return true;
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        if (c < p.dhi) {
          p.dhi = c;
          p.dhi_inclusive = true;
        }
        return true;
      default:
        return false;
    }
  }
  // An integer column compared with a fractional constant is left to the
  // codec path; the integral kinds cast exactly.
  if (type.id() == duckdb::LogicalTypeId::FLOAT ||
      type.id() == duckdb::LogicalTypeId::DOUBLE ||
      type.id() == duckdb::LogicalTypeId::DECIMAL) {
    return false;
  }
  int64_t c;
  try {
    c = constant.GetValue<int64_t>();
  } catch (...) {
    return false;
  }
  constexpr auto kMin = std::numeric_limits<int64_t>::min();
  constexpr auto kMax = std::numeric_limits<int64_t>::max();
  switch (op) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      p.lo = std::max(p.lo, c);
      p.hi = std::min(p.hi, c);
      return true;
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      if (c == kMax) {
        p.lo = kMax;
        p.hi = kMin;  // nothing passes
      } else {
        p.lo = std::max(p.lo, c + 1);
      }
      return true;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      p.lo = std::max(p.lo, c);
      return true;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      if (c == kMin) {
        p.lo = kMax;
        p.hi = kMin;
      } else {
        p.hi = std::min(p.hi, c - 1);
      }
      return true;
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      p.hi = std::min(p.hi, c);
      return true;
    default:
      return false;
  }
}

duckdb::ExpressionType Flip(duckdb::ExpressionType op) noexcept {
  switch (op) {
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return duckdb::ExpressionType::COMPARE_LESSTHAN;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      return duckdb::ExpressionType::COMPARE_GREATERTHAN;
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    default:
      return op;
  }
}

bool Compile(DecodedPredicate& p, const duckdb::Expression& e) {
  switch (e.GetExpressionClass()) {
    case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
      const auto& conj = e.Cast<duckdb::BoundConjunctionExpression>();
      if (conj.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_AND) {
        return false;
      }
      for (const auto& child : conj.GetChildren()) {
        if (!Compile(p, *child)) {
          return false;
        }
      }
      return true;
    }
    case duckdb::ExpressionClass::BOUND_FUNCTION: {
      // A comparison is a function expression in this duckdb; the helper
      // names its operands.
      if (!duckdb::BoundComparisonExpression::IsComparison(e)) {
        return false;
      }
      const auto& fn = e.Cast<duckdb::BoundFunctionExpression>();
      const auto& left = duckdb::BoundComparisonExpression::Left(fn);
      const auto& right = duckdb::BoundComparisonExpression::Right(fn);
      const auto op = e.GetExpressionType();
      if (IsColumn(left) && right.GetExpressionClass() ==
                              duckdb::ExpressionClass::BOUND_CONSTANT) {
        return Bound(p, op,
                     right.Cast<duckdb::BoundConstantExpression>().GetValue());
      }
      if (IsColumn(right) && left.GetExpressionClass() ==
                               duckdb::ExpressionClass::BOUND_CONSTANT) {
        return Bound(p, Flip(op),
                     left.Cast<duckdb::BoundConstantExpression>().GetValue());
      }
      return false;
    }
    default:
      return false;
  }
}

}  // namespace

std::optional<DecodedPredicate> CompileDecodedPredicate(
  const duckdb::Expression& expr, std::shared_ptr<const DecodedColumn> column) {
  if (!column) {
    return std::nullopt;
  }
  DecodedPredicate p;
  p.column = std::move(column);
  if (!Compile(p, expr)) {
    return std::nullopt;
  }
  return p;
}

}  // namespace sdb::connector
