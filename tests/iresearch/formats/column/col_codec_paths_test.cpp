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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/value_operations/value_operations.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_case_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <functional>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "tests_shared.hpp"

namespace {

using duckdb::LogicalType;
using duckdb::Value;
using irs::codecs::CodecType;

constexpr irs::field_id kField = 3;
constexpr std::string_view kSeg = "seg";
constexpr uint32_t kRgSize = 4096;
constexpr uint64_t kRows = 3 * kRgSize + 517;

struct Rows {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

struct Case {
  std::string name;
  LogicalType type;
  CodecType codec;
  std::function<bool(uint64_t)> valid;
  std::function<Value(uint64_t)> value;
  bool stored = true;
  irs::WriteTier tier = irs::WriteTier::Flush;
  uint64_t rows = kRows;
  uint32_t rg_size = kRgSize;
};

uint64_t Mix(uint64_t g) noexcept {
  g += 0x9E3779B97F4A7C15ULL;
  g = (g ^ (g >> 30)) * 0xBF58476D1CE4E5B9ULL;
  g = (g ^ (g >> 27)) * 0x94D049BB133111EBULL;
  return g ^ (g >> 31);
}

bool All(uint64_t) { return true; }
bool Periodic(uint64_t g) { return g % 17 != 0; }
bool NullRuns(uint64_t g) { return (g / 300) % 5 != 0; }
bool Rare(uint64_t g) { return g % 50 == 0; }
bool FirstRowsNull(uint64_t g) { return g % kRgSize >= 700; }

Value Int(const LogicalType& type, int64_t v) {
  return Value::BIGINT(v).DefaultCastAs(type);
}

Value NearMaximum(const LogicalType& type, uint64_t below) {
  const auto top = Value::MaximumValue(type)
                     .DefaultCastAs(LogicalType::HUGEINT)
                     .GetValue<duckdb::hugeint_t>();
  return Value::HUGEINT(top - duckdb::hugeint_t{static_cast<int64_t>(below)})
    .DefaultCastAs(type);
}

Value Wide(const LogicalType& type, uint64_t g) {
  const auto m = Mix(g);
  switch (type.id()) {
    case duckdb::LogicalTypeId::TINYINT:
      return Value::TINYINT(static_cast<int8_t>(m));
    case duckdb::LogicalTypeId::UTINYINT:
      return Value::UTINYINT(static_cast<uint8_t>(m));
    case duckdb::LogicalTypeId::SMALLINT:
      return Value::SMALLINT(static_cast<int16_t>(m));
    case duckdb::LogicalTypeId::USMALLINT:
      return Value::USMALLINT(static_cast<uint16_t>(m));
    case duckdb::LogicalTypeId::INTEGER:
      return Value::INTEGER(static_cast<int32_t>(m));
    case duckdb::LogicalTypeId::UINTEGER:
      return Value::UINTEGER(static_cast<uint32_t>(m));
    case duckdb::LogicalTypeId::BIGINT:
      return Value::BIGINT(static_cast<int64_t>(m));
    case duckdb::LogicalTypeId::UBIGINT:
      return Value::UBIGINT(m);
    case duckdb::LogicalTypeId::HUGEINT:
      return Value::HUGEINT(
        duckdb::hugeint_t{static_cast<int64_t>(m), Mix(g + 1)});
    default:
      return Value::UHUGEINT(duckdb::uhugeint_t{m, Mix(g + 1)});
  }
}

class ColCodecPathsTest : public TestBase {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }

  static std::vector<Value> Expected(const Case& c) {
    std::vector<Value> rows;
    rows.reserve(c.rows);
    for (uint64_t g = 0; g < c.rows; ++g) {
      rows.push_back(c.valid(g) ? c.value(g) : Value{c.type});
    }
    return rows;
  }

  void Write(irs::Directory& dir, const Case& c,
             const std::vector<Value>& rows) {
    irs::ColWriter w{dir, kSeg, Db(), c.tier};
    auto& cw = w.OpenColumn(
      kField, c.type, {.row_group_size = c.rg_size, .compression = c.codec});
    for (uint64_t pos = 0; pos < c.rows;) {
      const auto take =
        std::min<duckdb::idx_t>(c.rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{c.type, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, rows[pos + k]);
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    ASSERT_TRUE(w.Commit(0));
  }

  static void ExpectRow(const Case& c, const std::vector<Value>& rows,
                        duckdb::Vector& out, duckdb::idx_t k, uint64_t g) {
    const auto got = out.GetValue(k);
    ASSERT_TRUE(Value::NotDistinctFrom(got, rows[g]))
      << c.name << " row " << g << ": " << got.ToString()
      << " != " << rows[g].ToString();
  }

  static void CheckScans(const Case& c, const std::vector<Value>& rows,
                         irs::ColReader& r, const irs::ColumnReader& col) {
    {
      auto state = col.InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < c.rows;) {
        const auto take =
          std::min<duckdb::idx_t>(c.rows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col.Scan(state, out, take);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ASSERT_NO_FATAL_FAILURE(ExpectRow(c, rows, out, k, pos + k));
        }
        pos += take;
      }
    }
    for (const uint64_t skip : {uint64_t{5}, uint64_t{STANDARD_VECTOR_SIZE},
                                uint64_t{c.rg_size + 100}}) {
      auto state = col.InitScan(r.Ctx());
      col.Skip(state, skip);
      for (uint64_t pos = skip; pos < c.rows;) {
        const auto take = std::min<duckdb::idx_t>(c.rows - pos, 700);
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        col.ScanCount(state, out, take, 3);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ASSERT_NO_FATAL_FAILURE(ExpectRow(c, rows, out, 3 + k, pos + k));
        }
        pos += take;
        if (pos + 900 < c.rows) {
          col.Skip(state, 900);
          pos += 900;
        }
      }
    }
  }

  static void CheckGathers(const Case& c, const std::vector<Value>& rows,
                           irs::ColReader& r, const irs::ColumnReader& col) {
    for (const uint64_t step :
         {uint64_t{1}, uint64_t{3}, uint64_t{41}, uint64_t{700}}) {
      std::vector<uint64_t> picked;
      for (uint64_t g = step / 2; g < c.rows; g += step) {
        picked.push_back(g);
      }
      auto state = col.InitScan(r.Ctx());
      for (size_t i = 0; i < picked.size();) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        const Rows sub{&picked[i], take};
        irs::column_internal::GatherRows(col, state, sub, out, 0,
                                         /*whole_output=*/true);
        for (size_t k = 0; k < take; ++k) {
          ASSERT_NO_FATAL_FAILURE(ExpectRow(c, rows, out, k, picked[i + k]));
        }
        i += take;
      }
    }
  }

  static void CheckSelections(const Case& c, const std::vector<Value>& rows,
                              irs::ColReader& r, const irs::ColumnReader& col) {
    for (const bool dense : {true, false}) {
      for (const duckdb::idx_t stride :
           {duckdb::idx_t{2}, duckdb::idx_t{9}, duckdb::idx_t{20},
            duckdb::idx_t{41}, duckdb::idx_t{300}, duckdb::idx_t{1500}}) {
        auto state = col.InitScan(r.Ctx());
        for (uint64_t anchor = 0; anchor < c.rows;
             anchor += STANDARD_VECTOR_SIZE) {
          const auto span =
            std::min<uint64_t>(c.rows - anchor, STANDARD_VECTOR_SIZE);
          duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
          duckdb::idx_t hits = 0;
          for (duckdb::idx_t i = stride / 2; i < span; i += stride) {
            sel.set_index(hits++, i);
          }
          if (hits == 0) {
            continue;
          }
          duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
          if (dense) {
            col.GatherDense(state, anchor, sel, hits, span, out);
          } else {
            col.GatherScatter(state, anchor, sel, hits, out, 0);
          }
          for (duckdb::idx_t k = 0; k < hits; ++k) {
            ASSERT_NO_FATAL_FAILURE(
              ExpectRow(c, rows, out, k, anchor + sel.get_index(k)));
          }
        }
      }
    }
  }

  static void CheckPoints(const Case& c, const std::vector<Value>& rows,
                          irs::ColReader& r, const irs::ColumnReader& col) {
    std::vector<uint64_t> order;
    for (uint64_t g = 0; g < c.rows; g += 3) {
      order.push_back(g);
    }
    std::shuffle(order.begin(), order.end(), std::mt19937_64{42});
    for (uint64_t g = c.rows; g-- > 0;) {
      order.push_back(g);
    }
    for (uint64_t g = 0; g < c.rows; g += 2) {
      order.push_back(g);
    }
    {
      irs::ColumnReader::PointReader reader{r, col};
      duckdb::Vector out{c.type, 1};
      for (const auto g : order) {
        duckdb::FlatVector::ValidityMutable(out).Reset();
        const bool valid = reader.FetchRow(g, out, 0);
        ASSERT_EQ(valid, !rows[g].IsNull()) << c.name << " row " << g;
        if (valid) {
          ASSERT_NO_FATAL_FAILURE(ExpectRow(c, rows, out, 0, g));
        }
      }
    }
    irs::ColumnReader::PointReader cursor{r, col};
    constexpr duckdb::idx_t kOffset = 5;
    for (const uint64_t step : {uint64_t{1}, uint64_t{7}, uint64_t{97}}) {
      std::vector<uint32_t> batch;
      for (uint64_t g = step / 2; g < c.rows; g += step) {
        batch.push_back(static_cast<uint32_t>(g));
        if (batch.size() + kOffset < STANDARD_VECTOR_SIZE &&
            g + step < c.rows) {
          continue;
        }
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(batch, 0, out, kOffset);
        for (size_t k = 0; k < batch.size(); ++k) {
          ASSERT_NO_FATAL_FAILURE(
            ExpectRow(c, rows, out, kOffset + k, batch[k]));
        }
        batch.clear();
      }
    }
  }

  using ExpressionPtr = duckdb::unique_ptr<duckdb::Expression>;

  struct Form {
    std::string name;
    ExpressionPtr expr;
    irs::NullCheckKind null_check = irs::NullCheckKind::None;
  };

  static ExpressionPtr Ref(const Case& c) {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(c.type, 0);
  }

  static ExpressionPtr Const(const Value& v) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(v);
  }

  static ExpressionPtr Compare(const Case& c, duckdb::ExpressionType cmp,
                               const Value& v) {
    return duckdb::BoundComparisonExpression::Create(cmp, Ref(c), Const(v));
  }

  static ExpressionPtr Operator(duckdb::ExpressionType type, ExpressionPtr a,
                                ExpressionPtr b = nullptr,
                                ExpressionPtr d = nullptr) {
    auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
      type, LogicalType::BOOLEAN);
    op->GetChildrenMutable().push_back(std::move(a));
    if (b) {
      op->GetChildrenMutable().push_back(std::move(b));
    }
    if (d) {
      op->GetChildrenMutable().push_back(std::move(d));
    }
    return op;
  }

  static ExpressionPtr Join(duckdb::ExpressionType type, ExpressionPtr a,
                            ExpressionPtr b) {
    return duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
      type, std::move(a), std::move(b));
  }

  static std::vector<Form> Forms(const Case& c, const Value& lo,
                                 const Value& hi, const Value& mid) {
    using enum duckdb::ExpressionType;
    const auto outside = [&] {
      return Operator(COMPARE_NOT_IN, Ref(c), Const(lo), Const(hi));
    };
    std::vector<Form> forms;
    for (const auto cmp : {COMPARE_EQUAL, COMPARE_NOTEQUAL, COMPARE_LESSTHAN,
                           COMPARE_LESSTHANOREQUALTO, COMPARE_GREATERTHAN,
                           COMPARE_GREATERTHANOREQUALTO}) {
      forms.emplace_back(
        absl::StrCat(duckdb::ExpressionTypeToString(cmp), " a value"),
        Compare(c, cmp, mid));
    }
    forms.emplace_back("equals null", Compare(c, COMPARE_EQUAL, Value{c.type}));
    forms.emplace_back("distinct from a value",
                       Compare(c, COMPARE_DISTINCT_FROM, lo));
    forms.emplace_back(
      "equals itself",
      duckdb::BoundComparisonExpression::Create(COMPARE_EQUAL, Ref(c), Ref(c)));
    forms.emplace_back("is null", Operator(OPERATOR_IS_NULL, Ref(c)));
    forms.emplace_back("is null, checked on validity alone",
                       Operator(OPERATOR_IS_NULL, Ref(c)),
                       irs::NullCheckKind::IsNull);
    forms.emplace_back("is not null", Operator(OPERATOR_IS_NOT_NULL, Ref(c)));
    forms.emplace_back("is not null, checked on validity alone",
                       Operator(OPERATOR_IS_NOT_NULL, Ref(c)),
                       irs::NullCheckKind::IsNotNull);
    forms.emplace_back(
      "a comparison is null",
      Operator(OPERATOR_IS_NULL, Compare(c, COMPARE_EQUAL, lo)));
    forms.emplace_back("outside a list", outside());
    forms.emplace_back("between two values",
                       duckdb::BoundBetweenExpression::Create(
                         Ref(c), Const(lo), Const(hi), true, false));
    forms.emplace_back(
      "a case", duckdb::make_uniq<duckdb::BoundCaseExpression>(
                  Compare(c, COMPARE_EQUAL, lo), Const(Value::BOOLEAN(false)),
                  Const(Value::BOOLEAN(true))));
    forms.emplace_back(
      "a range",
      Join(CONJUNCTION_AND, Compare(c, COMPARE_GREATERTHANOREQUALTO, lo),
           Compare(c, COMPARE_LESSTHAN, hi)));
    forms.emplace_back("either value",
                       Join(CONJUNCTION_OR, Compare(c, COMPARE_EQUAL, lo),
                            Compare(c, COMPARE_EQUAL, hi)));
    forms.emplace_back(
      "a value or outside a list",
      Join(CONJUNCTION_OR, Compare(c, COMPARE_EQUAL, lo), outside()));
    forms.emplace_back(
      "outside a list and not a value",
      Join(CONJUNCTION_AND, outside(), Compare(c, COMPARE_NOTEQUAL, lo)));
    forms.emplace_back("null or a value",
                       Join(CONJUNCTION_OR, Operator(OPERATOR_IS_NULL, Ref(c)),
                            Compare(c, COMPARE_EQUAL, lo)));
    forms.emplace_back(
      "not null and not a value",
      Join(CONJUNCTION_AND, Operator(OPERATOR_IS_NOT_NULL, Ref(c)),
           Compare(c, COMPARE_NOTEQUAL, lo)));
    return forms;
  }

  static std::vector<bool> Evaluate(duckdb::ClientContext& context,
                                    const Case& c,
                                    const duckdb::Expression& expr,
                                    const std::vector<Value>& rows) {
    std::vector<bool> out(rows.size());
    duckdb::ExpressionExecutor executor{context, expr};
    duckdb::DataChunk chunk;
    const LogicalType types[] = {c.type};
    chunk.Initialize(duckdb::Allocator::Get(context), types);
    for (uint64_t pos = 0; pos < rows.size(); pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<uint64_t>(rows.size() - pos, STANDARD_VECTOR_SIZE);
      chunk.Reset();
      chunk.SetChildCardinality(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        chunk.data[0].SetValue(k, rows[pos + k]);
      }
      duckdb::Vector verdicts{LogicalType::BOOLEAN, STANDARD_VECTOR_SIZE};
      executor.ExecuteExpression(chunk, verdicts);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto verdict = verdicts.GetValue(k);
        out[pos + k] = !verdict.IsNull() && verdict.GetValue<bool>();
      }
    }
    return out;
  }

  void CheckFilterForms(const Case& c, const std::vector<Value>& rows,
                        irs::ColReader& r, const irs::ColumnReader& col) {
    duckdb::Connection con{_db};
    auto lo = c.value(0);
    auto hi = lo;
    auto mid = lo;
    bool any = false;
    for (uint64_t g = 0; g < c.rows; g += 7) {
      const auto& v = rows[g];
      if (v.IsNull()) {
        continue;
      }
      if (!any || duckdb::ValueOperations::LessThan(v, lo)) {
        lo = v;
      }
      if (!any || duckdb::ValueOperations::GreaterThan(v, hi)) {
        hi = v;
      }
      if (!any || g <= c.rows / 2) {
        mid = v;
      }
      any = true;
    }
    for (const auto& form : Forms(c, lo, hi, mid)) {
      SCOPED_TRACE(form.name);
      const auto expected = Evaluate(*con.context, c, *form.expr, rows);
      const duckdb::ExpressionFilter filter{form.expr->Copy()};
      auto filter_state =
        duckdb::TableFilterState::Initialize(*con.context, filter);
      bool agrees = true;
      for (const duckdb::idx_t stride : {duckdb::idx_t{1}, duckdb::idx_t{5}}) {
        auto state = col.InitScan(r.Ctx());
        for (uint64_t anchor = 0; agrees && anchor < c.rows;
             anchor += STANDARD_VECTOR_SIZE) {
          const auto span =
            std::min<uint64_t>(c.rows - anchor, STANDARD_VECTOR_SIZE);
          duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
          duckdb::idx_t count = 0;
          for (duckdb::idx_t i = 0; i < span; i += stride) {
            sel.set_index(count++, i);
          }
          duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
          const auto kept =
            col.GatherFilter(state, anchor, span, sel, count, filter,
                             *filter_state, form.null_check, out);
          duckdb::idx_t next = 0;
          for (duckdb::idx_t i = 0; agrees && i < span; i += stride) {
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            agrees = got == expected[anchor + i];
            EXPECT_TRUE(agrees) << c.name << " row " << anchor + i << " value "
                                << rows[anchor + i].ToString() << " stride "
                                << stride << " kept " << got;
          }
          if (agrees) {
            agrees = next == kept;
            EXPECT_TRUE(agrees) << c.name << " kept " << kept << " rows";
          }
        }
      }
    }
  }

  void DescribeBlocks(irs::ColReader& r, const irs::ColumnReader& col) {
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    uint64_t row = 0;
    for (const auto& block : col.DataBlocks()) {
      window = col.Locate(row, window);
      const auto segment = col.OpenSegment(window.block, ctx);
      if (segment->function.get_segment_info) {
        EXPECT_FALSE(segment->function.get_segment_info(*segment).empty())
          << irs::codecs::NameOf(block.codec->type) << " block at row " << row;
      }
      row += block.tuple_count;
    }
  }

  void Check(const Case& c) {
    SCOPED_TRACE(c.name);
    const auto rows = Expected(c);
    irs::MemoryDirectory dir{};
    ASSERT_NO_FATAL_FAILURE(Write(dir, c, rows));
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), c.rows);
    if (c.stored && c.codec != CodecType::Auto) {
      std::string codecs;
      bool found = false;
      for (const auto& block : col->DataBlocks()) {
        absl::StrAppend(&codecs, irs::codecs::NameOf(block.codec->type), " ");
        found = found || block.codec->type == c.codec;
      }
      ASSERT_TRUE(found) << codecs;
    }
    ASSERT_NO_FATAL_FAILURE(CheckScans(c, rows, r, *col));
    ASSERT_NO_FATAL_FAILURE(CheckGathers(c, rows, r, *col));
    ASSERT_NO_FATAL_FAILURE(CheckSelections(c, rows, r, *col));
    ASSERT_NO_FATAL_FAILURE(CheckPoints(c, rows, r, *col));
    ASSERT_NO_FATAL_FAILURE(CheckFilterForms(c, rows, r, *col));
    DescribeBlocks(r, *col);
  }

  duckdb::DuckDB _db;
};

const std::vector<LogicalType>& IntegerTypes() {
  static const std::vector<LogicalType> kTypes{
    LogicalType::TINYINT,   LogicalType::UTINYINT, LogicalType::SMALLINT,
    LogicalType::USMALLINT, LogicalType::INTEGER,  LogicalType::UINTEGER,
    LogicalType::BIGINT,    LogicalType::UBIGINT,  LogicalType::HUGEINT,
    LogicalType::UHUGEINT,
  };
  return kTypes;
}

bool AtLeast16Bits(const LogicalType& type) {
  return duckdb::GetTypeIdSize(type.InternalType()) >= sizeof(int16_t);
}

TEST_F(ColCodecPathsTest, BitpackingReadsEveryIntegerTypeInEveryMode) {
  for (const auto& type : IntegerTypes()) {
    const auto name = [&](std::string_view shape) {
      return absl::StrCat(type.ToString(), " ", shape);
    };
    Check({name("for"), type, CodecType::Bitpacking, Periodic,
           [&](uint64_t g) { return Int(type, (g * 7919) % 100); }});
    Check({name("constant groups"), type, CodecType::Bitpacking, All,
           [&](uint64_t g) { return Int(type, (g / 5000) % 3); }});
    Check({name("constant groups with nulls"), type, CodecType::Bitpacking,
           NullRuns,
           [&](uint64_t g) { return Int(type, 7 + (g / 3000) % 2); }});
    Check({name("mostly null"), type, CodecType::Bitpacking, Rare,
           [&](uint64_t g) { return Int(type, g % 77); }});
    Check({name("wide"), type, CodecType::Bitpacking, Periodic,
           [&](uint64_t g) { return Wide(type, g); }, /*stored=*/false});
    const auto size = duckdb::GetTypeIdSize(type.InternalType());
    if (size <= sizeof(int64_t)) {
      Check({name("near the maximum"), type, CodecType::Bitpacking, Periodic,
             [&](uint64_t g) { return NearMaximum(type, Mix(g) % 28); }});
      if (type.IsUnsigned()) {
        Check({name("the whole range"), type, CodecType::Bitpacking, All,
               [&](uint64_t g) { return Wide(type, g); }});
      }
    }
    if (!AtLeast16Bits(type)) {
      Check({name("steps"), type, CodecType::Bitpacking, All,
             [&](uint64_t g) { return Int(type, 3 + (g % 2048) % 40 * 3); },
             /*stored=*/false});
      Check({name("small steps with noise"), type, CodecType::Bitpacking, All,
             [&](uint64_t g) {
               return Int(type, (g % 2048) % 50 * 2 + (g * 7919) % 2);
             },
             /*stored=*/false});
      continue;
    }
    if (!type.IsUnsigned() && size <= sizeof(int64_t)) {
      const int64_t step = size == sizeof(int16_t)   ? 31
                           : size == sizeof(int32_t) ? 2'090'000
                                                     : 9'000'000'000'000'000;
      const auto lowest = Value::MinimumValue(type).GetValue<int64_t>() + 1;
      Check({name("a sawtooth across the range"), type, CodecType::Bitpacking,
             All, [&, step, lowest](uint64_t g) {
               return Int(type, lowest + static_cast<int64_t>(g % 2048) * step +
                                  static_cast<int64_t>(Mix(g) % 2));
             }});
    }
    Check({name("constant delta"), type, CodecType::Bitpacking, All,
           [&](uint64_t g) { return Int(type, 11 + (g % 2048) * 3); }});
    Check({name("falling constant delta"), type, CodecType::Bitpacking, All,
           [&](uint64_t g) { return Int(type, 9000 - (g % 2048) * 4); }});
    Check(
      {name("delta for"), type, CodecType::Bitpacking, All,
       [&](uint64_t g) { return Int(type, (g % 2048) * 3 + (g * 7919) % 5); }});
    Check({name("delta for with nulls"), type, CodecType::Bitpacking, Periodic,
           [&](uint64_t g) {
             return Int(type, 20000 - (g % 2048) * 7 - (g * 31) % 3);
           }});
  }
}

TEST_F(ColCodecPathsTest, RleReadsRunsOfEveryType) {
  auto types = IntegerTypes();
  types.push_back(LogicalType::FLOAT);
  types.push_back(LogicalType::DOUBLE);
  types.push_back(LogicalType::BOOLEAN);
  for (const auto& type : types) {
    const auto name = [&](std::string_view shape) {
      return absl::StrCat(type.ToString(), " ", shape);
    };
    const auto item = [&](int64_t v) {
      return type.id() == duckdb::LogicalTypeId::BOOLEAN
               ? Value::BOOLEAN(v % 2 == 0)
               : Int(type, v);
    };
    Check({name("runs"), type, CodecType::Rle, All,
           [&](uint64_t g) { return item((g / 37) % 11); }});
    Check({name("runs with nulls"), type, CodecType::Rle, Periodic,
           [&](uint64_t g) { return item((g / 13) % 100); }});
    Check({name("long runs with null runs"), type, CodecType::Rle, NullRuns,
           [&](uint64_t g) { return item(g / 3000); }});
    Check({name("runs longer than a vector"), type, CodecType::Rle, All,
           [&](uint64_t g) { return item(g / 5000); }});
    Check(
      {name("runs that turn single"), type, CodecType::Rle, All,
       [&](uint64_t g) { return item(g % kRgSize < 3000 ? g / 500 : g % 90); },
       /*stored=*/false});
  }
}

TEST_F(ColCodecPathsTest, RleReadsARunLongerThanARunCounter) {
  for (const LogicalType type : {LogicalType::BIGINT, LogicalType::DOUBLE}) {
    Check({absl::StrCat(type.ToString(), " one long run"), type, CodecType::Rle,
           All,
           [&](uint64_t g) {
             return Int(type, g < 70000 ? 3 : static_cast<int64_t>(g / 2 % 50));
           },
           /*stored=*/true, irs::WriteTier::Flush,
           /*rows=*/140000,
           /*rg_size=*/131072});
  }
}

TEST_F(ColCodecPathsTest, UncompressedAndConstantBlocksOfEveryType) {
  auto types = IntegerTypes();
  types.push_back(LogicalType::FLOAT);
  types.push_back(LogicalType::DOUBLE);
  types.push_back(LogicalType::BOOLEAN);
  for (const auto& type : types) {
    const auto name = [&](std::string_view shape) {
      return absl::StrCat(type.ToString(), " ", shape);
    };
    const auto item = [&](int64_t v) {
      return type.id() == duckdb::LogicalTypeId::BOOLEAN
               ? Value::BOOLEAN(v % 3 == 0)
               : Int(type, v);
    };
    for (const auto& valid : {std::function<bool(uint64_t)>{All},
                              std::function<bool(uint64_t)>{Periodic},
                              std::function<bool(uint64_t)>{NullRuns},
                              std::function<bool(uint64_t)>{FirstRowsNull}}) {
      Check({name("uncompressed"), type, CodecType::Uncompressed, valid,
             [&](uint64_t g) { return item((g * 7919) % 120); }});
      Check({name("constant"), type, CodecType::Auto, valid,
             [&](uint64_t) { return item(42); }});
    }
    Check({name("all null"), type, CodecType::Auto,
           [](uint64_t) { return false; }, [&](uint64_t) { return item(1); }});
  }
  Check({"interval uncompressed", LogicalType::INTERVAL,
         CodecType::Uncompressed, Periodic, [](uint64_t g) {
           return Value::INTERVAL(static_cast<int32_t>(g % 12),
                                  static_cast<int32_t>(g % 28),
                                  static_cast<int64_t>(g) * 1000);
         }});
  Check({"interval constant", LogicalType::INTERVAL, CodecType::Auto, All,
         [](uint64_t) { return Value::INTERVAL(1, 2, 3); }});
}

TEST_F(ColCodecPathsTest, AlpCodecsReadFloatsAndDoubles) {
  for (const auto& type :
       std::vector<LogicalType>{LogicalType::FLOAT, LogicalType::DOUBLE}) {
    const auto name = [&](std::string_view shape) {
      return absl::StrCat(type.ToString(), " ", shape);
    };
    const auto item = [&](double v) {
      return type.id() == duckdb::LogicalTypeId::FLOAT
               ? Value::FLOAT(static_cast<float>(v))
               : Value::DOUBLE(v);
    };
    for (const auto& valid : {std::function<bool(uint64_t)>{All},
                              std::function<bool(uint64_t)>{Periodic},
                              std::function<bool(uint64_t)>{NullRuns}}) {
      Check(
        {name("alp decimals"), type, CodecType::Alp, valid, [&](uint64_t g) {
           return item(static_cast<double>(g % 10000) / 100.0);
         }});
      Check(
        {name("alp exceptions"), type, CodecType::Alp, valid, [&](uint64_t g) {
           return g % 101 == 0
                    ? item(std::sqrt(static_cast<double>(g)) * 1.000001)
                    : item(static_cast<double>(g % 1000) / 10.0);
         }});
      Check({name("alp one value"), type, CodecType::Alp, valid,
             [&](uint64_t g) { return item(g % 4096 < 2048 ? 2.5 : 3.25); },
             /*stored=*/false});
      Check({name("alprd"), type, CodecType::AlpRd, valid, [&](uint64_t g) {
               return item(std::sin(static_cast<double>(g)) * 12345.678901234);
             }});
      Check({name("alprd exceptions"), type, CodecType::AlpRd, valid,
             [&](uint64_t g) {
               return g % 97 == 0 ? item(-1e30 * static_cast<double>(g + 1))
                                  : item(std::sin(static_cast<double>(g)));
             }});
    }
  }
}

TEST_F(ColCodecPathsTest, AutoReadsEveryNumericTypeAndShape) {
  auto types = IntegerTypes();
  types.resize(8);
  types.push_back(LogicalType::FLOAT);
  types.push_back(LogicalType::DOUBLE);
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& type : types) {
      const auto name = [&](std::string_view shape) {
        return absl::StrCat(type.ToString(), " ", shape, " tier ",
                            static_cast<int>(tier));
      };
      const bool real = type.id() == duckdb::LogicalTypeId::FLOAT ||
                        type.id() == duckdb::LogicalTypeId::DOUBLE;
      const auto item = [&](int64_t v) { return Int(type, v); };
      const auto wide = [&](uint64_t g) {
        return real ? Value::DOUBLE(std::sin(static_cast<double>(g)) * 1e6)
                        .DefaultCastAs(type)
                    : Wide(type, g);
      };
      Check({name("narrow"), type, CodecType::Auto, Periodic,
             [&](uint64_t g) { return item((g * 7919) % 100); },
             /*stored=*/false, tier});
      Check({name("runs"), type, CodecType::Auto, NullRuns,
             [&](uint64_t g) { return item((g / 37) % 11); },
             /*stored=*/false, tier});
      Check({name("few wide values"), type, CodecType::Auto, All,
             [&](uint64_t g) { return wide(g % 13); }, /*stored=*/false, tier});
      Check({name("runs of wide values"), type, CodecType::Auto, Periodic,
             [&](uint64_t g) { return wide((g / 50) % 7); }, /*stored=*/false,
             tier});
      Check({name("wide"), type, CodecType::Auto, All,
             [&](uint64_t g) { return wide(g); }, /*stored=*/false, tier});
      Check({name("mostly null"), type, CodecType::Auto, Rare,
             [&](uint64_t g) { return item(g % 77); }, /*stored=*/false, tier});
      if (AtLeast16Bits(type)) {
        Check({name("sorted"), type, CodecType::Auto, All,
               [&](uint64_t g) {
                 return item(static_cast<int64_t>(g) + (g * 7919) % 2);
               },
               /*stored=*/false, tier});
      }
    }
  }
}

TEST_F(ColCodecPathsTest, AutoReadsWideIntegers) {
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const LogicalType type :
         {LogicalType::HUGEINT, LogicalType::UHUGEINT}) {
      const auto name = [&](std::string_view shape) {
        return absl::StrCat(type.ToString(), " ", shape, " tier ",
                            static_cast<int>(tier));
      };
      const auto item = [&](int64_t v) { return Int(type, v); };
      Check({name("narrow"), type, CodecType::Auto, Periodic,
             [&](uint64_t g) { return item((g * 7919) % 100); },
             /*stored=*/false, tier});
      Check({name("runs"), type, CodecType::Auto, NullRuns,
             [&](uint64_t g) { return item((g / 37) % 11); },
             /*stored=*/false, tier});
      Check({name("steps"), type, CodecType::Auto, All,
             [&](uint64_t g) { return item(11 + (g % 2048) * 3); },
             /*stored=*/false, tier});
      Check({name("sorted"), type, CodecType::Auto, All,
             [&](uint64_t g) {
               return item(static_cast<int64_t>(g) * 5 + (g * 7919) % 3);
             },
             /*stored=*/false, tier});
      Check({name("wide"), type, CodecType::Auto, Periodic,
             [&](uint64_t g) { return Wide(type, g); }, /*stored=*/false,
             tier});
    }
  }
}

TEST_F(ColCodecPathsTest, TextCodecsReadEveryShape) {
  static constexpr CodecType kCodecs[] = {
    CodecType::DictLz4,    CodecType::Lz4,        CodecType::DictZstd,
    CodecType::Zstd,       CodecType::DictZxc,    CodecType::Zxc,
    CodecType::DictFsst,   CodecType::Fsst,       CodecType::DictFsstPlus,
    CodecType::FsstPlus,   CodecType::DictOnPair, CodecType::OnPair,
    CodecType::DictFsst12, CodecType::Fsst12,
  };
  const std::string filler(400, 'w');
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto codec : kCodecs) {
      const auto name = [&](std::string_view shape) {
        return absl::StrCat(irs::codecs::NameOf(codec), " ", shape, " tier ",
                            static_cast<int>(tier));
      };
      Check(
        {name("categories"), LogicalType::VARCHAR, codec, Periodic,
         [](uint64_t g) { return Value{absl::StrCat("category_", g % 50)}; },
         /*stored=*/true, tier});
      Check({name("unique"), LogicalType::VARCHAR, codec, All,
             [](uint64_t g) {
               return Value{absl::StrCat("https://example.org/items/", g * 7919,
                                         "/detail")};
             },
             /*stored=*/true, tier});
      Check({name("long and empty"), LogicalType::VARCHAR, codec, NullRuns,
             [&](uint64_t g) {
               return g % 5 == 0 ? Value{""}
                                 : Value{absl::StrCat(g % 211, "-", filler,
                                                      Mix(g % 300))};
             },
             /*stored=*/true, tier});
      Check({name("mostly null"), LogicalType::VARCHAR, codec, Rare,
             [](uint64_t g) { return Value{absl::StrCat("v", g % 9)}; },
             /*stored=*/true, tier});
      Check({name("all null"), LogicalType::VARCHAR, codec,
             [](uint64_t) { return false; },
             [](uint64_t) { return Value{"never stored"}; },
             /*stored=*/false, tier});
      Check({name("many values in no order"), LogicalType::VARCHAR, codec, All,
             [&](uint64_t g) {
               return Value{
                 absl::StrCat("https://example.org/", Mix(Mix(g) % 3000), "/",
                              std::string_view{filler}.substr(0, 40))};
             },
             /*stored=*/true, tier});
    }
  }
}

}  // namespace
