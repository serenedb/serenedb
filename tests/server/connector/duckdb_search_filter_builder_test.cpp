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

#include <absl/base/internal/endian.h>
#include <vpack/parser.h>

#include <duckdb.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/string_utils.h"
#include "catalog/mangling.h"
#include "connector/functions/search.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_filter_printer.hpp"
#include "gtest/gtest.h"

namespace {

using namespace sdb;
using sdb::connector::ColumnGetter;
using sdb::connector::SearchColumnInfo;

// ---------------------------------------------------------------------------
// Plan capture: the production MakeSearchFilter runs from an OptimizerExtension
// hook (iresearch_plan.cpp), which fires AFTER DuckDB's built-in optimizers but
// BEFORE ColumnBindingResolver converts BoundColumnRefExpression into
// BoundReferenceExpression. The test stands in for that hook: it snapshots a
// deep copy of the plan at exactly the same point, so MakeSearchFilter here
// sees the same expression shape it would in production.
// ---------------------------------------------------------------------------
thread_local duckdb::unique_ptr<duckdb::LogicalOperator> tlCapturedPlan;

void CaptureOptimizer(duckdb::OptimizerExtensionInput& input,
                      duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  tlCapturedPlan = plan->Copy(input.context);
}

class CapturePlanOptimizer : public duckdb::OptimizerExtension {
 public:
  CapturePlanOptimizer() { optimize_function = CaptureOptimizer; }
};

// Walks to the first LogicalFilter directly (or through a chain of
// LogicalFilter/LogicalProjection) above a LogicalGet, mirroring
// iresearch_plan.cpp:499-509.
std::pair<const duckdb::LogicalFilter*, const duckdb::LogicalGet*>
FindFilterAndGet(const duckdb::LogicalOperator& op) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    const auto* child = op.children.empty() ? nullptr : op.children[0].get();
    while (child &&
           (child->type == duckdb::LogicalOperatorType::LOGICAL_FILTER ||
            child->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION)) {
      child = child->children.empty() ? nullptr : child->children[0].get();
    }
    if (child && child->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
      return {&op.Cast<duckdb::LogicalFilter>(),
              &child->Cast<duckdb::LogicalGet>()};
    }
  }
  for (const auto& c : op.children) {
    auto result = FindFilterAndGet(*c);
    if (result.first) {
      return result;
    }
  }
  return {nullptr, nullptr};
}

// ---------------------------------------------------------------------------
// ColumnSpec: the test fixture's view of a table column. `id` is the catalog
// column id carried through into the iresearch field name mangling; `type`
// is the DuckDB column type used both for CREATE TABLE and for the
// SearchColumnInfo returned by the ColumnGetter; `name` is the unquoted
// column name that the SQL query references.
// ---------------------------------------------------------------------------
struct ColumnSpec {
  catalog::Column::Id id;
  duckdb::LogicalType type;
  std::string name;
};

using AnalyzerProvider =
  std::function<catalog::ColumnAnalyzer(catalog::Column::Id)>;

catalog::ColumnAnalyzer IdentityAnalyzerProvider(catalog::Column::Id) {
  auto make_identity = [] {
    return std::string(vpack::Slice::emptyObjectSlice().startAs<char>(),
                       vpack::Slice::emptyObjectSlice().byteSize());
  };
  static catalog::Tokenizer gStringTokenizer(
    ObjectId{12345}, "test_string_verbartim", {}, make_identity());
  auto tokenizer = gStringTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer),
          .features = irs::IndexFeatures::None};
}

template<irs::IndexFeatures Features>
catalog::ColumnAnalyzer SegmentationAnalyzerProviderBase(catalog::Column::Id) {
  auto make_segmentation = [] {
    auto builder =
      vpack::Parser::fromJson("{ \"analyzer\": {\"type\":\"segmentation\"}}");
    return std::string(builder->slice().startAs<char>(),
                       builder->slice().byteSize());
  };
  static catalog::Tokenizer gStringTokenizer(
    ObjectId{12346}, "test_segmentation", {}, make_segmentation());
  auto tokenizer = gStringTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer), .features = Features};
}

[[maybe_unused]] catalog::ColumnAnalyzer SegmentationAnalyzerProvider(
  catalog::Column::Id id) {
  return SegmentationAnalyzerProviderBase<irs::IndexFeatures::Pos |
                                          irs::IndexFeatures::Freq>(id);
}

[[maybe_unused]] catalog::ColumnAnalyzer NgramAnalyzerProvider(
  catalog::Column::Id) {
  auto make_ngram = [] {
    auto builder = vpack::Parser::fromJson(
      "{ \"analyzer\": {\"type\":\"ngram\","
      "\"properties\":{\"min\":2,\"max\":2,"
      "\"preserveOriginal\":false,\"streamType\":\"utf8\"}}}");
    return std::string(builder->slice().startAs<char>(),
                       builder->slice().byteSize());
  };
  static catalog::Tokenizer gNgramTokenizer(ObjectId{12347}, "test_ngram", {},
                                            make_ngram());
  auto tokenizer = gNgramTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer),
          .features = irs::IndexFeatures::Pos | irs::IndexFeatures::Freq};
}

[[maybe_unused]] catalog::ColumnAnalyzer WildcardAnalyzerProvider(
  catalog::Column::Id) {
  auto make_wildcard = [] {
    auto builder = vpack::Parser::fromJson(
      "{ \"analyzer\": {\"type\":\"wildcard\","
      "\"properties\":{\"ngramSize\":3,"
      "\"analyzer\":{\"type\":\"identity\"}}}}");
    return std::string(builder->slice().startAs<char>(),
                       builder->slice().byteSize());
  };
  static catalog::Tokenizer gWildcardTokenizer(ObjectId{12348}, "test_wildcard",
                                               {}, make_wildcard());
  auto tokenizer = gWildcardTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer),
          .features = irs::IndexFeatures::Pos | irs::IndexFeatures::Freq};
}

// ---------------------------------------------------------------------------
// Expected-filter builders (ported from velox test suite).
// Type dispatch is now by duckdb::LogicalType / native C++ type.
// ---------------------------------------------------------------------------
template<typename T>
std::string MakeFieldName(catalog::Column::Id column_id) {
  std::string field_name;
  basics::StrResize(field_name, sizeof(column_id));
  absl::big_endian::Store(field_name.data(), column_id);
  if constexpr (std::is_same_v<T, bool>) {
    search::mangling::MangleBool(field_name);
  } else if constexpr (std::is_same_v<T, std::string_view> ||
                       std::is_same_v<T, std::string>) {
    search::mangling::MangleString(field_name);
  } else if constexpr (std::is_floating_point_v<T> || std::is_integral_v<T>) {
    search::mangling::MangleNumeric(field_name);
  } else {
    static_assert(sizeof(T) == 0, "Unsupported term type for MakeFieldName");
  }
  return field_name;
}

template<typename Filter, typename Source>
auto& AddFilter(Source& parent) {
  if constexpr (std::is_same_v<irs::Not, Source>) {
    return parent.template filter<Filter>();
  } else {
    return parent.template add<Filter>();
  }
}

template<typename T, typename Filter>
irs::ByTerm& AddTermFilter(Filter& root, catalog::Column::Id column,
                           const T& value) {
  auto& term = AddFilter<irs::ByTerm>(root);
  *term.mutable_field() = MakeFieldName<T>(column);
  if constexpr (std::is_same_v<T, bool>) {
    term.mutable_options()->term.assign(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(value)));
  } else if constexpr (std::is_same_v<T, std::string_view> ||
                       std::is_same_v<T, std::string>) {
    irs::StringTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    stream.reset(value);
    stream.next();
    term.mutable_options()->term.assign(token->value);
  } else {
    static_assert(std::is_floating_point_v<T> || std::is_integral_v<T>,
                  "Unexpected term type");
    irs::NumericTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    stream.reset(value);
    stream.next();
    term.mutable_options()->term.assign(token->value);
  }
  return term;
}

template<typename T, typename Filter>
irs::FilterWithBoost& AddRangeFilter(Filter& root, catalog::Column::Id column,
                                     const std::optional<T>& min_value,
                                     bool min_inclusive,
                                     const std::optional<T>& max_value,
                                     bool max_inclusive) {
  if constexpr (std::is_same_v<T, std::string_view> ||
                std::is_same_v<T, std::string>) {
    auto& range = AddFilter<irs::ByRange>(root);
    *range.mutable_field() = MakeFieldName<T>(column);
    auto& options = range.mutable_options()->range;
    irs::StringTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    if (min_value.has_value()) {
      stream.reset(*min_value);
      stream.next();
      options.min.assign(token->value);
      options.min_type =
        min_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    } else {
      options.min_type = irs::BoundType::Unbounded;
    }
    if (max_value.has_value()) {
      stream.reset(*max_value);
      stream.next();
      options.max.assign(token->value);
      options.max_type =
        max_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    } else {
      options.max_type = irs::BoundType::Unbounded;
    }
    return range;
  } else {
    static_assert(std::is_floating_point_v<T> || std::is_integral_v<T>,
                  "Unexpected range type");
    auto& range = AddFilter<irs::ByGranularRange>(root);
    *range.mutable_field() = MakeFieldName<T>(column);
    auto& options = range.mutable_options()->range;
    irs::NumericTokenizer stream;
    if (min_value.has_value()) {
      stream.reset(*min_value);
      irs::SetGranularTerm(options.min, stream);
      options.min_type =
        min_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    } else {
      options.min_type = irs::BoundType::Unbounded;
    }
    if (max_value.has_value()) {
      stream.reset(*max_value);
      irs::SetGranularTerm(options.max, stream);
      options.max_type =
        max_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    } else {
      options.max_type = irs::BoundType::Unbounded;
    }
    return range;
  }
}

template<typename Filter>
irs::ByTerm& AddNullFilter(Filter& root, catalog::Column::Id column) {
  auto& term = AddFilter<irs::ByTerm>(root);
  std::string field_name;
  basics::StrResize(field_name, sizeof(column));
  absl::big_endian::Store(field_name.data(), column);
  search::mangling::MangleNull(field_name);
  *term.mutable_field() = field_name;
  term.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  return term;
}

template<typename Filter>
irs::ByWildcard& AddLikeFilter(Filter& root, catalog::Column::Id column,
                               std::string_view value) {
  auto& wc = AddFilter<irs::ByWildcard>(root);
  *wc.mutable_field() = MakeFieldName<std::string_view>(column);
  wc.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(value));
  return wc;
}

template<typename Filter>
irs::ByPrefix& AddPrefixFilter(Filter& root, catalog::Column::Id column,
                               std::string_view value) {
  auto& pf = AddFilter<irs::ByPrefix>(root);
  *pf.mutable_field() = MakeFieldName<std::string_view>(column);
  pf.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(value));
  return pf;
}

template<typename Filter>
irs::ByRegexp& AddRegexpFilter(
  Filter& root, catalog::Column::Id column, std::string_view pattern,
  irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
  auto& re = AddFilter<irs::ByRegexp>(root);
  *re.mutable_field() = MakeFieldName<std::string_view>(column);
  auto* opts = re.mutable_options();
  opts->pattern.assign(irs::ViewCast<irs::byte_type>(pattern));
  opts->syntax = syntax;
  return re;
}

template<typename Filter>
irs::ByNGramSimilarity& AddNgramSimilarityFilter(
  Filter& root, catalog::Column::Id column,
  std::vector<std::string_view> ngrams, float threshold = 0.7f) {
  auto& ngf = AddFilter<irs::ByNGramSimilarity>(root);
  *ngf.mutable_field() = MakeFieldName<std::string_view>(column);
  ngf.mutable_options()->threshold = threshold;
  for (auto ngram : ngrams) {
    ngf.mutable_options()->ngrams.emplace_back(
      irs::ViewCast<irs::byte_type>(ngram));
  }
  return ngf;
}

template<typename Filter>
irs::ByEditDistance& AddEditDistanceFilter(
  Filter& root, catalog::Column::Id column, std::string_view term,
  uint8_t max_distance, bool with_transpositions = true, size_t max_terms = 64,
  std::string_view prefix = "") {
  auto& ed = AddFilter<irs::ByEditDistance>(root);
  *ed.mutable_field() = MakeFieldName<std::string_view>(column);
  ed.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(term));
  ed.mutable_options()->max_distance = max_distance;
  ed.mutable_options()->with_transpositions = with_transpositions;
  ed.mutable_options()->max_terms = max_terms;
  if (!prefix.empty()) {
    ed.mutable_options()->prefix.assign(irs::ViewCast<irs::byte_type>(prefix));
  }
  return ed;
}

template<typename Filter>
irs::ByPhrase& AddPhraseFilter(Filter& root, catalog::Column::Id column,
                               std::vector<std::string_view> values) {
  auto& wc = AddFilter<irs::ByPhrase>(root);
  *wc.mutable_field() = MakeFieldName<std::string_view>(column);
  for (auto value : values) {
    wc.mutable_options()->template push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(value);
  }
  return wc;
}

template<typename Filter>
irs::ByWildcardNgram& AddWildcardNgramFilter(Filter& root,
                                             catalog::Column::Id column,
                                             std::string_view pattern,
                                             bool has_positions) {
  auto column_analyzer = WildcardAnalyzerProvider(column);
  auto& wf = AddFilter<irs::ByWildcardNgram>(root);
  *wf.mutable_field() = MakeFieldName<std::string_view>(column);
  *wf.mutable_options() = {pattern,
                           basics::downCast<irs::analysis::WildcardAnalyzer>(
                             *column_analyzer.analyzer.get()),
                           has_positions};
  return wf;
}

template<typename T, typename Filter>
irs::ByTerms& AddTermsFilter(Filter& root, catalog::Column::Id column,
                             const std::vector<T>& values) {
  auto& terms = AddFilter<irs::ByTerms>(root);
  *terms.mutable_field() = MakeFieldName<T>(column);
  for (const auto& value : values) {
    if constexpr (std::is_same_v<T, bool>) {
      terms.mutable_options()->terms.emplace(
        irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(value)));
    } else if constexpr (std::is_same_v<T, std::string_view> ||
                         std::is_same_v<T, std::string>) {
      irs::StringTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(value);
      stream.next();
      terms.mutable_options()->terms.emplace(token->value);
    } else {
      static_assert(std::is_floating_point_v<T> || std::is_integral_v<T>,
                    "Unexpected term type");
      irs::NumericTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(value);
      stream.next();
      terms.mutable_options()->terms.emplace(token->value);
    }
  }
  return terms;
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
class SearchFilterBuilderTest : public ::testing::Test {
 public:
  SearchFilterBuilderTest() : _db(nullptr), _conn(_db) {}

  static void SetUpTestCase() {
    irs::analysis::analyzers::Init();
    irs::formats::Init();
    irs::scorers::Init();
    irs::compression::Init();
  }

  void SetUp() override {
    sdb::connector::RegisterSearchFunctions(*_db.instance);
    auto& db_config = duckdb::DBConfig::GetConfig(*_db.instance);
    // Keep filter predicates on LogicalFilter so MakeSearchFilter can see
    // them (FILTER_PUSHDOWN would move them into LogicalGet.table_filters
    // for types DuckDB understands natively). Disable the empty-result and
    // stats-propagation passes so a query against an empty test table
    // doesn't get collapsed before we see it.
    auto& opts = db_config.options.disabled_optimizers;
    opts.insert(duckdb::OptimizerType::FILTER_PUSHDOWN);
    opts.insert(duckdb::OptimizerType::STATISTICS_PROPAGATION);
    opts.insert(duckdb::OptimizerType::EMPTY_RESULT_PULLUP);
    // REORDER_FILTER permutes LogicalFilter expressions by estimated
    // selectivity. That's fine in production but would make expected filter
    // trees brittle here, so we keep the original source-order.
    opts.insert(duckdb::OptimizerType::REORDER_FILTER);
    // IN_CLAUSE would rewrite `x IN (a, b)` into `x = a OR x = b`, flipping
    // the builder to produce an Or of ByTerm rather than a single ByTerms.
    opts.insert(duckdb::OptimizerType::IN_CLAUSE);
    // FILTER_PULLUP extracts common disjuncts from an OR and attaches them
    // as extra AND siblings ((a=10 AND b=t) OR (a=20 AND b=d) also becomes
    // a IN {10,20} AND b IN {t,d}) which would change the filter shape.
    opts.insert(duckdb::OptimizerType::FILTER_PULLUP);
    duckdb::OptimizerExtension::Register(db_config, CapturePlanOptimizer());
  }

  // When `expected_error` is non-empty, MakeSearchFilter is expected to
  // throw a user-visible validation error whose message contains that
  // substring; `must_succeed` and `expected` are ignored in that case.
  void AssertFilter(
    const irs::And& expected, std::string_view sql,
    const std::vector<ColumnSpec>& columns, bool must_succeed,
    const AnalyzerProvider& analyzer_provider = IdentityAnalyzerProvider,
    std::string_view expected_error = {}) {
    SCOPED_TRACE(testing::Message("Parsing: <") << sql << ">");

    // All tests reference a single well-known table "foo"; each test case
    // gets a fresh DuckDB instance in SetUp, but sub-cases within a single
    // test (test_TypesResolving, test_FieldCastError) reuse the fixture, so
    // we use CREATE OR REPLACE to let them redefine the schema freely.
    std::string create_sql = "CREATE OR REPLACE TABLE memory.main.foo (";
    for (size_t i = 0; i < columns.size(); ++i) {
      if (i != 0) {
        create_sql += ", ";
      }
      create_sql += columns[i].name + " " + columns[i].type.ToString();
    }
    create_sql += ")";
    auto create_res = _conn.Query(create_sql);
    ASSERT_FALSE(create_res->HasError())
      << "CREATE TABLE failed: " << create_res->GetError()
      << " (SQL: " << create_sql << ")";

    tlCapturedPlan.reset();
    // ExtractPlan may throw duckdb::Exception on binding errors. We want
    // those surfaced into the test result, not swallowed.
    try {
      (void)_conn.ExtractPlan(std::string{sql});
    } catch (const std::exception& e) {
      if (must_succeed) {
        FAIL() << "ExtractPlan threw: " << e.what();
      }
      return;
    }
    ASSERT_TRUE(tlCapturedPlan) << "optimizer hook did not fire";

    auto [filter_op, get_op] = FindFilterAndGet(*tlCapturedPlan);
    if (!filter_op || !get_op) {
      if (must_succeed) {
        FAIL() << "No LogicalFilter above LogicalGet in plan for: " << sql;
      }
      return;
    }

    // Resolve BoundColumnRef bindings against the LogicalGet's projection
    // vector. The test controls the schema so column indices are always
    // valid and always have a primary index -- no defensive checks needed.
    const auto& projected = get_op->GetColumnIds();
    ColumnGetter getter =
      [table_index = get_op->table_index, projected, &columns,
       &analyzer_provider](const duckdb::BoundColumnRefExpression& ref)
      -> std::optional<SearchColumnInfo> {
      // Mismatched table_index would mean the query referenced a table we
      // didn't set up -- always a bug in the test itself.
      SDB_ASSERT(ref.binding.table_index == table_index);
      const auto local = ref.binding.column_index.GetIndexUnsafe();
      const auto phys = projected[local].GetPrimaryIndex();
      return SearchColumnInfo{
        .column_id = columns[phys].id,
        .logical_type = columns[phys].type,
        .tokenizer = analyzer_provider(columns[phys].id)};
    };

    // Per-expression claim loop, mirroring production
    // (iresearch_plan.cpp:572-584): MakeSearchFilter is invoked once per
    // LogicalFilter expression, and a predicate that cannot be translated
    // is simply not claimed instead of failing the whole build. This is
    // also what lets us coexist with DuckDB optimizer rewrites (e.g. the
    // distributivity rule adding factored conjuncts to an OR) that may
    // introduce predicates the iresearch builder doesn't translate on its
    // own. When `expected_error` is set, the first throw is captured and
    // validated; remaining expressions are not processed.
    irs::And root;
    size_t claimed = 0;
    std::string caught_message;
    for (const auto& expr : filter_op->expressions) {
      const auto before = root.size();
      std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&expr, 1};
      try {
        auto result = sdb::connector::MakeSearchFilter(root, single, getter);
        if (result.ok() && root.size() > before) {
          ++claimed;
        } else {
          while (root.size() > before) {
            root.PopBack();
          }
        }
      } catch (const std::exception& e) {
        caught_message = e.what();
        break;
      }
    }
    if (!expected_error.empty()) {
      ASSERT_FALSE(caught_message.empty())
        << "expected MakeSearchFilter to throw";
      ASSERT_NE(caught_message.find(std::string{expected_error}),
                std::string::npos)
        << "exception message: <" << caught_message << ">\n"
        << "expected substring: <" << expected_error << ">";
      return;
    }
    ASSERT_TRUE(caught_message.empty())
      << "MakeSearchFilter threw unexpectedly: " << caught_message;
    ASSERT_EQ(claimed > 0, must_succeed);
    if (must_succeed) {
      ASSERT_EQ(root, expected) << "actual:   " << irs::ToString(root) << "\n"
                                << "expected: " << irs::ToString(expected);
    }
  }

 protected:
  duckdb::DuckDB _db;
  duckdb::Connection _conn;
};

// ---------------------------------------------------------------------------
// Tests (ported from velox search_filter_builder_test.cpp)
//
// DuckDB doesn't parse the PostgreSQL type aliases (bpchar, int2, int4) the
// velox test used in CAST expressions, so those become DuckDB's native
// spellings (VARCHAR, SMALLINT, INTEGER) here.
// ---------------------------------------------------------------------------

// ===========================================================================
// Type resolution
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_TypesResolving) {
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::FLOAT, .name = "b"}};
    irs::And expected;
    AddTermFilter<float>(expected, 1, 10);
    AssertFilter(expected, "SELECT * FROM foo WHERE b = 10", columns, true);
  }
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::DOUBLE, .name = "b"}};
    irs::And expected;
    AddTermFilter<double>(expected, 1, 10);
    AssertFilter(expected, "SELECT * FROM foo WHERE b = 10", columns, true);
  }
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::TINYINT, .name = "b"}};
    irs::And expected;
    AddTermFilter<int32_t>(expected, 1, 1);
    AssertFilter(expected, "SELECT * FROM foo WHERE b = CAST(1 AS VARCHAR)",
                 columns, true);
  }
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::SMALLINT, .name = "b"}};
    irs::And expected;
    AddTermFilter<int32_t>(expected, 1, 10);
    AssertFilter(expected, "SELECT * FROM foo WHERE b = CAST(10 AS SMALLINT)",
                 columns, true);
  }
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::SMALLINT, .name = "b"}};
    irs::And expected;
    AddRangeFilter<int32_t>(expected, 1, 10, false, std::nullopt, false);
    AssertFilter(expected, "SELECT * FROM foo WHERE b > CAST(10 AS SMALLINT)",
                 columns, true);
  }
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::SMALLINT, .name = "b"}};
    irs::And expected;
    AddTermsFilter<int32_t>(expected, 1, {10, 11});
    AssertFilter(expected,
                 "SELECT * FROM foo WHERE b IN (CAST(10 AS SMALLINT), "
                 "CAST(11 AS SMALLINT))",
                 columns, true);
  }
  {
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
    irs::And expected;
    AddTermFilter<bool>(expected, 1, true);
    AssertFilter(expected, "SELECT * FROM foo WHERE b = true", columns, true);
  }
  {
    // Non-identity analyzer rejects plain a = 'foo' on a VARCHAR column.
    // Use the TSQUERY surface (`b @@ 'foo'`) for analyzed columns.
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
    irs::And expected;
    AssertFilter(expected, "SELECT * FROM foo WHERE b = 'foo'", columns, false,
                 SegmentationAnalyzerProvider);
  }
}

// ===========================================================================
// OR / AND / NOT
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_SimpleDisjunction) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<int32_t>(or_filter, 1, 11);
  AssertFilter(expected, "SELECT * FROM foo WHERE b = 10 OR b = 11", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_SimpleDisjunctionDifferentFields) {
  std::vector<ColumnSpec> columns{
    {.id = 300, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 512, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 300, 10);
  AddTermFilter<std::string_view>(or_filter, 512, std::string_view{"foobar"});
  AssertFilter(expected, "SELECT * FROM foo WHERE a = '10' OR b = 'foobar'",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_MultipleOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 5);
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<int32_t>(or_filter, 1, 15);
  AssertFilter(expected, "SELECT * FROM foo WHERE a = 5 OR a = 10 OR a = 15",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_SimpleConjunction) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1, 10);
  AddTermFilter<int32_t>(expected, 2, 20);
  AssertFilter(expected, "SELECT * FROM foo WHERE a = 10 AND b = 20", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_MultipleAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1000, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2000, .type = duckdb::LogicalType::VARCHAR, .name = "b"},
    {.id = 3000, .type = duckdb::LogicalType::BOOLEAN, .name = "c"}};
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1000, 10);
  AddTermFilter<std::string_view>(expected, 2000, std::string_view{"test"});
  AddTermFilter<bool>(expected, 3000, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a = 10 AND b = 'test' AND c = true",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotTerm) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddTermFilter<int32_t>(not_filter, 1, 10);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a = '10')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_NotOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& or_filter = AddFilter<irs::Or>(not_filter);
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<int32_t>(or_filter, 1, 20);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a = 10 OR a = 20)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& and_filter = AddFilter<irs::And>(not_filter);
  AddTermFilter<int32_t>(and_filter, 1, 10);
  AddTermFilter<int32_t>(and_filter, 2, 20);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a = 10 AND b = 20)",
               columns, true);
}

// ===========================================================================
// Comparison operators
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_LessThanInteger) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a < 100", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LessThanString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"xyz"}, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a < 'xyz'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LessThanOrEqualInteger) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE a <= 100", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LessThanOrEqualString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"test"}, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE a <= 'test'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LessThanOrEqualStringNotIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE a <= 'test'", columns, false,
               SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GreaterThanInteger) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, false, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a > 50", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_GreaterThanString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"abc"}, false,
                                   std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a > 'abc'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_GreaterThanOrEqualInteger) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= 50", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_GreaterThanOrEqualString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"start"}, true,
                                   std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= 'start'", columns, true);
}

// ===========================================================================
// BETWEEN
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_BetweenInteger) {
  std::vector<ColumnSpec> columns{
    {.id = 500, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 500, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 500, std::nullopt, false, 100, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE a BETWEEN 10 AND 100",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_BetweenString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"apple"}, true,
                                   std::nullopt, false);
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"orange"}, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a BETWEEN 'apple' AND 'orange'",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotBetween) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddRangeFilter<int32_t>(or_filter, 1, std::nullopt, false, 10, false);
  AddRangeFilter<int32_t>(or_filter, 1, 50, false, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a NOT BETWEEN 10 AND 50",
               columns, true);
}

// ===========================================================================
// Combined AND/OR
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_AndWithOr) {
  std::vector<ColumnSpec> columns{
    {.id = 400, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 800, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<int32_t>(expected, 400, 10);
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 800, std::string_view{"foo"});
  AddTermFilter<std::string_view>(or_filter, 800, std::string_view{"bar"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a = 10 AND (b = 'foo' OR b = 'bar')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_AndWithComparison) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= 10 AND a <= 100",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_OrWithComparison) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddRangeFilter<int32_t>(or_filter, 1, std::nullopt, false, 10, false);
  AddRangeFilter<int32_t>(or_filter, 1, 100, false, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a < 10 OR a > 100", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_MixedEqualsAndComparison) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "status"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "age"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"active"});
  AddRangeFilter<int32_t>(expected, 2, 18, true, std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE status = 'active' AND age >= 18",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_ComparisonNotConst) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "status"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "age"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE status <= age", columns,
               false);
}

// ===========================================================================
// NOT combined with comparisons
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_NotWithComparison) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 50, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a > 50)", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotLessThan) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 100, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a < 100)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_NotGreaterThanOrEqual) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 50, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a >= 50)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_NotLessThanOrEqual) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 25, false, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a <= 25)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_AndWithNotOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "active"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "value"}};
  irs::And expected;
  AddTermFilter<bool>(expected, 1, true);
  auto& not_filter = expected.add<irs::Not>();
  auto& or_filter = AddFilter<irs::Or>(not_filter);
  AddTermFilter<int32_t>(or_filter, 2, 10);
  AddTermFilter<int32_t>(or_filter, 2, 20);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE active = true AND NOT (value = 10 OR value = 20)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_OrWithNot) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 5);
  auto& not_filter = or_filter.add<irs::And>().add<irs::Not>();
  AddTermFilter<std::string_view>(not_filter, 2, std::string_view{"test"});
  AssertFilter(expected, "SELECT * FROM foo WHERE a = 5 OR NOT (b = 'test')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_DoubleNegation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1, 10);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (NOT (a = 10))", columns,
               true);
}

// ===========================================================================
// Complex nested
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_ComplexNested) {
  std::vector<ColumnSpec> columns{
    {.id = 1024, .type = duckdb::LogicalType::INTEGER, .name = "price"},
    {.id = 2048, .type = duckdb::LogicalType::VARCHAR, .name = "tier"},
    {.id = 4096, .type = duckdb::LogicalType::BOOLEAN, .name = "enabled"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1024, 100, true, std::nullopt, false);
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 2048, std::string_view{"premium"});
  AddTermFilter<std::string_view>(or_filter, 2048, std::string_view{"gold"});
  auto& not_filter = expected.add<irs::Not>();
  AddTermFilter<bool>(not_filter, 4096, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE price >= 100 AND (tier = 'premium' OR "
               "tier = 'gold') AND NOT (enabled = false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NestedNotWithComparisons) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddRangeFilter<int32_t>(or_filter, 1, std::nullopt, false, 50, true);
  AddRangeFilter<int32_t>(or_filter, 2, 100, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a > 50 AND b < 100)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NestedNotWithOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And root;
  auto& expected = root.add<irs::And>();
  AddRangeFilter<int32_t>(expected, 1, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, true);
  AssertFilter(root, "SELECT * FROM foo WHERE NOT (a < 10 OR a > 100)", columns,
               true);
}

// ===========================================================================
// Implicit casts, multi-op
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_ImplicitCastIntegerToString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1, 42);
  AssertFilter(expected, "SELECT * FROM foo WHERE a = '42'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_ImplicitCastInComparison) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= '10'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_ImplicitCastInBetween) {
  std::vector<ColumnSpec> columns{
    {.id = 65535, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 65535, 5, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 65535, std::nullopt, false, 15, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE a BETWEEN '5' AND '15'",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_MultipleComparisonsOnSameField) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, false, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a > 10 AND a < 100", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_MixedOperatorsOnSameField) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "value"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 0);
  AddRangeFilter<int32_t>(or_filter, 1, 100, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE value = 0 OR value >= 100",
               columns, true);
}

// ===========================================================================
// IN operator
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_InOperatorIntegers) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {10, 20, 30, 40});
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (10, 20, 30, 40)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorStrings) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "status"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1,
    {std::string_view{"active"}, std::string_view{"pending"},
     std::string_view{"completed"}});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE status IN ('active', 'pending', 'completed')",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorStringsNotIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "status"}};
  irs::And expected;
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE status IN ('active', 'pending', 'completed')",
    columns, false, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorLongStrings) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "status"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1,
    {std::string_view{
       "ACTIVE LONG STRING THAT WILL NOT FIT INTO INLINE STRINGVIEW"},
     std::string_view{
       "pending super string that will not fit into inline string view"},
     std::string_view{
       "COMPLETED FULL STRING THAT WILL NOT FIT INTO INLINE STRING VIEW"}});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE status IN (UPPER('active long string that will "
    "not fit into inline stringView'), LOWER('pending super strinG that will "
    "not fit into inline string view'), UPPER('completed full string that will "
    "not fit into inline string view'))",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorWithImplicitCast) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "fibonacci"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {1, 2, 3, 5, 8, 13});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE fibonacci IN ('1', '2', '3', '5', '8', '13')",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorWithAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "type"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "value"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {10, 20, 30});
  AddRangeFilter<int32_t>(expected, 2, 100, true, std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE type IN (10, 20, 30) AND value >= 100",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorWithOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "category"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "name"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermsFilter<int32_t>(or_filter, 1, {1, 2, 3});
  AddTermFilter<std::string_view>(or_filter, 2, std::string_view{"special"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category IN (1, 2, 3) OR name = 'special'",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotIn) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "excluded"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddTermsFilter<int32_t>(not_filter, 1, {100, 200, 300});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE excluded NOT IN (100, 200, 300)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InWithSingleValue) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "answer"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {42});
  AssertFilter(expected, "SELECT * FROM foo WHERE answer IN (42)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorLargeColumnId) {
  std::vector<ColumnSpec> columns{
    {.id = 8192, .type = duckdb::LogicalType::INTEGER, .name = "code"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 8192, {10, 20, 30, 40, 50});
  AssertFilter(expected, "SELECT * FROM foo WHERE code IN (10, 20, 30, 40, 50)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InNotConst) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (10, b, 30, 40)",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_InNotConst2) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (b)", columns, false);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorIntegersNulls) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {10, 20, 30, 40});
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (10, 20, NULL, 30, 40)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorOnlyNulls) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "a"}};
  irs::And expected;
  AddFilter<irs::Empty>(expected);
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (NULL, NULL)", columns,
               true);
}

// ===========================================================================
// IS NULL / IS NOT NULL
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_IsNull) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "optional_field"}};
  irs::And expected;
  AddNullFilter(expected, 1);
  AssertFilter(expected, "SELECT * FROM foo WHERE optional_field IS NULL",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "description"}};
  irs::And expected;
  AddNullFilter(expected, 1);
  AssertFilter(expected, "SELECT * FROM foo WHERE description IS NULL", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_IsNotNull) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "required_field"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddNullFilter(not_filter, 1);
  AssertFilter(expected, "SELECT * FROM foo WHERE required_field IS NOT NULL",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_IsNotNotNull) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "required_field"}};
  irs::And expected;
  AddNullFilter(expected, 1);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT(required_field IS NOT NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotIsNull) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "required_field"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddNullFilter(not_filter, 1);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT(required_field IS NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullWithAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "deleted_at"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "status"}};
  irs::And expected;
  AddNullFilter(expected, 1);
  AddTermFilter<std::string_view>(expected, 2, std::string_view{"active"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE deleted_at IS NULL AND status = 'active'", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullWithOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "count"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddNullFilter(or_filter, 1);
  AddTermFilter<int32_t>(or_filter, 1, 0);
  AssertFilter(expected, "SELECT * FROM foo WHERE count IS NULL OR count = 0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullLargeColumnId) {
  std::vector<ColumnSpec> columns{
    {.id = 16384, .type = duckdb::LogicalType::VARCHAR, .name = "extra_data"}};
  irs::And expected;
  AddNullFilter(expected, 16384);
  AssertFilter(expected, "SELECT * FROM foo WHERE extra_data IS NULL", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullOrNotInside) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "field1"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "field2"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddNullFilter(or_filter, 1);
  auto& and_filter = or_filter.add<irs::And>();
  auto& not_filter = and_filter.add<irs::Not>();
  AddTermFilter<std::string_view>(not_filter, 2, std::string_view{"invalid"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE field1 IS NULL OR NOT (field2 = 'invalid')",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_ComplexWithInAndNull) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "category"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "priority"}};
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {1, 2, 3});
  auto& or_filter = expected.add<irs::Or>();
  AddNullFilter(or_filter, 2);
  AddRangeFilter<int32_t>(or_filter, 2, 100, true, std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category IN (1, 2, 3) AND (priority IS "
               "NULL OR priority >= 100)",
               columns, true);
}

// ===========================================================================
// LIKE
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_Like) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo_");
  AssertFilter(expected, "SELECT * FROM foo WHERE required_field LIKE '%foo_'",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LikeOp) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo_");
  AssertFilter(expected, "SELECT * FROM foo WHERE required_field ~~ '%foo_'",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LikeCustomEscape) {
  // LIKE '!%!!foo_' ESCAPE '!' -> iresearch wildcard pattern \%\!foo_
  // (Doubled escape '!!' becomes '\!' which iresearch treats as a plain
  // literal '!' -- equivalent to '!' on its own as far as matching goes.)
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "\\%\\!foo_");
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE required_field LIKE '!%!!foo_' ESCAPE '!'",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_NotLike) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddLikeFilter(not_filter, 1, "%bar_");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT(required_field LIKE '%bar_')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LikeWithFunc) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "!!!%FOO_");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE required_field LIKE UPPER('!!!%foo_')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LikeNotConst) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "value_field"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE required_field LIKE UPPER(value_field)",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_FieldCastError) {
  irs::And expected;
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::SMALLINT, .name = "b"}};
  AssertFilter(expected, "SELECT * FROM foo WHERE b = 999999999999", columns,
               false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b <= 999999999999", columns,
               false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b IN (1.24, 3.0, 4.5)",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_LikeWithNotIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE required_field LIKE UPPER('!!!%foo_')",
               columns, false, SegmentationAnalyzerProvider);
}

// ===========================================================================
// sdb_phrase
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_SimplePhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ PHRASE('quick brown fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_SimplePhraseNoFeatures) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AssertFilter(
    expected, "SELECT * FROM foo WHERE category @@ PHRASE('quick brown fox')",
    columns, false, SegmentationAnalyzerProviderBase<irs::IndexFeatures::Freq>);
}

TEST_F(SearchFilterBuilderTest, test_SimpleAndPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AddPhraseFilter(expected, 1, {"quick", "lazy", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ PHRASE('quick brown fox') "
               "AND category @@ PHRASE('quick lazy fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_SimpleOrPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddPhraseFilter(or_filter, 1, {"quick", "brown", "fox"});
  AddPhraseFilter(or_filter, 1, {"quick", "lazy", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ PHRASE('quick brown fox') OR "
               "category @@ PHRASE('quick lazy fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseExactGap) {
  // PHRASE(field, 'quick', 2, 'fox') -- exactly 2 words between 'quick' and
  // 'fox', e.g. "quick brown lazy fox"
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  // First term: offsets zeroed by insert() for the first element
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  // Second term: gap=2 words -> offs_min=offs_max=3 (2+1, no implicit +1)
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ PHRASE('quick', 2, 'fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseRangeGap) {
  // PHRASE(field, 'quick', ARRAY[1,2], 'fox') -- 1 to 2 words between 'quick'
  // and 'fox'
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  // gap=[1,2] words -> offs_min=2, offs_max=3 (min+1, max+1)
  phrase.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ PHRASE('quick', ARRAY[1,2], 'fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMultipleGaps) {
  // PHRASE(field, 'quick', 1, 'brown', 2, 'fox') -- multiple gaps
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  // gap=1 -> offs=2 (1+1)
  phrase.mutable_options()->push_back<irs::ByTermOptions>(2, 2).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"brown"});
  // gap=2 -> offs=3 (2+1)
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ PHRASE('quick', 1, 'brown', 2, 'fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseGapBetweenMultiTokenPatterns) {
  // PHRASE(field, 'quick brown', 2, 'lazy fox') -- multi-token patterns with a
  // gap: 'quick' adj 'brown', then gap=2, then 'lazy' adj 'fox'
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  // 'brown' is adjacent to 'quick' (within same pattern)
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"brown"});
  // 'lazy' is the first token of the next pattern -- gap=2 -> offs=3
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"lazy"});
  // 'fox' is adjacent to 'lazy' (within same pattern)
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ PHRASE('quick brown', 2, 'lazy fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseGapTrailingError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE category @@ PHRASE('quick', 2)", columns,
               false, SegmentationAnalyzerProvider, "PHRASE ends with a gap");
}

TEST_F(SearchFilterBuilderTest, test_PhraseConsecutiveGapsError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE category @@ PHRASE('quick', 1, 2, 'fox')",
               columns, false, SegmentationAnalyzerProvider,
               "PHRASE has consecutive gaps at argument 2");
}

TEST_F(SearchFilterBuilderTest, test_PhraseGapRangeMinExceedsMaxError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(
    irs::And{},
    "SELECT * FROM foo WHERE category @@ PHRASE('quick', ARRAY[3,1], 'fox')",
    columns, false, SegmentationAnalyzerProvider,
    "PHRASE interval gap must satisfy 0 <= min <= max, got [3, 1]");
}

// ===========================================================================
// Term equality / range / IN / LIKE -- migrated from the legacy
// TERM_EQ / TERM_LT / TERM_LIKE / TERM_IN / BOOST functions to the
// native SQL operators (= / < / <= / > / >= / IN / LIKE) on identity-
// analyzed columns and the TSQUERY surface (`@@ 'val'`,
// `@@ LIKE(...)`, `@@ ANY_OF([...])`) on analyzed columns.
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_TermEq_Segmentation) {
  // `b @@ 'fOo'` on a segmenting analyzer tokenises 'fOo' to 'foo'
  // and emits ByTerm (single-token tokenisation collapses into a
  // ByTerm; multi-token would emit ByTerms with min_match=1).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'fOo'", columns, true,
               SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermEq_Identity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b = 'foo'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TermLess_Identity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"Foo"}, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b < 'Foo'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TermLess_Segmentation) {
  // LESS / LESS_EQ / GREATER / GREATER_EQ on the TSQUERY surface
  // tokenise their VARCHAR argument via the ambient analyzer
  // (segmenting -> 'Foo' becomes 'foo') and use the resulting single
  // token as the range bound.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ LESS('Foo')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermGreater_Segmentation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"foo"}, false,
                                   std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ GREATER('foo')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLessEq_Segmentation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ LESS_EQ('foo')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermGreaterEq_Segmentation) {
  // 'fOo' tokenises to 'foo' under segmenting; the token is used as
  // the inclusive lower bound.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"foo"}, true,
                                   std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ GREATER_EQ('fOo')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermGreaterEq_AndLessEq_Composed) {
  // Composing GE & LE inside @@ via TSQUERY `&&` produces an iresearch
  // And of two ByRange filters.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  AddRangeFilter<std::string_view>(and_group, 1, std::string_view{"apple"},
                                   true, std::nullopt, false);
  AddRangeFilter<std::string_view>(and_group, 1, std::nullopt, false,
                                   std::string_view{"orange"}, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a @@ (GREATER_EQ('apple') && LESS_EQ('orange'))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLess_IntegerColumn) {
  // LESS on a numeric column emits irs::ByGranularRange (same shape
  // as RANGE's numeric path).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ LESS(100)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermGreaterEq_IntegerColumn) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ GREATER_EQ(50)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TermLessEq_BooleanColumn) {
  // LESS_EQ on a BOOLEAN column emits irs::ByRange via BooleanTokenizer.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
  irs::And expected;
  auto& range = expected.add<irs::ByRange>();
  *range.mutable_field() = MakeFieldName<bool>(1);
  auto& opts = range.mutable_options()->range;
  opts.max.assign(
    irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(true)));
  opts.max_type = irs::BoundType::Inclusive;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ LESS_EQ(true)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermLess_TypeMismatchVarcharVsInt) {
  // VARCHAR bound on an INTEGER column -> bind-time error, same shape
  // as RANGE's mismatch error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({}, "SELECT * FROM foo WHERE b @@ LESS('foo')", columns, false,
               IdentityAnalyzerProvider, "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest, test_TermLess_NullBoundRejected) {
  // NULL bound is a bind-time error -- use RANGE(NULL, ...) for
  // unbounded semantics.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({}, "SELECT * FROM foo WHERE b @@ LESS(NULL)", columns, false,
               IdentityAnalyzerProvider, "bound must be non-null");
}

TEST_F(SearchFilterBuilderTest, test_TermLike_Segmentation) {
  // LIKE on the TSQUERY surface does NOT tokenise the pattern --
  // it's a raw wildcard match against indexed terms.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foO_");
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (LIKE('%foO_'))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_Identity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%fOo_");
  AssertFilter(expected, "SELECT * FROM foo WHERE b LIKE '%fOo_'", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_EscapedPatternIdentity) {
  // Escaped LIKE pattern: the leading `\%` is a literal `%` (the `%`
  // is no longer a wildcard) per DuckDB's default `\`-escape rule.
  // The iresearch wildcard pattern bytes preserve the backslash
  // escape so the indexed term must contain a literal `%` followed
  // by `!foo` followed by exactly one trailing character.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "\\%!foo_");
  AssertFilter(expected, "SELECT * FROM foo WHERE b LIKE '\\%!foo_'", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_EscapedPatternSegmentation) {
  // Same escaped pattern through the TSQUERY-surface LIKE
  // constructor on a non-identity column. LIKE on the TSQUERY
  // surface does NOT tokenise; the pattern is matched as wildcard
  // bytes against indexed terms exactly like the identity case.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "\\%!foo_");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ (LIKE('\\%!foo_'))", columns,
               true, SegmentationAnalyzerProvider);
}

// Columns indexed by WildcardAnalyzer get the ngram-aware ByWildcardNgram
// filter (instead of ByWildcard), so the LIKE pattern is evaluated through
// the inverted index using the analyzer's ngram tokenization.
TEST_F(SearchFilterBuilderTest, test_TermLike_WildcardAnalyzer) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddWildcardNgramFilter(expected, 1, "%foo_", true);
  AssertFilter(expected, "SELECT * FROM foo WHERE b LIKE '%foo_'", columns,
               true, WildcardAnalyzerProvider);
}

// Same column kind, accessed via the TSQUERY surface -- exercises
// BuildFtsLike's WildcardAnalyzer dispatch.
TEST_F(SearchFilterBuilderTest, test_TermLike_WildcardAnalyzer_TSQuery) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddWildcardNgramFilter(expected, 1, "%foo_", true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ (LIKE('%foo_'))", columns,
               true, WildcardAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_WildcardAnalyzer_NotConst) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE a LIKE b", columns, false,
               WildcardAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_WildcardAnalyzer_WithNot) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddWildcardNgramFilter(not_filter, 1, "%foo_", true);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT(a LIKE '%foo_')",
               columns, true, WildcardAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermIn_Segmentation) {
  // ANY_OF on a segmenting analyzer tokenises each list element; for
  // single-token elements that's just ByTerms with one entry per
  // tokenised input.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"foo"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"bar"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"baz"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ANY_OF(['foo', 'bar', 'baZ'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermIn_Identity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1,
    {std::string_view{"foo"}, std::string_view{"bAr"},
     std::string_view{"baz"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b IN ('foo', 'bAr', 'baz')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermEq_WithAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AddRangeFilter<int32_t>(expected, 2, 10, true, std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a @@ 'foo' AND b >= 10", columns, true,
               SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermEq_WithOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"foo"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"bar"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a @@ 'foo' OR a @@ 'bar'", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_WithNot) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddLikeFilter(not_filter, 1, "%foo_");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT(a @@ (LIKE('%foo_')))", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermIn_WithAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"y"});
  AddRangeFilter<int32_t>(expected, 2, 10, true, std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a @@ ANY_OF(['x', 'y']) AND b >= 10",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermEq_NotConst) {
  // Comparing two columns has no claimable shape on either surface --
  // the predicate is left for runtime.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE a = b", columns, false);
}

// ===========================================================================
// NGRAM -- TSQUERY-surface n-gram similarity. The canonical basic
// case is here; bare-name and threshold variants exercise arity, the
// no-features case verifies the analyzer-feature requirement.
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramBasic) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ NGRAM('hello')",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramWithThreshold) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"}, 0.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ NGRAM('hello', 0.5)", columns,
               true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramNoFeatures) {
  // Default identity analyzer doesn't have Pos+Freq features required
  // for NGRAM -- bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ NGRAM('hello')",
               columns, false);
}

// ===========================================================================
// LEVENSHTEIN -- TSQUERY-surface fuzzy matching. Edge cases that the
// canonical TSQUERY-surface block (`test_TSQueryMatch_Levenshtein`)
// doesn't already cover.
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinNoTranspositions) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2, false);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ LEVENSHTEIN('test', 2, false)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinDistanceTooHigh) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ LEVENSHTEIN('test', 5)", columns,
               false);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_LevenshteinTranspositionDistanceTooHigh) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ LEVENSHTEIN('test', 4, true)",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinNotNegation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& negated = expected.add<irs::Not>();
  AddEditDistanceFilter(negated, 1, "test", 2);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ !!LEVENSHTEIN('test', 2)",
               columns, true);
}

// ===========================================================================
// Boost (`^`) -- migrated from the legacy `BOOST(predicate, factor)`
// function to the TSQUERY-surface `^` operator. The new surface only
// supports boost on TSQUERY values (composes inside `@@`); per-
// predicate boost on arbitrary BOOLEAN expressions is no longer
// available.
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_Boost_TermEq) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(2.0f);
  // The `'foo'::TSQUERY` cast disambiguates `^` from the numeric
  // power overload -- without it DuckDB tries `^(VARCHAR, DOUBLE)`
  // which doesn't exist.
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY ^ 2.0", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Phrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"}).boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ "
               "PHRASE('quick brown fox') ^ 1.5",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Like) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "foo%").boost(3.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ (LIKE('foo%')) ^ 3.0", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_WildcardFilter) {
  // Boost on a TSQUERY-surface LIKE against a WildcardAnalyzer column
  // dispatches to ByWildcardNgram and threads the boost through.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddWildcardNgramFilter(expected, 1, "foo%", true).boost(3.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ (LIKE('foo%')) ^ 3.0", columns,
               true, WildcardAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramBoost) {
  // NGRAM(...) ^ N -- TSQUERY-surface boost on n-gram similarity.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"fo", "oo"}).boost(2.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ NGRAM('foo') ^ 2.5", columns,
               true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinBoost) {
  // LEVENSHTEIN(...) ^ N -- TSQUERY-surface boost on Levenshtein.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2).boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ LEVENSHTEIN('test', 2) ^ 1.5",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_TermIn) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  or_group.boost(2.0f);
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"foo"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"bar"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ANY_OF(['foo', 'bar']) ^ 2.0", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_RangeComparison) {
  // `^` on an LT/LE/GT/GE TSQUERY value boosts the resulting ByRange.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, false)
    .boost(1.5f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ LESS('foo') ^ 1.5",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_AndGroup) {
  // `^` on a TSQUERY `&&` group boosts the whole conjunction.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& group = expected.add<irs::And>();
  group.boost(3.0f);
  AddTermFilter<std::string_view>(group, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(group, 1, std::string_view{"y"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ('x'::TSQUERY && 'y'::TSQUERY) ^ 3.0",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_OrGroup) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& group = expected.add<irs::Or>();
  group.boost(2.0f);
  AddTermFilter<std::string_view>(group, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(group, 1, std::string_view{"y"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ('x'::TSQUERY || 'y'::TSQUERY) ^ 2.0",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Zero) {
  // Boost factor 0 disables scoring contribution but still claims.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(0.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY ^ 0.0", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Negative) {
  // Negative boost factor is rejected at bind time.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY ^ -1.0", columns,
               false);
}

// ===========================================================================
// `@@` TSQUERY surface (v1 redesign)
//
// The stubs registered in search.cpp are claimed at bind time by the filter
// builder. Tests here lock down the dispatch for the primary surface:
//
//  - operators: @@, ||, &&, !!, ##, ^
//  - unprefixed constructors: PHRASE, LIKE, PREFIX, LEVENSHTEIN, ANY_OF, ALL_OF
//  - PG-compat: plainto_tsquery, phraseto_tsquery, tsquery_phrase
//  - commutative @@ (column on either side)
//
// Deferred / NOT covered yet (return errors):
//  - to_tsquery (Lucene parser)
//  - websearch_to_tsquery (mini-parser)
//  - TOKENIZE(text, analyzer) / ::tokenizer(name) cast
//  - Bare-string tokenisation through column analyzer (currently raw ByTerm)
//  - ByTerms optimisation for ANY_OF / ALL_OF
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Phrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ PHRASE('quick brown fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BareStringIsTerm) {
  // v1: bare string is a raw ByTerm (no tokenisation yet).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'quick'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_OrOfPhrases) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddPhraseFilter(or_filter, 1, {"quick", "brown"});
  AddPhraseFilter(or_filter, 1, {"lazy", "dog"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "(PHRASE('quick brown') || PHRASE('lazy dog'))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AndOfPhrases) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddPhraseFilter(and_filter, 1, {"quick", "brown"});
  AddPhraseFilter(and_filter, 1, {"lazy", "dog"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "(PHRASE('quick brown') && PHRASE('lazy dog'))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Not) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_group = expected.add<irs::Not>();
  auto& inner = not_group.filter<irs::ByTerm>();
  *inner.mutable_field() = MakeFieldName<std::string_view>(1);
  irs::StringTokenizer stream;
  const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
  stream.reset(std::string_view{"spam"});
  stream.next();
  inner.mutable_options()->term.assign(token->value);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ !!'spam'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostOperator) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"})
    .boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'quick'::TSQUERY ^ 2.0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsLiteral) {
  // `'quick' @@ b` -- column is on RHS. Same filter tree as `b @@ 'quick'`.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"});
  AssertFilter(expected, "SELECT * FROM foo WHERE 'quick' @@ b", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsPhrase) {
  // PHRASE(...) @@ b -- mirrors test_TSQueryMatch_Phrase.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE PHRASE('quick brown fox') @@ b",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsOr) {
  // (PHRASE || PHRASE) @@ b -- mirrors test_TSQueryMatch_OrOfPhrases.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddPhraseFilter(or_filter, 1, {"quick", "brown"});
  AddPhraseFilter(or_filter, 1, {"lazy", "dog"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE "
               "(PHRASE('quick brown') || PHRASE('lazy dog')) @@ b",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsAnd) {
  // (PHRASE && PHRASE) @@ b -- mirrors test_TSQueryMatch_AndOfPhrases.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddPhraseFilter(and_filter, 1, {"quick", "brown"});
  AddPhraseFilter(and_filter, 1, {"lazy", "dog"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE "
               "(PHRASE('quick brown') && PHRASE('lazy dog')) @@ b",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsAnyOf) {
  // ANY_OF([..::TSQUERY]) @@ b -- list form on LHS, column on RHS.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE "
               "ANY_OF(['a'::TSQUERY, 'b'::TSQUERY]) @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsAnyOfList) {
  // ANY_OF([list]) @@ b -- bare-string list form on LHS.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ANY_OF(['a', 'b']) @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsPhraseSeq) {
  // 'a' ## 1 ## 'b' @@ b -- bare-string ## with column on RHS.
  // Mirrors test_TSQueryMatch_PhraseSeqExactGap. SegmentationAnalyzer
  // is required because ## emits a phrase that needs Pos+Freq features.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(2, 2).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ('a' ## 1 ## 'b') @@ b", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsBoost) {
  // (PHRASE ^ 2.0) @@ b -- mirrors test_TSQueryMatch_BoostOperator.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"})
    .boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE 'quick'::TSQUERY ^ 2.0 @@ b", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsLike) {
  // "LIKE"(pattern) @@ b -- mirrors test_TSQueryMatch_LikeWildcard.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, std::string_view{"quic%"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (\"LIKE\"('quic%')) @@ b", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsToTsquery) {
  // to_tsquery(...) @@ b -- mirrors test_TSQueryMatch_ToTsqueryAnd.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& mixed = expected.add<irs::MixedBooleanFilter>();
  AddTermFilter<std::string_view>(mixed.GetRequired(), 1,
                                  std::string_view{"quick"});
  AddTermFilter<std::string_view>(mixed.GetRequired(), 1,
                                  std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE to_tsquery('quick AND brown') @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsTokenizerCast) {
  // 'foo'::tokenizer('identity') @@ b -- mirrors
  // test_TSQueryMatch_TokenizerCastIdentity.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE 'quick fox'::tokenizer('identity') @@ b", columns,
    true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeAmbiguousColumns) {
  // Both sides are indexed VARCHAR columns -- bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "c"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ c", columns, false);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LikeWildcard) {
  // LIKE is TYPE_FUNC_NAME_KEYWORD in DuckDB's grammar. Using it as a
  // bare function call right after `@@` fails to parse because the
  // parser treats LIKE as an infix operator in that position. Parens
  // around the RHS disambiguate, and double-quoted "LIKE" bypasses the
  // keyword rule entirely. We lock in the double-quoted form for
  // predictability.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, std::string_view{"quic%"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (\"LIKE\"('quic%'))",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Prefix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, std::string_view{"qu"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ PREFIX('qu')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Regexp) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"qu.*ck"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ REGEXP('qu.*ck')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpPerlExplicit) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"\\d+"},
                  irs::RegexpSyntax::Perl);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ REGEXP('\\d+', 'perl')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpPosix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"[[:alpha:]]+"},
                  irs::RegexpSyntax::PosixEre);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ REGEXP('[[:alpha:]]+', 'posix')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpSyntaxCaseInsensitive) {
  // Syntax names are matched case-insensitively.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"abc"},
                  irs::RegexpSyntax::PosixEre);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ REGEXP('abc', 'POSIX')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpUnknownSyntax) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ REGEXP('abc', 'pcre')", columns,
               false, IdentityAnalyzerProvider,
               "REGEXP syntax must be one of");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpNonVarcharColumn) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ REGEXP('abc')", columns, false,
               IdentityAnalyzerProvider, "REGEXP field is not VARCHAR");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpUnderNot) {
  // Negated regexp: NOT REGEXP('foo.*').
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& re = not_filter.filter<irs::ByRegexp>();
  *re.mutable_field() = MakeFieldName<std::string_view>(1);
  re.mutable_options()->pattern.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"foo.*"}));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ !!REGEXP('foo.*')", columns,
               true);
}


TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Levenshtein) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, std::string_view{"quikc"}, 2, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ LEVENSHTEIN('quikc', 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeVarchar) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"a"}, true,
                                   std::string_view{"f"}, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE('a', 'f', true, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeVarcharOpenLeft) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"m"}, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE(NULL, 'm', false, true)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeVarcharOpenRight) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"a"}, false,
                                   std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE('a', NULL, false, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeInt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, true, 100, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE(10, 100, true, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeBool) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
  irs::And expected;
  auto& range = expected.add<irs::ByRange>();
  *range.mutable_field() = MakeFieldName<bool>(1);
  auto& opts = range.mutable_options()->range;
  opts.min.assign(
    irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(false)));
  opts.min_type = irs::BoundType::Inclusive;
  opts.max.assign(
    irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(true)));
  opts.max_type = irs::BoundType::Inclusive;
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ RANGE(false, true, true, true)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeBothNullMatchesAll) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::All>();
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ RANGE(NULL, NULL, false, false)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangePhrasePart) {
  // 'a' ## RANGE('b', 'd', true, true) -- RANGE as phrase part
  // emits a ByRangeOptions slot at the phrase position.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  auto& rng_opts =
    phrase.mutable_options()->push_back<irs::ByRangeOptions>(1, 1);
  rng_opts.range.min.assign(irs::ViewCast<irs::byte_type>(std::string_view{"b"}));
  rng_opts.range.min_type = irs::BoundType::Inclusive;
  rng_opts.range.max.assign(irs::ViewCast<irs::byte_type>(std::string_view{"d"}));
  rng_opts.range.max_type = irs::BoundType::Inclusive;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## RANGE('b', 'd', true, true))",
               columns, true, SegmentationAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// RANGE negative cases. Mismatched min/max types and value-vs-column
// type mismatches are bind-time errors (throw InvalidInputException).
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedVarcharInt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE('a', 5, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "RANGE bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedIntVarchar) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE(1, 'foo', true, true)",
               columns, false, IdentityAnalyzerProvider,
               "RANGE bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedBoolInt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE(false, 1, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "RANGE bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedVarcharBool) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE('a', true, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "RANGE bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeIntBigintMismatch) {
  // INTEGER vs BIGINT are different LogicalType ids -- the strict
  // type-identity rule rejects mixing them. Users should make both
  // bounds the same type.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "RANGE(1::INTEGER, 100::BIGINT, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "RANGE bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeColumnMismatchVarcharVsInt) {
  // Bounds are VARCHAR but column is INTEGER -- column-vs-value type
  // mismatch (different from min/max mismatch).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE('a', 'z', true, true)",
               columns, false, IdentityAnalyzerProvider,
               "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeColumnMismatchIntVsVarchar) {
  // Bounds are INTEGER but column is VARCHAR.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE(1, 100, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_RangeOnlyOneBoundColumnMismatch) {
  // Type-mismatch check uses the non-NULL bound when one side is NULL;
  // INT bound + NULL bound on a VARCHAR column should still error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE(NULL, 5, false, true)",
               columns, false, IdentityAnalyzerProvider,
               "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeNonIdentityVarchar) {
  // VARCHAR column with a non-identity analyzer rejects standalone
  // RANGE (the indexed tokens are transformed; raw bounds wouldn't
  // line up with them).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ RANGE('a', 'm', true, true)",
               columns, false, SegmentationAnalyzerProvider,
               "requires identity-analyzed column");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangePhraseNumericRejected) {
  // Inside ##, only VARCHAR bounds are meaningful (phrase positions
  // live on the analyzed text field). Numeric bounds throw.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## RANGE(1, 5, true, true))",
               columns, false, SegmentationAnalyzerProvider,
               "## RANGE phrase part requires VARCHAR bounds");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangePhraseBoolRejected) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## RANGE(false, true, true, true))",
               columns, false, SegmentationAnalyzerProvider,
               "## RANGE phrase part requires VARCHAR bounds");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeIntExclusiveBoth) {
  // Exclusive bounds on numeric: a > 1 AND a < 5.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 1, false, 5, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE(1, 5, false, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeIntInclusiveExclusive) {
  // Mixed inclusivity: a >= 1 AND a < 5.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 1, true, 5, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE(1, 5, true, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeBigint) {
  // BIGINT column accepts BIGINT bounds.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BIGINT, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int64_t>(expected, 1, int64_t{10}, true, int64_t{100}, true);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ "
    "RANGE(10::BIGINT, 100::BIGINT, true, true)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeDouble) {
  // DOUBLE column accepts DOUBLE bounds.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::DOUBLE, .name = "b"}};
  irs::And expected;
  AddRangeFilter<double>(expected, 1, 1.5, true, 9.5, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ RANGE(1.5, 9.5, true, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeBoolOpenRight) {
  // Open-right BOOLEAN range: just `false` (or unbounded above).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
  irs::And expected;
  auto& range = expected.add<irs::ByRange>();
  *range.mutable_field() = MakeFieldName<bool>(1);
  auto& opts = range.mutable_options()->range;
  opts.min.assign(
    irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(false)));
  opts.min_type = irs::BoundType::Inclusive;
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ RANGE(false, NULL, true, false)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTsqueryList) {
  // ANY_OF([..::TSQUERY]) -- list of explicitly-typed TSQUERYs.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ANY_OF(['quick'::TSQUERY, "
               "'fox'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfTsqueryList) {
  // ALL_OF([..::TSQUERY]) -- list of explicitly-typed TSQUERYs.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ALL_OF(['quick'::TSQUERY, "
               "'brown'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfBareStringList) {
  // ANY_OF([bare strings]) -- the registered VARCHAR[] -> TSQUERY[]
  // list cast lifts each element via the implicit VARCHAR -> TSQUERY
  // scalar cast.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ANY_OF(['quick', 'fox'])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfBareStringList) {
  // ALL_OF([bare strings]) via VARCHAR[] -> TSQUERY[] list cast.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ALL_OF(['quick', 'brown'])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_CommutativeLhsAnyOfBareStringList) {
  // ANY_OF([bare strings]) @@ b -- column on RHS.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ANY_OF(['a', 'b']) @@ b", columns,
               true);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_CommutativeLhsAllOfBareStringList) {
  // ALL_OF([bare strings]) @@ b.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ALL_OF(['a', 'b']) @@ b", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfList) {
  // ANY_OF([list]) -- explicit list form. Equivalent to variadic.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"c"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ANY_OF(['a'::TSQUERY, 'b'::TSQUERY, 'c'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfListMinMatch) {
  // ANY_OF([list], min_match) -- min_match=2 means at least 2 of 3
  // alternatives must match. Encoded via irs::Or.min_match_count.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  or_filter.min_match_count(2);
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"c"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ANY_OF(['a', 'b', 'c'], 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfList) {
  // ALL_OF([list]) -- explicit list form, no min_match.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ALL_OF(['x'::TSQUERY, 'y'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfMinExceedsArgs) {
  // min_match exceeds number of args -> bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ANY_OF(['a', 'b'], 5)",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfRejectsMinMatchArg) {
  // ALL_OF doesn't accept a min_match argument (no `(list, int)` arm).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ALL_OF(['x', 'y'], 1)", columns,
               false);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_ToTsqueryTerm) {
  // to_tsquery('quick') -- single term via the Lucene parser. Identity
  // analyzer keeps the term raw.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& mixed = expected.add<irs::MixedBooleanFilter>();
  AddTermFilter<std::string_view>(mixed.GetOptional(), 1,
                                  std::string_view{"quick"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ to_tsquery('quick')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_ToTsqueryAnd) {
  // to_tsquery('quick AND brown') -- conjunction via Lucene's AND.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& mixed = expected.add<irs::MixedBooleanFilter>();
  AddTermFilter<std::string_view>(mixed.GetRequired(), 1,
                                  std::string_view{"quick"});
  AddTermFilter<std::string_view>(mixed.GetRequired(), 1,
                                  std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ to_tsquery('quick AND brown')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_ToTsqueryOr) {
  // to_tsquery('quick OR brown') -- disjunction via Lucene's OR.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& mixed = expected.add<irs::MixedBooleanFilter>();
  AddTermFilter<std::string_view>(mixed.GetOptional(), 1,
                                  std::string_view{"quick"});
  AddTermFilter<std::string_view>(mixed.GetOptional(), 1,
                                  std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ to_tsquery('quick OR brown')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_ToTsqueryError) {
  // Bad Lucene syntax surfaces as a bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ to_tsquery('AND AND AND')",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseToTsquery) {
  // phraseto_tsquery shares PHRASE's body.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ phraseto_tsquery('quick brown')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqExactGap) {
  // 'a' ## 2 ## 'b' -- bare INTEGER gap operand (no FTS_NEAR).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ('a'::TSQUERY ## 2 ## 'b'::TSQUERY)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqInterval) {
  // 'a' ## [1, 3] ## 'b' -- bare INTEGER[] interval gap.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## [1, 3] ## 'b'::TSQUERY)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAnyOfPart) {
  // 'a' ## ANY_OF(['b', 'c']) -- ANY_OF as phrase part maps to a
  // ByTermsOptions slot at the phrase position with min_match=1.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  auto& terms = phrase.mutable_options()->push_back<irs::ByTermsOptions>(1, 1);
  terms.min_match = 1;
  terms.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"b"}));
  terms.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"c"}));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ANY_OF(['b', 'c']))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAnyOfPartExplicit1) {
  // 'a' ## ANY_OF(['b', 'c'], 1) -- explicit min_match=1 is allowed
  // and is the only accepted form besides the no-min_match default.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"a"});
  auto& terms = phrase.mutable_options()->push_back<irs::ByTermsOptions>(1, 1);
  terms.min_match = 1;
  terms.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"b"}));
  terms.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"c"}));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ANY_OF(['b', 'c'], 1))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAnyOfPartMinMatchRejected) {
  // ANY_OF with min_match > 1 inside ## is rejected: a phrase position
  // can match only one token, so min_match > 1 is unsatisfiable.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ANY_OF(['b', 'c'], 2))",
               columns, false, SegmentationAnalyzerProvider,
               "ANY_OF phrase part requires min_match=1");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAllOfPartRejected) {
  // ALL_OF as a phrase part is rejected (same reason as ANY_OF
  // with min_match > 1: a phrase position holds at most one token).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ALL_OF(['b', 'c']))",
               columns, false, SegmentationAnalyzerProvider,
               "ALL_OF phrase part is not supported");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TsqueryPhraseFunction) {
  // tsquery_phrase(PHRASE('hello'), PHRASE('world'), 3) -- function
  // form of ##.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"hello"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(4, 4).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"world"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "tsquery_phrase(PHRASE('hello'), PHRASE('world'), 3)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BitwiseOnIntegersUnchanged) {
  // Smoke test: non-FTS expressions continue to work.
  auto res = _conn.Query("SELECT 5 | 3");
  ASSERT_FALSE(res->HasError());
  ASSERT_EQ(res->types[0].id(), duckdb::LogicalTypeId::INTEGER);
  auto chunk = res->Fetch();
  ASSERT_EQ(chunk->GetValue(0, 0).GetValue<int32_t>(), 7);

  auto res2 = _conn.Query("SELECT 'a' || 'b'");
  ASSERT_FALSE(res2->HasError());
  ASSERT_EQ(res2->types[0].id(), duckdb::LogicalTypeId::VARCHAR);
  auto chunk2 = res2->Fetch();
  ASSERT_EQ(chunk2->GetValue(0, 0).GetValue<std::string>(), "ab");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BareMultiTokenOr) {
  // On a tokenising analyzer, a bare multi-word string produces a
  // ByTerms with min_match=1 (ANY_OF semantics). Matches rows
  // containing either token.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(expected, 1, {"quick", "fox"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'quick fox'",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BareSingleTokenTerm) {
  // Single-token bare string stays as ByTerm (no ByTerms wrapping).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'quick'", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizeFunction) {
  // TOKENIZE(text) 1-arg is same as bare-string semantics.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(expected, 1, {"quick", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ TOKENIZE('quick fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizeIdentity) {
  // TOKENIZE(text, 'identity') bypasses the column analyzer entirely
  // and emits a raw ByTerm -- matches the unsplit input string as-is.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ TOKENIZE('quick fox', 'identity')", columns,
    true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizerCastIdentity) {
  // 'foo'::tokenizer('identity') -- parameterized-type cast equivalent
  // to TOKENIZE(text, 'identity'). Bypasses the column analyzer.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ 'quick fox'::tokenizer('identity')", columns,
    true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizerCastNamedNotImpl) {
  // 'foo'::tokenizer('english') with a non-identity name is deferred.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ 'quick fox'::tokenizer('english')", columns,
    false, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizeNamedAnalyzerNotImpl) {
  // 2-arg TOKENIZE(text, analyzer) with a non-identity analyzer name
  // is deferred -- catalog-registered analyzer lookup is not yet plumbed
  // through the filter builder.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ TOKENIZE('quick fox', 'english')",
               columns, false, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PlainToTsqueryAnd) {
  // plainto_tsquery(text) = tokenise + AND of all tokens (min_match=count).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;  // ALL of 2 tokens
    {
      irs::StringTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(std::string_view{"quick"});
      stream.next();
      opts.terms.emplace(token->value);
      stream.reset(std::string_view{"fox"});
      stream.next();
      opts.terms.emplace(token->value);
    }
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ plainto_tsquery('quick fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BareEmptyStopwordsEmpty) {
  // Bare string that yields 0 tokens after analysis is an empty filter.
  // Our segmentation analyzer produces tokens from whitespace-split;
  // an empty string produces no tokens.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::Empty>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ''", columns, true,
               SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchSingleWord) {
  // Single bare word -- same as `col @@ 'quick'` via BuildFtsTokens.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchAndOfWords) {
  // `quick brown` -> AND of two tokens (each wrapped in an
  // analyzer-driven leaf). Each bare word goes through BuildFtsTokens
  // which for single-token input emits ByTerm directly.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"brown"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick brown')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchPhrase) {
  // Quoted phrase `"quick brown"` is a ByPhrase.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('\"quick brown\"')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchOrChain) {
  // `quick OR fox` -> single group of OR'd atoms.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick OR fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchOrPrecedence) {
  // `quick OR fox brown` -> (quick OR fox) AND brown. OR binds tighter
  // than implicit AND.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  auto& or_group = and_group.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"fox"});
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"brown"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick OR fox brown')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchNegation) {
  // `quick -spam` -> quick AND NOT spam.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"quick"});
  auto& not_group = and_group.add<irs::Not>();
  auto& inner = not_group.filter<irs::ByTerm>();
  *inner.mutable_field() = MakeFieldName<std::string_view>(1);
  {
    irs::StringTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    stream.reset(std::string_view{"spam"});
    stream.next();
    inner.mutable_options()->term.assign(token->value);
  }
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick -spam')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchFullExample) {
  // `"Quick Fox" OR slow -spam` ->
  //   AND { OR { ByPhrase[quick, fox], ByTerm(slow) }, NOT ByTerm(spam) }
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  auto& or_group = and_group.add<irs::Or>();
  AddPhraseFilter(or_group, 1, {"quick", "fox"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"slow"});
  auto& not_group = and_group.add<irs::Not>();
  auto& inner = not_group.filter<irs::ByTerm>();
  *inner.mutable_field() = MakeFieldName<std::string_view>(1);
  {
    irs::StringTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    stream.reset(std::string_view{"spam"});
    stream.next();
    inner.mutable_options()->term.assign(token->value);
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "websearch_to_tsquery('\"Quick Fox\" OR slow -spam')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_WebsearchEmpty) {
  // Empty input -> Empty filter (no match claim).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::Empty>();
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('')",
               columns, true, SegmentationAnalyzerProvider);
}

}  // namespace
