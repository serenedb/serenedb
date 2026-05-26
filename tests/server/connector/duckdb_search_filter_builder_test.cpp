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
#include <s2/s2latlng.h>
#include <vpack/parser.h>

#include <duckdb.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/geo_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
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
  uint64_t id;
  duckdb::LogicalType type;
  std::string name;
};

using AnalyzerProvider = std::function<catalog::ColumnTokenizer(uint64_t)>;

catalog::ColumnTokenizer IdentityAnalyzerProvider(uint64_t) {
  static catalog::Tokenizer gStringTokenizer(
    ObjectId{0}, ObjectId{12345}, "test_string_verbartim", {},
    DEFAULT_ROW_GROUP_SIZE,
    irs::analysis::TokenizerConfig{
      .config = irs::StringTokenizer::Options{}});
  auto tokenizer = gStringTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer),
          .features = irs::IndexFeatures::None};
}

template<irs::IndexFeatures Features>
catalog::ColumnTokenizer SegmentationAnalyzerProviderBase(uint64_t) {
  static catalog::Tokenizer gStringTokenizer(
    ObjectId{0}, ObjectId{12346}, "test_segmentation", {},
    DEFAULT_ROW_GROUP_SIZE,
    irs::analysis::TokenizerConfig{
      .config = irs::analysis::SegmentationTokenizer::Options{}});
  auto tokenizer = gStringTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer), .features = Features};
}

[[maybe_unused]] catalog::ColumnTokenizer SegmentationAnalyzerProvider(
  uint64_t id) {
  return SegmentationAnalyzerProviderBase<irs::IndexFeatures::Pos |
                                          irs::IndexFeatures::Freq>(id);
}

[[maybe_unused]] catalog::ColumnTokenizer NgramAnalyzerProvider(uint64_t) {
  irs::analysis::NGramTokenizerBase::Options ngram_opts{
    .min_gram = 2,
    .max_gram = 2,
    .preserve_original = false,
    .stream_bytes_type = irs::analysis::NGramTokenizerBase::InputType::UTF8,
  };
  static catalog::Tokenizer gNgramTokenizer(
    ObjectId{0}, ObjectId{12347}, "test_ngram", {}, DEFAULT_ROW_GROUP_SIZE,
    irs::analysis::TokenizerConfig{.config = std::move(ngram_opts)});
  auto tokenizer = gNgramTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {.analyzer = *std::move(tokenizer),
          .features = irs::IndexFeatures::Pos | irs::IndexFeatures::Freq};
}

[[maybe_unused]] catalog::ColumnTokenizer WildcardAnalyzerProvider(uint64_t) {
  irs::analysis::WildcardAnalyzer::Options wildcard_opts{
    .base_analyzer = std::make_unique<irs::analysis::TokenizerConfig>(
      irs::analysis::TokenizerConfig{
        .config = irs::StringTokenizer::Options{}}),
    .ngram_size = 3,
  };
  static catalog::Tokenizer gWildcardTokenizer(
    ObjectId{0}, ObjectId{12348}, "test_wildcard", {}, DEFAULT_ROW_GROUP_SIZE,
    irs::analysis::TokenizerConfig{.config = std::move(wildcard_opts)});
  auto tokenizer = gWildcardTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {
    .analyzer = *std::move(tokenizer),
    .features = irs::IndexFeatures::Pos | irs::IndexFeatures::Freq,
    .tokenizer_column = catalog::Column::Id{catalog::Column::kMaxRealIdValue},
  };
}

[[maybe_unused]] catalog::ColumnTokenizer GeoJsonAnalyzerProvider(uint64_t) {
  static catalog::Tokenizer gGeoTokenizer(
    ObjectId{0}, ObjectId{12349}, "test_geojson", {}, DEFAULT_ROW_GROUP_SIZE,
    irs::analysis::TokenizerConfig{
      .config = irs::analysis::GeoJsonAnalyzer::Options{}});
  auto tokenizer = gGeoTokenizer.GetTokenizer();
  EXPECT_TRUE(tokenizer);
  return {
    .analyzer = *std::move(tokenizer),
    .features = irs::IndexFeatures::None,
    .tokenizer_column = catalog::Column::Id{catalog::Column::kMaxRealIdValue},
  };
}

// Expected-filter builders (ported from velox test suite).
// Type dispatch is now by duckdb::LogicalType / native C++ type.
template<typename T>
std::string MakeFieldName(uint64_t column_id) {
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
irs::ByTerm& AddTermFilter(Filter& root, uint64_t column, const T& value) {
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
irs::FilterWithBoost& AddRangeFilter(Filter& root, uint64_t column,
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
irs::ByTerm& AddNullFilter(Filter& root, uint64_t column) {
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
irs::ByWildcard& AddLikeFilter(Filter& root, uint64_t column,
                               std::string_view value) {
  auto& wc = AddFilter<irs::ByWildcard>(root);
  *wc.mutable_field() = MakeFieldName<std::string_view>(column);
  wc.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(value));
  return wc;
}

template<typename Filter>
irs::ByPrefix& AddPrefixFilter(Filter& root, uint64_t column,
                               std::string_view value) {
  auto& pf = AddFilter<irs::ByPrefix>(root);
  *pf.mutable_field() = MakeFieldName<std::string_view>(column);
  pf.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(value));
  return pf;
}

template<typename Filter>
irs::ByRegexp& AddRegexpFilter(
  Filter& root, uint64_t column, std::string_view pattern,
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
  Filter& root, uint64_t column, std::vector<std::string_view> ngrams,
  float threshold = 0.7f) {
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
irs::ByEditDistance& AddEditDistanceFilter(Filter& root, uint64_t column,
                                           std::string_view term,
                                           uint8_t max_distance,
                                           bool with_transpositions = true,
                                           size_t max_terms = 1024,
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
irs::ByPhrase& AddPhraseFilter(Filter& root, uint64_t column,
                               std::vector<std::string_view> values) {
  auto& wc = AddFilter<irs::ByPhrase>(root);
  *wc.mutable_field() = MakeFieldName<std::string_view>(column);
  for (auto value : values) {
    wc.mutable_options()->template push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(value);
  }
  return wc;
}

// Build the same S2Point that production's ParseShape produces for a
// GeoJSON Point at (lng, lat). GeoJSON coordinate order is [lng, lat], so
// the expected origin in tests is constructed via S2LatLng::FromDegrees.
S2Point GeoPointFromDegrees(double lat, double lng) {
  return S2LatLng::FromDegrees(lat, lng).Normalized().ToPoint();
}

// GeoDistanceFilterOptions::operator== compares only `origin` and `range`
// (the analyzer-derived prefix / indexer options / stored / coding fields are
// not part of the equality contract), so the expected filter only needs to
// set those two -- exactly what FromGeoInRange / FromGeoDistanceComparison
// populate from user inputs.
template<typename Filter>
irs::GeoDistanceFilter& AddGeoDistanceFilter(Filter& root, uint64_t column,
                                             const S2Point& origin,
                                             std::optional<double> min_distance,
                                             bool min_inclusive,
                                             std::optional<double> max_distance,
                                             bool max_inclusive) {
  auto& geo = AddFilter<irs::GeoDistanceFilter>(root);
  *geo.mutable_field() = MakeFieldName<std::string_view>(column);
  auto* options = geo.mutable_options();
  options->origin = origin;
  if (min_distance.has_value()) {
    options->range.min = *min_distance;
    options->range.min_type =
      min_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  if (max_distance.has_value()) {
    options->range.max = *max_distance;
    options->range.max_type =
      max_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  // FromGeoDistanceComparison stamps the tokenizer column id onto the
  // filter; mirror that here so operator== matches.
  auto column_analyzer = GeoJsonAnalyzerProvider(column);
  SDB_ASSERT(column_analyzer.tokenizer_column);
  options->store_field_id = *column_analyzer.tokenizer_column;
  return geo;
}

// GeoFilterOptions::operator== compares only `type` and `shape`; other
// analyzer-derived fields (prefix, indexer options, stored, coding on the
// base) are ignored. We construct a Point ShapeContainer to match what the
// filter builder produces from a GeoJSON Point literal -- ParseShape walks
// the same `reset(S2Point, ...)` path, and ShapeContainer::equals checks
// type + S2 contents (coding compared via IsSameLoss, where Invalid and
// any non-U32 coding compare equal).
template<typename Filter>
irs::GeoFilter& AddGeoFilter(Filter& root, uint64_t column,
                             const S2Point& shape_point,
                             irs::GeoFilterType type) {
  auto& gf = AddFilter<irs::GeoFilter>(root);
  *gf.mutable_field() = MakeFieldName<std::string_view>(column);
  auto* options = gf.mutable_options();
  options->type = type;
  options->shape.reset(shape_point);
  // FromGeoInRange stamps the tokenizer column id onto the filter;
  // mirror that here so operator== matches.
  auto column_analyzer = GeoJsonAnalyzerProvider(column);
  SDB_ASSERT(column_analyzer.tokenizer_column);
  options->store_field_id = *column_analyzer.tokenizer_column;
  return gf;
}

template<typename Filter>
irs::ByWildcardNgram& AddWildcardNgramFilter(Filter& root, uint64_t column,
                                             std::string_view pattern,
                                             bool has_positions) {
  auto column_analyzer = WildcardAnalyzerProvider(column);
  auto& wf = AddFilter<irs::ByWildcardNgram>(root);
  *wf.mutable_field() = MakeFieldName<std::string_view>(column);
  auto* opts = wf.mutable_options();
  *opts = {pattern,
           basics::downCast<irs::analysis::WildcardAnalyzer>(
             *column_analyzer.analyzer.get()),
           has_positions};
  SDB_ASSERT(column_analyzer.tokenizer_column);
  opts->store_field_id = *column_analyzer.tokenizer_column;
  return wf;
}

template<typename T, typename Filter>
irs::ByTerms& AddTermsFilter(Filter& root, uint64_t column,
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
    irs::formats::Init();
    irs::compression::Init();
  }

  void SetUp() final {
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
        .field_id = static_cast<irs::field_id>(columns[phys].id),
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
        // Pass the test connection's ClientContext through so the
        // filter builder's named-analyzer resolver runs with a real
        // context (the resolver returns nullptr for unknown names,
        // surfacing the "tokenizer not found in catalog" error).
        sdb::connector::SearchFilterOptions opts{.client_context =
                                                   *_conn.context};
        auto result =
          sdb::connector::MakeSearchFilter(root, single, getter, opts);
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
    // Non-keyword analyzer rejects plain a = 'foo' on a VARCHAR column.
    // Use the TSQUERY surface (`b @@ 'foo'`) for analyzed columns.
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
    irs::And expected;
    AssertFilter(expected, "SELECT * FROM foo WHERE b = 'foo'", columns, false,
                 SegmentationAnalyzerProvider);
  }
  {
    // TSQUERY-surface equality is accepted on a segmenting analyzer:
    // `b @@ 'foo'` tokenises through the column analyzer and emits
    // ByTerm for the single resulting token.
    std::vector<ColumnSpec> columns{
      {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
    irs::And expected;
    AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
    AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'foo'", columns, true,
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

TEST_F(SearchFilterBuilderTest, test_TSQLikeEscape) {
  // TSQUERY-surface analogue: ts_like('\%!foo_') passes the literal
  // pattern straight through to iresearch -- no SQL ESCAPE re-mapping
  // because the function form takes a raw wildcard pattern.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "required_field"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "\\%!foo_");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE required_field @@ ts_like('\\%!foo_')",
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
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick brown fox')", columns,
    true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_SimplePhraseNoFeatures) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick brown fox')", columns,
    false, SegmentationAnalyzerProviderBase<irs::IndexFeatures::Freq>,
    "Positions and Frequency");
}

TEST_F(SearchFilterBuilderTest, test_SimpleAndPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AddPhraseFilter(expected, 1, {"quick", "lazy", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick brown fox') "
    "AND category @@ ts_phrase('quick lazy fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_SimpleOrPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddPhraseFilter(or_filter, 1, {"quick", "brown", "fox"});
  AddPhraseFilter(or_filter, 1, {"quick", "lazy", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick brown fox') OR "
    "category @@ ts_phrase('quick lazy fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseExactGap) {
  // ts_phrase(field, 'quick', 2, 'fox') -- exactly 2 words between 'quick' and
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
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick', 2, 'fox')", columns,
    true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseRangeGap) {
  // ts_phrase(field, 'quick', ARRAY[1,2], 'fox') -- 1 to 2 words between
  // 'quick' and 'fox'
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
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick', ARRAY[1,2], 'fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMultipleGaps) {
  // ts_phrase(field, 'quick', 1, 'brown', 2, 'fox') -- multiple gaps
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
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ ts_phrase('quick', 1, "
               "'brown', 2, 'fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseGapBetweenMultiTokenPatterns) {
  // ts_phrase(field, 'quick brown', 2, 'lazy fox') -- multi-token patterns with
  // a gap: 'quick' adj 'brown', then gap=2, then 'lazy' adj 'fox'
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
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ ts_phrase('quick brown', "
               "2, 'lazy fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseGapTrailingError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(
    irs::And{}, "SELECT * FROM foo WHERE category @@ ts_phrase('quick', 2)",
    columns, false, SegmentationAnalyzerProvider, "ts_phrase ends with a gap");
}

TEST_F(SearchFilterBuilderTest, test_PhraseConsecutiveGapsError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(
    irs::And{},
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick', 1, 2, 'fox')",
    columns, false, SegmentationAnalyzerProvider,
    "ts_phrase has consecutive gaps at argument 2");
}

TEST_F(SearchFilterBuilderTest, test_PhraseGapRangeMinExceedsMaxError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(
    irs::And{},
    "SELECT * FROM foo WHERE category @@ ts_phrase('quick', ARRAY[3,1], 'fox')",
    columns, false, SegmentationAnalyzerProvider,
    "ts_phrase interval gap must satisfy 0 <= min <= max, got [3, 1]");
}

TEST_F(SearchFilterBuilderTest, test_PhraseGap_DateTimestampRejected) {
  // DATE / TIMESTAMP cast to BIGINT in DuckDB (days / microseconds),
  // but a date literal as a phrase-gap offset is nonsense. Reject at
  // parse time.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE category @@ ts_phrase('quick', "
               "DATE '2024-01-01', 'fox')",
               columns, false, SegmentationAnalyzerProvider,
               "ts_phrase gap must be a non-null non-negative integer");
}

TEST_F(SearchFilterBuilderTest, test_PhraseGap_BroadNumericTypes) {
  // The gap parser accepts any numeric type whose value coerces to a
  // non-negative int64. This covers UBIGINT, HUGEINT, and DOUBLE/
  // DECIMAL with no fractional part.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  for (const auto* gap_expr : {
         "CAST(2 AS UBIGINT)",
         "CAST(2 AS HUGEINT)",
         "CAST(2.0 AS DOUBLE)",
         "CAST(2.0 AS DECIMAL(5,2))",
       }) {
    irs::And expected;
    auto& phrase = AddFilter<irs::ByPhrase>(expected);
    *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
    phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
    phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
    AssertFilter(expected,
                 "SELECT * FROM foo WHERE category @@ ts_phrase('quick', " +
                   std::string{gap_expr} + ", 'fox')",
                 columns, true, SegmentationAnalyzerProvider);
  }
}

// ===========================================================================
// Term equality / range / IN / LIKE -- migrated from the legacy
// TERM_LT / TERM_LIKE / TERM_IN / BOOST functions to the
// native SQL operators (= / < / <= / > / >= / IN / LIKE) on identity-
// analyzed columns and the TSQUERY surface (`@@ 'val'`,
// `@@ ts_like(...)`, `@@ ts_any([...])`) on analyzed columns.
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
  // LESS / LESS_EQUAL / GREATER / GREATER_EQUAL on the TSQUERY surface
  // tokenise their VARCHAR argument via the ambient analyzer
  // (segmenting -> 'Foo' becomes 'foo') and use the resulting single
  // token as the range bound.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_lt('Foo')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermGreater_Segmentation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"foo"}, false,
                                   std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_gt('foo')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLessEq_Segmentation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, true);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_le('foo')", columns,
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_ge('fOo')", columns,
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
               "SELECT * FROM foo WHERE a @@ (ts_ge('apple') && "
               "ts_le('orange'))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermGe_AndTermLe_Range_SqlAnd) {
  // Two separate `@@` predicates AND'd at the SQL level (rather than
  // composed inside a single @@ via TSQUERY `&&`) produce two
  // ByRange filters at the top-level And.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"apple"}, true,
                                   std::nullopt, false);
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"orange"}, true);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE a @@ ts_ge('apple') AND a @@ ts_le('orange')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLess_IntegerColumn) {
  // LESS on a numeric column emits irs::ByGranularRange (same shape
  // as RANGE's numeric path).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_lt(100)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermGreaterEq_IntegerColumn) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_ge(50)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermLessEq_BooleanColumn) {
  // LESS_EQUAL on a BOOLEAN column emits irs::ByRange via BooleanTokenizer.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
  irs::And expected;
  auto& range = expected.add<irs::ByRange>();
  *range.mutable_field() = MakeFieldName<bool>(1);
  auto& opts = range.mutable_options()->range;
  opts.max.assign(
    irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(true)));
  opts.max_type = irs::BoundType::Inclusive;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_le(true)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TermLess_TypeMismatchVarcharVsInt) {
  // VARCHAR bound on an INTEGER column -> bind-time error, same shape
  // as RANGE's mismatch error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({}, "SELECT * FROM foo WHERE b @@ ts_lt('foo')", columns, false,
               IdentityAnalyzerProvider, "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest, test_TermLess_NullBoundRejected) {
  // NULL bound is a bind-time error -- use ts_between(NULL, ...) for
  // unbounded semantics.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({}, "SELECT * FROM foo WHERE b @@ ts_lt(NULL)", columns, false,
               IdentityAnalyzerProvider, "bound must be non-null");
}

TEST_F(SearchFilterBuilderTest, test_TermLike_Segmentation) {
  // LIKE on the TSQUERY surface does NOT tokenise the pattern --
  // it's a raw wildcard match against indexed terms.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foO_");
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (ts_like('%foO_'))",
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
  // constructor on a non-keyword column. LIKE on the TSQUERY
  // surface does NOT tokenise; the pattern is matched as wildcard
  // bytes against indexed terms exactly like the identity case.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "\\%!foo_");
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (ts_like('\\%!foo_'))",
               columns, true, SegmentationAnalyzerProvider);
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (ts_like('%foo_'))",
               columns, true, WildcardAnalyzerProvider);
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
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT(a LIKE '%foo_')", columns,
               true, WildcardAnalyzerProvider);
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
               "SELECT * FROM foo WHERE b @@ ts_any(['foo', 'bar', 'baZ'])",
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b IN ('foo', 'bAr', 'baz')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TermEq_WithAnd) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AddRangeFilter<int32_t>(expected, 2, 10, true, std::nullopt, false);
  AssertFilter(expected, "SELECT * FROM foo WHERE a @@ 'foo' AND b >= 10",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermEq_WithOr) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"foo"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"bar"});
  AssertFilter(expected, "SELECT * FROM foo WHERE a @@ 'foo' OR a @@ 'bar'",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TermLike_WithNot) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddLikeFilter(not_filter, 1, "%foo_");
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT(a @@ (ts_like('%foo_')))",
               columns, true, SegmentationAnalyzerProvider);
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
               "SELECT * FROM foo WHERE a @@ ts_any(['x', 'y']) AND b >= 10",
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_ngram('hello')",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramWithThreshold) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"}, 0.5f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_ngram('hello', 0.5)",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramNoFeatures) {
  // Default keyword analyzer doesn't have Pos+Freq features required
  // for NGRAM -- bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_ngram('hello')",
               columns, false, IdentityAnalyzerProvider, "ts_ngram");
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
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 2, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinDistanceTooHigh) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 5)",
               columns, false, IdentityAnalyzerProvider, "ts_levenshtein");
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_LevenshteinTranspositionDistanceTooHigh) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 4, true)",
               columns, false, IdentityAnalyzerProvider, "ts_levenshtein");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinNotNegation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& negated = expected.add<irs::Not>();
  AddEditDistanceFilter(negated, 1, "test", 2);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ !!ts_levenshtein('test', 2)",
               columns, true);
}

// LEVENSHTEIN with the optional `prefix` 4th argument: only the suffix
// after `prefix` participates in edit-distance matching.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinWithPrefix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "roximate", 1,
                        /*with_transpositions=*/true,
                        /*max_terms=*/1024, /*prefix=*/"app");
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_levenshtein('roximate', 1, true, 'app')",
    columns, true);
}

// 4-arg form with empty prefix should behave like the 3-arg form.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinEmptyPrefix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2, /*with_transpositions=*/false);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 2, false, '')",
    columns, true);
}

// 5+ arguments must be rejected with the expected hint.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinTooManyArgs) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  // DuckDB rejects the call at bind time because there is no
  // (VARCHAR, INTEGER, BOOLEAN, VARCHAR, VARCHAR) overload.
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 1, true, "
               "'p', 'extra')",
               columns, false, IdentityAnalyzerProvider, "ts_levenshtein");
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY ^ 2.0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Phrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"}).boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category @@ "
               "ts_phrase('quick brown fox') ^ 1.5",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Like) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "foo%").boost(3.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (ts_like('foo%')) ^ 3.0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_WildcardFilter) {
  // Boost on a TSQUERY-surface LIKE against a WildcardAnalyzer column
  // dispatches to ByWildcardNgram and threads the boost through.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddWildcardNgramFilter(expected, 1, "foo%", true).boost(3.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (ts_like('foo%')) ^ 3.0",
               columns, true, WildcardAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_NgramBoost) {
  // ts_ngram(...) ^ N -- TSQUERY-surface boost on n-gram similarity.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"fo", "oo"}).boost(2.5f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_ngram('foo') ^ 2.5",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinBoost) {
  // ts_levenshtein(...) ^ N -- TSQUERY-surface boost on Levenshtein.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2).boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 2) ^ 1.5",
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
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(['foo', 'bar']) ^ 2.0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_RangeComparison) {
  // `^` on an LT/LE/GT/GE TSQUERY value boosts the resulting ByRange.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, false)
    .boost(1.5f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_lt('foo') ^ 1.5",
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY ^ 0.0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_Negative) {
  // Negative boost factor is rejected at bind time.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY ^ -1.0",
               columns, false, IdentityAnalyzerProvider, "boost");
}

// `::boost(K)` parameterised-type cast -- composable analogue of `^ K`.
// Uses the BOOSTED_TSQUERY alias so the cast wrapper survives in the
// bound tree (different alias from source) and the walker can read
// the factor.

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastSimple) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"}).boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_phrase('quick brown fox')::boost(1.5)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastAndGroup) {
  // `(a && b)::boost(K)` -- both legs get boost K via the conjunction's
  // own boost slot (mirrors `^` on a `&&` group).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& group = expected.add<irs::And>();
  group.boost(3.0f);
  AddTermFilter<std::string_view>(group, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(group, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "('x'::TSQUERY && 'y'::TSQUERY)::boost(3.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastNestedWithCaret) {
  // `(expr ^ 2)::boost(3)` -- multiplicative compose: 6x.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(6.0f);
  AssertFilter(
    expected, "SELECT * FROM foo WHERE b @@ ('foo'::TSQUERY ^ 2.0)::boost(3.0)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastCaretOnTop) {
  // `expr::boost(2) ^ 3` -- symmetric, also 6x.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(6.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY::boost(2.0) ^ 3.0",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastTokenizeThenBoost) {
  // ts_phrase(...)::tokenize('keyword')::boost(42) -- inner tokenize
  // forces raw-bytes phrase parts; outer boost multiplies.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.boost(42.0f);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"quick fox"}));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_phrase('quick fox')::tokenize('keyword')::boost(42.0)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastBoostThenTokenize) {
  // ts_phrase(...)::boost(42)::tokenize('keyword') -- symmetric ordering;
  // both effects must apply (tokenize override + boost 42).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.boost(42.0f);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"quick fox"}));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_phrase('quick fox')::boost(42.0)::tokenize('keyword')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastZero) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(0.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY::boost(0.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastNegative) {
  // Negative boost factor is rejected at bind time (mirrors `^ -K`).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'foo'::TSQUERY::boost(-1.0)",
               columns, false);
}

// Boost on geo predicates: the legacy `BOOST(predicate, factor)` function was
// removed in favour of the TSQUERY-surface `^` operator (TSQUERY-only) and the
// `(predicate)::boost(K)` cast (BOOLEAN-predicate-friendly via
// TryDispatchSqlBoostCast). Geo functions return BOOLEAN, so the cast form
// applies. The filter builder peels the BOOSTED_TSQUERY->BOOLEAN coercion
// DuckDB inserts at the WHERE root, reads the boost factor from the cast's
// modifier, and dispatches the inner expression with `ctx.boost = K`.

TEST_F(SearchFilterBuilderTest, test_Boost_GeoInRange) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       500.0, true)
    .boost(2.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0))"
               "::boost(2.5)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Boost_GeoDistance) {
  // ST_Distance_Centroid returns DOUBLE so the cast wraps the comparison
  // expression rather than the function call itself; the filter builder
  // rewrites `ST_Distance_Centroid(...) < d` into a one-sided
  // GeoDistanceFilter range and applies the boost from the surrounding
  // ::boost(K).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), std::nullopt,
                       false, 100.0, false)
    .boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') < 100.0)"
               "::boost(1.5)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Boost_GeoIntersects) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoFilter(expected, 1, GeoPointFromDegrees(20, 10),
               irs::GeoFilterType::Intersects)
    .boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (ST_Intersects(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}'))::boost(2.0)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Boost_GeoContains) {
  // ST_Contains(field, shape) -> IsContained (filter shape is contained
  // within indexed data).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoFilter(expected, 1, GeoPointFromDegrees(20, 10),
               irs::GeoFilterType::IsContained)
    .boost(3.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (ST_Contains(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}'))::boost(3.0)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Boost_GeoContains_SwappedArgs) {
  // ST_Contains(shape, field) -> Contains (filter shape contains indexed
  // data); the boost should still propagate.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoFilter(expected, 1, GeoPointFromDegrees(20, 10),
               irs::GeoFilterType::Contains)
    .boost(0.75f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (ST_Contains("
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', g))"
               "::boost(0.75)",
               columns, true, GeoJsonAnalyzerProvider);
}

// ===========================================================================
// ST_Distance_Between
//
// All tests use the canonical (lat=20, lng=10) Point centroid -- expressed in
// SQL as the GeoJSON literal '{"type":"Point","coordinates":[10,20]}' (GeoJSON
// orders coordinates [lng, lat]) and reconstructed in the expected filter via
// S2LatLng::FromDegrees(20, 10).
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_GeoInRange_Basic) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       500.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_GeometryField) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::GEOMETRY(), .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       500.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_MinZeroLeavesUnbounded) {
  // FromGeoInRange treats min_distance == 0.0 as the absent lower bound: it
  // skips assigning range.min so the bound stays Unbounded (and `min` stays
  // at its default 0.0).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), std::nullopt,
                       false, 500.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 0.0, 500.0)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_ExclusiveBounds) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, false,
                       500.0, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0, "
               "false, false)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_FiveArgsExclusiveMin) {
  // 5-arg form: only include_min explicit; include_max defaults to true.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, false,
                       500.0, true);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE ST_Distance_Between(g, "
    "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0, false)",
    columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_NotNegation) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  auto& negated = expected.add<irs::Not>();
  AddGeoDistanceFilter(negated, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       500.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0)",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_NonConstantCentroid) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "c"}};
  irs::And expected;
  AssertFilter(
    expected, "SELECT * FROM foo WHERE ST_Distance_Between(g, c, 100.0, 500.0)",
    columns, false, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_WrongAnalyzer) {
  // Field exists but its analyzer is not a geo analyzer -- SetupGeoFilter
  // rejects it.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Between(g, "
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}', 100.0, 500.0)",
               columns, false, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoInRange_InvalidGeoJsonCentroid) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Between(g, "
               "'not a geojson', 100.0, 500.0)",
               columns, false, GeoJsonAnalyzerProvider);
}

// ===========================================================================
// ST_Distance_Centroid (rewritten by FromBinaryEq / FromComparison into
// GeoDistanceFilter range bounds when used as
// `ST_Distance_Centroid(...) OP <const>`)
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_GeoDistance_Eq) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       100.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') = 100.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_NotEq) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  auto& negated = expected.add<irs::Not>();
  AddGeoDistanceFilter(negated, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       100.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') != 100.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_Lt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), std::nullopt,
                       false, 100.0, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') < 100.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_Le) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), std::nullopt,
                       false, 100.0, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') <= 100.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_Gt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, false,
                       std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') > 100.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_Ge) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), 100.0, true,
                       std::nullopt, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') >= 100.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_GeometryField) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::GEOMETRY(), .name = "g"}};
  irs::And expected;
  AddGeoDistanceFilter(expected, 1, GeoPointFromDegrees(20, 10), std::nullopt,
                       false, 500.0, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') < 500.0",
               columns, true, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_NonConstantDistance) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"},
    {.id = 2, .type = duckdb::LogicalType::DOUBLE, .name = "d"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') < d",
               columns, false, GeoJsonAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_GeoDistance_WrongAnalyzer) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "g"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ST_Distance_Centroid(g,"
               "'{\"type\":\"Point\",\"coordinates\":[10,20]}') < 100.0",
               columns, false, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastLevenshtein) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2).boost(2.5f);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_levenshtein('test', 2)::boost(2.5)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastNgram) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"fo", "oo"}).boost(2.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_ngram('foo')::boost(2.5)",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastLike) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "foo%").boost(3.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ (ts_like('foo%'))::boost(3.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostCastRange) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"foo"}, false)
    .boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_lt('foo')::boost(1.5)", columns,
               true);
}

// `(predicate)::boost(K)` -- boost a SQL predicate outside `@@`.
// The cast peels through both the WHERE-coercion (BOOSTED -> BOOLEAN)
// and the user-written boost cast (BOOLEAN -> BOOSTED), then
// recurses into the SQL-surface dispatch with WithBoost(K).

TEST_F(SearchFilterBuilderTest, test_Boost_SqlComparison) {
  // (b > 50)::boost(2.0) -- numeric range gets boost 2.0.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, false, std::nullopt, false)
    .boost(2.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE (b > 50)::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlEquality) {
  // (b = 'foo')::boost(0.5) -- term filter gets boost 0.5.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"})
    .boost(0.5f);
  AssertFilter(expected, "SELECT * FROM foo WHERE (b = 'foo')::boost(0.5)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlBetween) {
  // (b BETWEEN 1 AND 10)::boost(2.0) -- BETWEEN's And group gets boost 2.0.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  auto& group = expected.add<irs::And>();
  group.boost(2.0f);
  AddRangeFilter<int32_t>(group, 1, 1, true, std::nullopt, false);
  AddRangeFilter<int32_t>(group, 1, std::nullopt, false, 10, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (b BETWEEN 1 AND 10)::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlIn) {
  // (a IN ('x','y'))::boost(2.0) -- ByTerms gets boost 2.0.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"x"}, std::string_view{"y"}})
    .boost(2.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE (a IN ('x','y'))::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlLike) {
  // (col LIKE '%foo_')::boost(2.0) -- the underlying `~~` operator
  // path; the wildcard pattern is preserved as-is.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "col"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo_").boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (col LIKE '%foo_')::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlLikePrefix) {
  // (col LIKE 'pre%')::boost(2.0) -- DuckDB rewrites this to
  // `prefix(col, 'pre')`. On keyword-analyzed columns we emit the
  // dedicated irs::ByPrefix filter (cheaper than a wildcard pattern
  // scan).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "col"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, std::string_view{"pre"}).boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (col LIKE 'pre%')::boost(2.0)", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_SqlPrefixUnboosted) {
  // Bare `col LIKE 'pre%'` (no boost) -- claim the rewritten
  // `prefix(col, 'pre')` and emit irs::ByPrefix directly.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "col"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, std::string_view{"pre"});
  AssertFilter(expected, "SELECT * FROM foo WHERE col LIKE 'pre%'", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlEqualityInOr) {
  // Boosted equality inside an OR group -- verifies that the boost
  // peel propagates the factor through MakeGroup<irs::Or>'s child
  // dispatch and onto the per-leg ByTerm filter.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "sku"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"alpha-1"});
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"beta-1"})
    .boost(1000.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE sku = 'alpha-1' OR (sku = "
               "'beta-1')::boost(1000.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlPrefixInOr) {
  // Boosted LIKE 'pre%' (rewritten by DuckDB to prefix(...)) inside
  // an OR group -- verifies boost propagates onto the ByPrefix leg.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "sku"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddPrefixFilter(or_group, 1, std::string_view{"alpha-"});
  AddPrefixFilter(or_group, 1, std::string_view{"beta-"}).boost(1000.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE sku LIKE 'alpha-%' OR (sku LIKE "
               "'beta-%')::boost(1000.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlAndCombined) {
  // Mix TSQUERY-surface boost with SQL-surface boost in one query.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "a"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"q"});
  AddRangeFilter<int32_t>(expected, 2, 50, false, std::nullopt, false)
    .boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a @@ 'q' AND (b > 50)::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlNegated) {
  // NOT (b > 50)::boost(2.0) -- boost survives negation. DuckDB's
  // expression simplifier folds NOT (b > 50) -> b <= 50 before the
  // optimizer extension runs, so we observe the rewritten range
  // (with boost still applied).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 50, true)
    .boost(2.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (b > 50)::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlUnclaimable_BindError) {
  // (unindexed_col > 50)::boost(2.0) -- unindexed column; the boost
  // peel can't claim and escalates to a specific error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "indexed_col"}};
  AssertFilter(
    irs::And{}, "SELECT * FROM foo WHERE (unindexed_col > 50)::boost(2.0)",
    columns, false, IdentityAnalyzerProvider,
    "::boost(K) used on a predicate the inverted index could not claim");
}

TEST_F(SearchFilterBuilderTest, test_Boost_SqlUnclaimable_NonConstRhs) {
  // (b > c)::boost(2.0) -- non-constant RHS isn't claimable; same
  // bind-time error as the unindexed-column case.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "c"}};
  AssertFilter(
    irs::And{}, "SELECT * FROM foo WHERE (b > c)::boost(2.0)", columns, false,
    IdentityAnalyzerProvider,
    "::boost(K) used on a predicate the inverted index could not claim");
}

// Trivial BOOLEAN constants short-circuit at any TSQUERY position.
// NULL is handled by DuckDB's NULL-strict operator semantics (folds
// the whole predicate to NULL -> no rows), not by our walker.

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialFalse) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::Empty>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ false", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialTrue) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::All>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ true", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialOrAll) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"x"});
  or_group.add<irs::All>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ('x'::TSQUERY || true)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialOrFalse) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"x"});
  or_group.add<irs::Empty>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ('x'::TSQUERY || false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialAndFalse) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"x"});
  and_group.add<irs::Empty>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ('x'::TSQUERY && false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialAndTrue) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"x"});
  and_group.add<irs::All>();
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ('x'::TSQUERY && true)",
               columns, true);
}

// Bool on the LHS of a binary combinator -- commutative shape.

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialBoolOrTsquery) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  or_group.add<irs::All>();
  AddTermFilter<std::string_view>(or_group, 1, std::string_view{"x"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (true || 'x'::TSQUERY)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialBoolAndTsquery) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  and_group.add<irs::Empty>();
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"x"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ (false && 'x'::TSQUERY)",
               columns, true);
}

// Nested compounds with bool legs.

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialOrAndCompound) {
  // (true || 'x') && 'y' -- the inner Or has the bool All leg, the
  // outer And combines that with another term.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_group = expected.add<irs::And>();
  auto& inner_or = and_group.add<irs::Or>();
  inner_or.add<irs::All>();
  AddTermFilter<std::string_view>(inner_or, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(and_group, 1, std::string_view{"y"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ((true || 'x'::TSQUERY) && 'y'::TSQUERY)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TrivialBothBoolsOr) {
  // true || false -- both legs trivial; walker emits Or{All, Empty}.
  // (DuckDB doesn't pre-fold this because the operator is our stub.)
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_group = expected.add<irs::Or>();
  or_group.add<irs::All>();
  or_group.add<irs::Empty>();
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ (true::TSQUERY || false::TSQUERY)",
               columns, true);
}

// `ts_compound(must, must_not, should [, min_should_match])` --
// Elasticsearch-style bool query.

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundMustOnly) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound(['x'::TSQUERY, 'y'::TSQUERY], NULL, NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundMustSingleClause) {
  // Single TSQUERY without list wrapping.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound('x'::TSQUERY, NULL, NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundMustNotOnly) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  auto& not_filter = and_filter.add<irs::Not>();
  AddTermFilter<std::string_view>(not_filter, 1, std::string_view{"x"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound(NULL, 'x'::TSQUERY, NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundShouldOnly) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  auto& or_filter = and_filter.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound(NULL, NULL, ['x'::TSQUERY, 'y'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundShouldMin2) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  auto& or_filter = and_filter.add<irs::Or>();
  or_filter.min_match_count(2);
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"c"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound(NULL, NULL, "
               "['a'::TSQUERY, 'b'::TSQUERY, 'c'::TSQUERY], 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundShouldMinOutOfRange) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound(NULL, NULL, ['a'::TSQUERY, 'b'::TSQUERY], 5)",
               columns, false, IdentityAnalyzerProvider,
               "min_should_match must be in");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundShouldMinNoShould) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_compound('x'::TSQUERY, NULL, NULL, 2)",
               columns, false, IdentityAnalyzerProvider,
               "min_should_match makes no sense");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundAllEmpty) {
  // All buckets empty -> Empty filter (matches no rows). Mirrors the
  // SQL NULL semantics of `b @@ NULL` -- no error, just zero rows.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::Empty>();
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound(NULL, NULL, NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundFull) {
  // All three buckets populated: must=[x, y], must_not=z, should=[a, b].
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"y"});
  auto& not_filter = and_filter.add<irs::Not>();
  AddTermFilter<std::string_view>(not_filter, 1, std::string_view{"z"});
  auto& or_filter = and_filter.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound("
               "['x'::TSQUERY, 'y'::TSQUERY], "
               "'z'::TSQUERY, "
               "['a'::TSQUERY, 'b'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundCommutative) {
  // ts_compound(...) on the LHS of @@.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE ts_compound('x'::TSQUERY, NULL, NULL) @@ b",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundBoostCast) {
  // ts_compound(...)::boost(K) puts boost K on the top And.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  and_filter.boost(2.5f);
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound("
               "['x'::TSQUERY, 'y'::TSQUERY], NULL, NULL)::boost(2.5)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundTokenizeCast) {
  // ts_compound(ts_phrase('q f'))::tokenize('keyword') -- tokenizer
  // override applies to every clause's recursion. Phrase emits one raw-bytes
  // part.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  auto& phrase = and_filter.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"quick fox"}));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ "
    "ts_compound(ts_phrase('quick fox'), NULL, NULL)::tokenize('keyword')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundPerClauseTokenize) {
  // Per-clause modifier cast applies only to that clause's subtree.
  // must = ts_phrase('q f')::tokenize('keyword') -> raw-bytes phrase part.
  // must_not = ts_phrase('z') -> uses the column analyzer (segmentation).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  // must clause -- identity-tokenized phrase.
  auto& phrase_must = and_filter.add<irs::ByPhrase>();
  *phrase_must.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase_must.mutable_options()->push_back<irs::ByTermOptions>().term.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"quick fox"}));
  // must_not clause -- segmentation analyzer.
  auto& not_filter = and_filter.add<irs::Not>();
  AddPhraseFilter(not_filter, 1, {"z"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound("
               "ts_phrase('quick fox')::tokenize('keyword'), "
               "ts_phrase('z'), NULL)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_CompoundArbitraryClauseShapes) {
  // must = ts_phrase('a') || 'b' (combinator), must_not = ts_lt('z') (range
  // constructor), should = ['x', 'y'] (bare-string list auto-lifted).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  // must: Or { ts_phrase('a'), term('b') }.
  auto& or_must = and_filter.add<irs::Or>();
  AddPhraseFilter(or_must, 1, {"a"});
  AddTermFilter<std::string_view>(or_must, 1, std::string_view{"b"});
  // must_not: ByRange (string lower=open, upper=z exclusive).
  auto& not_filter = and_filter.add<irs::Not>();
  AddRangeFilter<std::string_view>(not_filter, 1, std::nullopt, false,
                                   std::string_view{"z"}, false);
  // should: Or { term('x'), term('y') }.
  auto& or_should = and_filter.add<irs::Or>();
  AddTermFilter<std::string_view>(or_should, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(or_should, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound("
               "ts_phrase('a') || 'b', ts_lt('z'), ['x', 'y'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundWithCaretBoost) {
  // Per-clause `^` boost inside the must-list. Works because `^`
  // returns TSQUERY (not TOK) so `[expr ^ K, expr]` is LIST(TSQUERY).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"})
    .boost(2.0f);
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound("
               "['x'::TSQUERY ^ 2.0, 'y'::TSQUERY], NULL, NULL)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CompoundNested) {
  // compound nested as a clause inside another compound.
  // outer must = ts_compound(must='a', must_not=NULL, should='b')
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& outer_and = expected.add<irs::And>();
  // outer's must: inner compound.
  auto& inner_and = outer_and.add<irs::And>();
  AddTermFilter<std::string_view>(inner_and, 1, std::string_view{"a"});
  auto& inner_or = inner_and.add<irs::Or>();
  AddTermFilter<std::string_view>(inner_or, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_compound("
               "ts_compound('a'::TSQUERY, NULL, 'b'::TSQUERY), "
               "NULL, NULL)",
               columns, true);
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
//  - ts_tokenize(text, analyzer) / ::tokenize(name) cast
//  - Bare-string tokenisation through column analyzer (currently raw ByTerm)
//  - ByTerms optimisation for ANY_OF / ALL_OF
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Phrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_phrase('quick brown fox')",
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
               "(ts_phrase('quick brown') || ts_phrase('lazy dog'))",
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
               "(ts_phrase('quick brown') && ts_phrase('lazy dog'))",
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ !!'spam'", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BoostOperator) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"})
    .boost(2.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'quick'::TSQUERY ^ 2.0",
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
  // ts_phrase(...) @@ b -- mirrors test_TSQueryMatch_Phrase.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ts_phrase('quick brown fox') @@ b",
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
               "(ts_phrase('quick brown') || ts_phrase('lazy dog')) @@ b",
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
               "(ts_phrase('quick brown') && ts_phrase('lazy dog')) @@ b",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsAnyOf) {
  // ts_any([..::TSQUERY]) @@ b -- list form on LHS, column on RHS.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE "
               "ts_any(['a'::TSQUERY, 'b'::TSQUERY]) @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsAnyOfList) {
  // ts_any([list]) @@ b -- bare-string list form on LHS.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected, "SELECT * FROM foo WHERE ts_any(['a', 'b']) @@ b",
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
  AssertFilter(expected, "SELECT * FROM foo WHERE ('a' ## 1 ## 'b') @@ b",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsBoost) {
  // (PHRASE ^ 2.0) @@ b -- mirrors test_TSQueryMatch_BoostOperator.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"})
    .boost(2.0f);
  AssertFilter(expected, "SELECT * FROM foo WHERE 'quick'::TSQUERY ^ 2.0 @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeLhsLike) {
  // ts_like(pattern) @@ b -- mirrors test_TSQueryMatch_LikeWildcard.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, std::string_view{"quic%"});
  AssertFilter(expected, "SELECT * FROM foo WHERE ts_like('quic%') @@ b",
               columns, true);
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
  // 'foo'::tokenize('keyword') @@ b -- mirrors
  // test_TSQueryMatch_TokenizerCastIdentity.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE 'quick fox'::tokenize('keyword') @@ b",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_CommutativeAmbiguousColumns) {
  // Both sides are indexed VARCHAR columns -- bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"},
    {.id = 2, .type = duckdb::LogicalType::VARCHAR, .name = "c"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ c", columns, false,
               IdentityAnalyzerProvider,
               "@@ has column references on both sides");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LikeWildcard) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, std::string_view{"quic%"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_like('quic%')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Prefix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, std::string_view{"qu"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_starts_with('qu')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Regexp) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"qu.*ck"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_regexp('qu.*ck')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpPerlExplicit) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"\\d+"},
                  irs::RegexpSyntax::Perl);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_regexp('\\d+', 'perl')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpPosix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, std::string_view{"[[:alpha:]]+"},
                  irs::RegexpSyntax::PosixEre);
  AssertFilter(
    expected, "SELECT * FROM foo WHERE b @@ ts_regexp('[[:alpha:]]+', 'posix')",
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
               "SELECT * FROM foo WHERE b @@ ts_regexp('abc', 'POSIX')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpUnknownSyntax) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({}, "SELECT * FROM foo WHERE b @@ ts_regexp('abc', 'pcre')",
               columns, false, IdentityAnalyzerProvider,
               "ts_regexp syntax must be one of");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpNonVarcharColumn) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({}, "SELECT * FROM foo WHERE b @@ ts_regexp('abc')", columns,
               false, IdentityAnalyzerProvider,
               "ts_regexp field is not VARCHAR");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RegexpUnderNot) {
  // Negated regexp: NOT ts_regexp('foo.*').
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& re = not_filter.filter<irs::ByRegexp>();
  *re.mutable_field() = MakeFieldName<std::string_view>(1);
  re.mutable_options()->pattern.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"foo.*"}));
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ !!ts_regexp('foo.*')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_Levenshtein) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, std::string_view{"quikc"}, 2, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_levenshtein('quikc', 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeVarchar) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"a"}, true,
                                   std::string_view{"f"}, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_between('a', 'f', true, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeVarcharOpenLeft) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::nullopt, false,
                                   std::string_view{"m"}, true);
  AssertFilter(
    expected, "SELECT * FROM foo WHERE b @@ ts_between(NULL, 'm', false, true)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeVarcharOpenRight) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRangeFilter<std::string_view>(expected, 1, std::string_view{"a"}, false,
                                   std::nullopt, false);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_between('a', NULL, false, false)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeInt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, true, 100, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_between(10, 100, true, false)",
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
    "SELECT * FROM foo WHERE b @@ ts_between(false, true, true, true)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeBothNullMatchesAll) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::All>();
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_between(NULL, NULL, false, false)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangePhrasePart) {
  // 'a' ## ts_between('b', 'd', true, true) -- RANGE as phrase part
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
  rng_opts.range.min.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"b"}));
  rng_opts.range.min_type = irs::BoundType::Inclusive;
  rng_opts.range.max.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{"d"}));
  rng_opts.range.max_type = irs::BoundType::Inclusive;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ts_between('b', 'd', true, true))",
               columns, true, SegmentationAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// RANGE negative cases. Mismatched min/max types and value-vs-column
// type mismatches are bind-time errors (throw InvalidInputException).
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedVarcharInt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(
    {}, "SELECT * FROM foo WHERE b @@ ts_between('a', 5, true, true)", columns,
    false, IdentityAnalyzerProvider, "ts_between bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedIntVarchar) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ ts_between(1, 'foo', true, true)",
               columns, false, IdentityAnalyzerProvider,
               "ts_between bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedBoolInt) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BOOLEAN, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ ts_between(false, 1, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "ts_between bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeMismatchedVarcharBool) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ ts_between('a', true, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "ts_between bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeIntBigintMismatch) {
  // INTEGER vs BIGINT are different LogicalType ids -- the strict
  // type-identity rule rejects mixing them. Users should make both
  // bounds the same type.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "ts_between(1::INTEGER, 100::BIGINT, true, true)",
               columns, false, IdentityAnalyzerProvider,
               "ts_between bounds have mismatched types");
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_RangeColumnMismatchVarcharVsInt) {
  // Bounds are VARCHAR but column is INTEGER -- column-vs-value type
  // mismatch (different from min/max mismatch).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  AssertFilter(
    {}, "SELECT * FROM foo WHERE b @@ ts_between('a', 'z', true, true)",
    columns, false, IdentityAnalyzerProvider, "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_RangeColumnMismatchIntVsVarchar) {
  // Bounds are INTEGER but column is VARCHAR.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(
    {}, "SELECT * FROM foo WHERE b @@ ts_between(1, 100, true, true)", columns,
    false, IdentityAnalyzerProvider, "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_RangeOnlyOneBoundColumnMismatch) {
  // Type-mismatch check uses the non-NULL bound when one side is NULL;
  // INT bound + NULL bound on a VARCHAR column should still error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(
    {}, "SELECT * FROM foo WHERE b @@ ts_between(NULL, 5, false, true)",
    columns, false, IdentityAnalyzerProvider, "incompatible with column type");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeNonIdentityVarchar) {
  // VARCHAR column with a non-keyword analyzer rejects standalone
  // RANGE (the indexed tokens are transformed; raw bounds wouldn't
  // line up with them).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ ts_between('a', 'm', true, true)",
               columns, false, SegmentationAnalyzerProvider,
               "requires keyword-analyzed column");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangePhraseNumericRejected) {
  // Inside ##, only VARCHAR bounds are meaningful (phrase positions
  // live on the analyzed text field). Numeric bounds throw.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ts_between(1, 5, true, true))",
               columns, false, SegmentationAnalyzerProvider,
               "## ts_between phrase part requires VARCHAR bounds");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangePhraseBoolRejected) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ts_between(false, true, true, true))",
               columns, false, SegmentationAnalyzerProvider,
               "## ts_between phrase part requires VARCHAR bounds");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeIntExclusiveBoth) {
  // Exclusive bounds on numeric: a > 1 AND a < 5.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 1, false, 5, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_between(1, 5, false, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeIntInclusiveExclusive) {
  // Mixed inclusivity: a >= 1 AND a < 5.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::INTEGER, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 1, true, 5, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_between(1, 5, true, false)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeBigint) {
  // BIGINT column accepts BIGINT bounds.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::BIGINT, .name = "b"}};
  irs::And expected;
  AddRangeFilter<int64_t>(expected, 1, int64_t{10}, true, int64_t{100}, true);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_between(10::BIGINT, 100::BIGINT, true, true)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_RangeDouble) {
  // DOUBLE column accepts DOUBLE bounds.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::DOUBLE, .name = "b"}};
  irs::And expected;
  AddRangeFilter<double>(expected, 1, 1.5, true, 9.5, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_between(1.5, 9.5, true, false)",
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
    "SELECT * FROM foo WHERE b @@ ts_between(false, NULL, true, false)",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTsqueryList) {
  // ts_any([..::TSQUERY]) -- list of explicitly-typed TSQUERYs.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(['quick'::TSQUERY, "
               "'fox'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfTsqueryList) {
  // ts_all([..::TSQUERY]) -- list of explicitly-typed TSQUERYs.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_all(['quick'::TSQUERY, "
               "'brown'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfBareStringList) {
  // ts_any([bare strings]) -- the registered VARCHAR[] -> TSQUERY[]
  // list cast lifts each element via the implicit VARCHAR -> TSQUERY
  // scalar cast.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(['quick', 'fox'])", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTsqueryArray) {
  // ts_any(CAST(... AS TSQUERY[N])) -- fixed-length ARRAY input.
  // Routes through the unsized-ARRAY function overload and the ARRAY
  // branch of the filter-builder dispatch.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_any(CAST(['quick'::TSQUERY, 'fox'::TSQUERY] AS TSQUERY[2]))",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfTsqueryArray) {
  // ts_all(CAST(... AS TSQUERY[N])) -- ARRAY input on the AND side.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"brown"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ "
    "ts_all(CAST(['quick'::TSQUERY, 'brown'::TSQUERY] AS TSQUERY[2]))",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfBareStringList) {
  // ts_all([bare strings]) via VARCHAR[] -> TSQUERY[] list cast.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"quick"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"brown"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_all(['quick', 'brown'])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_CommutativeLhsAnyOfBareStringList) {
  // ts_any([bare strings]) @@ b -- column on RHS.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AssertFilter(expected, "SELECT * FROM foo WHERE ts_any(['a', 'b']) @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_CommutativeLhsAllOfBareStringList) {
  // ts_all([bare strings]) @@ b.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"b"});
  AssertFilter(expected, "SELECT * FROM foo WHERE ts_all(['a', 'b']) @@ b",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfList) {
  // ts_any([list]) -- explicit list form. Equivalent to variadic.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"a"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"b"});
  AddTermFilter<std::string_view>(or_filter, 1, std::string_view{"c"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_any(['a'::TSQUERY, 'b'::TSQUERY, 'c'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfListMinMatch) {
  // ts_any([list], min_match) -- min_match=2 means at least 2 of 3
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
               "ts_any(['a', 'b', 'c'], 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfList) {
  // ts_all([list]) -- explicit list form, no min_match.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& and_filter = expected.add<irs::And>();
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"x"});
  AddTermFilter<std::string_view>(and_filter, 1, std::string_view{"y"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_all(['x'::TSQUERY, 'y'::TSQUERY])",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfMinExceedsArgs) {
  // min_match exceeds number of args -> bind-time error.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_any(['a', 'b'], 5)",
               columns, false, IdentityAnalyzerProvider, "min_match");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfRejectsMinMatchArg) {
  // ALL_OF doesn't accept a min_match argument (no `(list, int)` arm).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ ts_all(['x', 'y'], 1)",
               columns, false);
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ to_tsquery('quick')",
               columns, true);
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
               columns, false, IdentityAnalyzerProvider, "to_tsquery");
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
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ('a'::TSQUERY ## 2 ## 'b'::TSQUERY)", columns,
    true, SegmentationAnalyzerProvider);
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

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqIntervalArray) {
  // 'a' ## CAST([1, 3] AS INTEGER[2]) ## 'b' -- ARRAY (not LIST) interval
  // gap.
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
               "('a'::TSQUERY ## CAST([1, 3] AS INTEGER[2]) ## 'b'::TSQUERY)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAnyOfPart) {
  // 'a' ## ts_any(['b', 'c']) -- ANY_OF as phrase part maps to a
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
               "('a'::TSQUERY ## ts_any(['b', 'c']))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAnyOfPartExplicit1) {
  // 'a' ## ts_any(['b', 'c'], 1) -- explicit min_match=1 is allowed
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
               "('a'::TSQUERY ## ts_any(['b', 'c'], 1))",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_PhraseSeqAnyOfPartMinMatchRejected) {
  // ANY_OF with min_match > 1 inside ## is rejected: a phrase position
  // can match only one token, so min_match > 1 is unsatisfiable.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ts_any(['b', 'c'], 2))",
               columns, false, SegmentationAnalyzerProvider,
               "ts_any phrase part requires min_match=1");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseSeqAllOfPartRejected) {
  // ALL_OF as a phrase part is rejected (same reason as ANY_OF
  // with min_match > 1: a phrase position holds at most one token).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter({},
               "SELECT * FROM foo WHERE b @@ "
               "('a'::TSQUERY ## ts_all(['b', 'c']))",
               columns, false, SegmentationAnalyzerProvider,
               "ts_all phrase part is not supported");
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TsqueryPhraseFunction) {
  // tsquery_phrase(ts_phrase('hello'), ts_phrase('world'), 3) -- function
  // form of ##.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& phrase = expected.add<irs::ByPhrase>();
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>(0, 0).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"hello"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"world"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "tsquery_phrase(ts_phrase('hello'), ts_phrase('world'), 3)",
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
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'quick fox'", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_BareSingleTokenTerm) {
  // Single-token bare string stays as ByTerm (no ByTerms wrapping).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick"});
  AssertFilter(expected, "SELECT * FROM foo WHERE b @@ 'quick'", columns, true,
               SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizeFunction) {
  // ts_tokenize(text) 1-arg is same as bare-string semantics.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(expected, 1, {"quick", "fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_tokenize('quick fox')", columns,
               true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizeIdentity) {
  // ts_tokenize(text, 'keyword') bypasses the column analyzer entirely
  // and emits a raw ByTerm -- matches the unsplit input string as-is.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_tokenize('quick fox', 'keyword')", columns,
    true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizerCastIdentity) {
  // 'foo'::tokenize('keyword') -- parameterized-type cast equivalent
  // to ts_tokenize(text, 'keyword'). Bypasses the column analyzer.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'quick fox'::tokenize('keyword')",
               columns, true, SegmentationAnalyzerProvider);
}

// ts_phrase('text')::tokenize('keyword') -- the cast wraps a TSQUERY-typed
// expression. Two-alias scheme keeps the cast wrapper alive (TSQUERY ->
// TOKENIZED_TSQUERY differs in alias, so DuckDB doesn't elide). Walker
// reads the modifier from the cast's return_type, sets sub_ctx.tokenizer,
// recurses into PHRASE which uses the override at its leaf. With
// 'keyword' override, PHRASE tokenises 'quick fox' through the identity
// analyzer -> single phrase part with raw bytes (vs the segmentation
// column's split tokens). The non-keyword catalog-resolved path is
// covered end-to-end in sqllogic.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_PhraseCastIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  // PHRASE under identity tokeniser: identity emits a single raw token
  // for the whole input string, so the phrase has one part.
  {
    auto& phrase = expected.add<irs::ByPhrase>();
    *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
    phrase.mutable_options()->push_back<irs::ByTermOptions>().term.assign(
      irs::ViewCast<irs::byte_type>(std::string_view{"quick fox"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_phrase('quick "
               "fox')::tokenize('keyword')",
               columns, true, SegmentationAnalyzerProvider);
}

// (ts_levenshtein(...))::tokenize('keyword') -- cast on a TSQUERY-returning
// constructor with an analyzer-aware tokeniser inside. Verifies the
// override flows into LEVENSHTEIN's term-tokenisation step (which
// normally goes through the column analyzer).
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_LevenshteinCastIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "Quikc", 1);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_levenshtein('Quikc', 1)::tokenize('keyword')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizerCastNullSugar) {
  // 'foo'::tokenize(NULL) is sugar for ::tokenize('keyword'): both
  // bypass the column analyzer and emit a single raw ByTerm.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"quick fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'quick fox'::tokenize(NULL)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_TokenizerCastNamedNoCatalog) {
  // 'foo'::tokenize('english') against a unit-test connection has no
  // SereneDBClientState wired up, so ResolveTokenizerAnalyzer returns
  // an empty wrapper and the cast surfaces a "tokenizer not found"
  // bind error (ERRCODE_UNDEFINED_OBJECT). End-to-end coverage with
  // a real catalog lives in sqllogic.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ 'quick fox'::tokenize('english')",
               columns, false, SegmentationAnalyzerProvider,
               "tokenizer not found in catalog");
}

TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_TokenizeNamedAnalyzerNoCatalog) {
  // 2-arg ts_tokenize(text, name): named analyzer requires a real catalog.
  // The unit-test connection has no SereneDBClientState, so the
  // resolver returns null and the filter builder throws with
  // ERRCODE_UNDEFINED_OBJECT. End-to-end coverage with a real catalog
  // lives in sqllogic.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_tokenize('quick fox', 'english')", columns,
    false, SegmentationAnalyzerProvider, "tokenizer not found in catalog");
}

// Array form: ts_any(ts_tokenize([list], 'keyword')) -- bypass column
// analyzer, each list element becomes one ByTerm leaf with raw bytes.
// Two-element input -> ByTerms with min_match=1.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTokenizeListIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['foo', "
               "'bar'], 'keyword'))",
               columns, true, SegmentationAnalyzerProvider);
}

// Same as above but with a fixed-size VARCHAR[2] (ARRAY) input. Routes
// through the unsized-ARRAY TOKENIZE overload and the ARRAY branch of
// FromTokenizeListInAnyAllOf's child extraction.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTokenizeArrayIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_any(ts_tokenize(['foo', 'bar']::VARCHAR[2], 'keyword'))",
               columns, true, SegmentationAnalyzerProvider);
}

// Single-element identity collapses to ByTerm.
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_AnyOfTokenizeListIdentitySingle) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['foo'], 'keyword'))",
    columns, true, SegmentationAnalyzerProvider);
}

// Ambient column analyzer (1-arg form): each input element runs through
// the column's analyzer, all produced tokens flatten into one ByTerms
// with min_match=1.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTokenizeListAmbient) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['Foo', 'Bar']))", columns,
    true, SegmentationAnalyzerProvider);
}

// ALL_OF: same flatten, but min_match=token-count so every token must
// be present. Two raw-identity tokens -> ByTerms with min_match=2.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AllOfTokenizeListIdentity) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_all(ts_tokenize(['foo', "
               "'bar'], 'keyword'))",
               columns, true, SegmentationAnalyzerProvider);
}

// 2-arg ts_tokenize(text_array, name) requires a real catalog. The
// unit-test connection has no SereneDBClientState, so the resolver
// returns null and the filter builder throws with
// ERRCODE_UNDEFINED_OBJECT.
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_AnyOfTokenizeListNamedNoCatalog) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['foo','bar'], "
               "'english'))",
               columns, false, SegmentationAnalyzerProvider,
               "tokenizer not found in catalog");
}

// 'keyword' preserves multi-word inputs verbatim -- a single 'foo bar'
// element becomes a single ByTerm("foo bar"), no tokenisation. Confirms
// the identity path skips the analyzer split.
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_AnyOfTokenizeListIdentityKeepsSpaces) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo bar"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['foo bar'], 'keyword'))",
    columns, true, SegmentationAnalyzerProvider);
}

// Ambient analyzer + multi-token element: 'foo bar' splits to two
// tokens via the segmentation analyzer; combined with a sibling 'baz'
// element, the flattened list is [foo, bar, baz] under min_match=1.
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_AnyOfTokenizeListAmbientFlattens) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1,
    {std::string_view{"foo"}, std::string_view{"bar"},
     std::string_view{"baz"}});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['Foo Bar', 'BAZ']))",
    columns, true, SegmentationAnalyzerProvider);
}

// NULL elements in the list are skipped.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTokenizeListSkipsNulls) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_any(ts_tokenize(['foo', NULL, 'bar'], 'keyword'))",
               columns, true, SegmentationAnalyzerProvider);
}

// Empty-after-tokenisation input -> Empty filter. Stop-word-style empty
// tokenisation is exercised here by passing a list of empty strings to
// the segmenting analyzer (it produces zero tokens).
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_AnyOfTokenizeListEmptyTokensYieldsEmpty) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  expected.add<irs::Empty>();
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(['', '   ']))",
               columns, true, SegmentationAnalyzerProvider);
}

// ANY_OF with explicit min_match >= 1 against an array TOKENIZE: forces
// at least N of the flattened tokens to match. min_match=2 with 3
// tokens behaves like a 2-of-3 disjunction.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTokenizeListMinMatch) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"baz"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ "
               "ts_any(ts_tokenize(['foo','bar','baz'], 'keyword'), 2)",
               columns, true, SegmentationAnalyzerProvider);
}

// Negation via NOT(b @@ ts_any(ts_tokenize(...))): wraps the resulting
// ByTerms in irs::Not.
TEST_F(SearchFilterBuilderTest, test_TSQueryMatch_AnyOfTokenizeListWithNot) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddTermsFilter<std::string_view>(
    not_filter, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT(b @@ "
               "ts_any(ts_tokenize(['foo','bar'], 'keyword')))",
               columns, true, SegmentationAnalyzerProvider);
}

// Direct top-level use of TOKENIZE-array (without ANY_OF/ALL_OF wrapper)
// is rejected at SQL bind time -- @@ expects scalar TSQUERY, not
// LIST(TSQUERY). The DuckDB binder surfaces a "no function matches"
// error before the filter builder sees anything.
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_TokenizeListBareRejectedByBinder) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_tokenize(['foo','bar'])",
               columns, false, SegmentationAnalyzerProvider);
}

// Non-constant array (e.g. the column itself) is rejected: v1 only
// supports constant-folded LIST(VARCHAR) inputs to the array form.
TEST_F(SearchFilterBuilderTest,
       test_TSQueryMatch_AnyOfTokenizeListNonConstRejected) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"},
    {.id = 2,
     .type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
     .name = "tags"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE b @@ ts_any(ts_tokenize(tags))",
               columns, false, SegmentationAnalyzerProvider, "ts_tokenize");
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
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick brown')", columns,
    true, SegmentationAnalyzerProvider);
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
    "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('quick -spam')", columns,
    true, SegmentationAnalyzerProvider);
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
               "SELECT * FROM foo WHERE b @@ websearch_to_tsquery('')", columns,
               true, SegmentationAnalyzerProvider);
}

// ===========================================================================
// SQL-native predicate sugar -- `<name>(col, args...)` rewrites at
// filter-build time to `col @@ <inner ts_*(...)>`. Each test asserts
// the predicate produces the exact same filter as the @@ equivalent.
// ===========================================================================

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_eq_AtAtTsPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_WithGap_eq_AtAtTsPhrase) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick', 2, 'fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_NgramMatches_eq_AtAtTsNgram) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE ngram_matches(b, 'hello')",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest,
       test_NgramMatches_WithThreshold_eq_AtAtTsNgram) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"}, 0.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ngram_matches(b, 'hello', 0.5)",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_eq_AtAtTsLevenshtein) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2, false);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE levenshtein_matches(b, 'test', 2, false)", columns,
    true);
}

TEST_F(SearchFilterBuilderTest,
       test_LevenshteinMatches_WithPrefix_eq_AtAtTsLevenshtein) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "roximate", 1,
                        /*with_transpositions=*/true,
                        /*max_terms=*/1024, /*prefix=*/"app");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE levenshtein_matches(b, 'roximate', 1, "
               "true, 'app')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_eq_AtAtTsAllTsTokenize) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_all_tokens(b, ['Foo', 'Bar'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_List_eq_AtAtTsAnyTsTokenize) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_any_tokens(b, ['Foo', 'Bar'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest,
       test_HasAnyToken_ListWithMinMatch_eq_AtAtTsAnyTsTokenizeMinMatch) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"baz"}));
  }
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE has_any_tokens(b, ['Foo', 'Bar', 'Baz'], 2)",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_Text_eq_AtAtBareString) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE has_any_tokens(b, 'Foo')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest,
       test_HasAnyToken_TextWithMinMatch_eq_AtAtTsAnyListValueTokenize) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  // 'foo bar' tokenises to two tokens through the segmentation
  // analyzer -- both must match (min_match=2).
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_any_tokens(b, 'Foo Bar', 2)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_BoostCastWraps) {
  // Predicates return BOOLEAN, so the SQL `::boost(K)` cast composes
  // through the same machinery as direct `@@` results.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"}).boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (phrase_matches(category, 'quick "
               "brown fox'))::boost(2.0)",
               columns, true, SegmentationAnalyzerProvider);
}

// ===========================================================================
// Predicate-sugar comprehensive coverage. Each block exercises a single
// predicate across argument shapes, boolean composition, negation, and
// boost. Cross-cutting tests at the end mix predicates and verify
// error paths.
// ===========================================================================

// ---------------------------------------------------------------------------
// phrase_matches
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_SingleToken) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"foo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE phrase_matches(b, 'foo')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_MultipleGaps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(2, 2).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"brown"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(3, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE phrase_matches(category, 'quick', 1, "
               "'brown', 2, 'fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_RangeGap) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& phrase = AddFilter<irs::ByPhrase>(expected);
  *phrase.mutable_field() = MakeFieldName<std::string_view>(1);
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE phrase_matches(category, 'quick', "
               "ARRAY[1,2], 'fox')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AddPhraseFilter(expected, 1, {"quick", "lazy", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown fox') "
    "AND phrase_matches(category, 'quick lazy fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_OredWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddPhraseFilter(or_filter, 1, {"quick", "brown", "fox"});
  AddPhraseFilter(or_filter, 1, {"quick", "lazy", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown fox') "
    "OR phrase_matches(category, 'quick lazy fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddPhraseFilter(not_filter, 1, {"quick", "brown", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE NOT phrase_matches(category, 'quick brown fox')",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_AndedWithNumericRange) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "n"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown", "fox"});
  AddRangeFilter<int32_t>(expected, 2, 10, true, std::nullopt, false);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown fox') "
    "AND n >= 10",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PhraseMatches_GapEndingError) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  // ts_phrase ends with a gap -- error message preserved through the
  // sugar rewrite (FromPredicate dispatches to FromTSQueryMatch which
  // dispatches to FromPhrase, where the validation lives).
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE phrase_matches(category, 'quick', 2)",
               columns, false, SegmentationAnalyzerProvider, "ts_phrase");
}

// ---------------------------------------------------------------------------
// ngram_matches
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_NgramMatches_DefaultThreshold) {
  // Default threshold = 0.7 (matches AddNgramSimilarityFilter default).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE ngram_matches(b, 'hello')",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_NgramMatches_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddNgramSimilarityFilter(not_filter, 1, {"he", "el", "ll", "lo"});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT ngram_matches(b, 'hello')", columns,
               true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_NgramMatches_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"});
  AddNgramSimilarityFilter(expected, 1, {"wo", "or", "rl", "ld"}, 0.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ngram_matches(b, 'hello') AND "
               "ngram_matches(b, 'world', 0.5)",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_NgramMatches_OredWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddNgramSimilarityFilter(or_filter, 1, {"he", "el", "ll", "lo"}, 0.5f);
  AddNgramSimilarityFilter(or_filter, 1, {"wo", "or", "rl", "ld"}, 0.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ngram_matches(b, 'hello', 0.5) OR "
               "ngram_matches(b, 'world', 0.5)",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_NgramMatches_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddNgramSimilarityFilter(expected, 1, {"he", "el", "ll", "lo"}, 0.5f)
    .boost(3.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (ngram_matches(b, 'hello', "
               "0.5))::boost(3.5)",
               columns, true, NgramAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_NgramMatches_NoFeaturesError) {
  // Default keyword analyzer lacks Pos+Freq features required for
  // ngram. Error surfaces through the sugar rewrite.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{}, "SELECT * FROM foo WHERE ngram_matches(b, 'hello')",
               columns, false, IdentityAnalyzerProvider, "ts_ngram");
}

// ---------------------------------------------------------------------------
// levenshtein_matches
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_3Arg) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE levenshtein_matches(b, 'test', 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_5Arg_EmptyPrefix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2, /*with_transpositions=*/false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE levenshtein_matches(b, 'test', 2, "
               "false, '')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddEditDistanceFilter(not_filter, 1, "test", 2);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT levenshtein_matches(b, 'test', 2)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 1);
  AddEditDistanceFilter(expected, 1, "best", 1);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE levenshtein_matches(b, 'test', 1) AND "
               "levenshtein_matches(b, 'best', 1)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_OredWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddEditDistanceFilter(or_filter, 1, "test", 1);
  AddEditDistanceFilter(or_filter, 1, "best", 1);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE levenshtein_matches(b, 'test', 1) OR "
               "levenshtein_matches(b, 'best', 1)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2).boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (levenshtein_matches(b, 'test', "
               "2))::boost(1.5)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_LevenshteinMatches_AndedWithRange) {
  // `n > 0 AND n < 100` produces two separate GranularRange filters at
  // the AND root (DuckDB doesn't fuse the bounds).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"},
    {.id = 2, .type = duckdb::LogicalType::INTEGER, .name = "n"}};
  irs::And expected;
  AddEditDistanceFilter(expected, 1, "test", 2);
  AddRangeFilter<int32_t>(expected, 2, 0, false, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 2, std::nullopt, false, 100, false);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE levenshtein_matches(b, 'test', 2) "
               "AND n > 0 AND n < 100",
               columns, true);
}

// ---------------------------------------------------------------------------
// has_all_tokens
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_SingleElementList) {
  // Single-element list with single-token element collapses to ByTerm
  // (FromTokenizeListInAnyAllOf single-token short-circuit).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE has_all_tokens(b, ['Foo'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_MultiTokenElement) {
  // 'foo bar' tokenises to ['foo', 'bar'] under segmentation; combined
  // with 'baz' the flattened list is [foo, bar, baz] under min_match=3
  // (ALL_OF semantics).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 3;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"baz"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_all_tokens(b, ['Foo Bar', 'Baz'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  {
    auto& terms = AddFilter<irs::ByTerms>(not_filter);
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT has_all_tokens(b, ['Foo', 'Bar'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"baz"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"qux"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_all_tokens(b, ['Foo', 'Bar']) "
               "AND has_all_tokens(b, ['Baz', 'Qux'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_OredWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  {
    auto& terms = or_filter.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  {
    auto& terms = or_filter.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"baz"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"qux"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_all_tokens(b, ['Foo', 'Bar']) "
               "OR has_all_tokens(b, ['Baz', 'Qux'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
    static_cast<irs::FilterWithBoost&>(terms).boost(2.5f);
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (has_all_tokens(b, ['Foo', "
               "'Bar']))::boost(2.5)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAllTokens_IdentityAnalyzer) {
  // With identity analyzer: each list element becomes one raw term.
  // 'Foo Bar' stays as a single ByTerm("Foo Bar") because the identity
  // analyzer does not split on whitespace -- but list path doesn't
  // accept analyzer override and uses ambient (identity here), so
  // each element is preserved verbatim with min_match = list size.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"Foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"Bar"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_all_tokens(b, ['Foo', 'Bar'])",
               columns, true, IdentityAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// has_any_tokens
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_List_MinMatch1Default) {
  // Default min_match for ts_any is 1 -- equivalent to no min_match arg.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_any_tokens(b, ['Foo', 'Bar'], 1)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_List_MinMatchEqualsSize) {
  // min_match = list size -- behaves like has_all_tokens.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_any_tokens(b, ['Foo', 'Bar'], 2)",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_Text_SingleToken) {
  // Single-token text via segmentation -> bare-string @@ produces a ByTerm.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermFilter<std::string_view>(expected, 1, std::string_view{"foo"});
  AssertFilter(expected, "SELECT * FROM foo WHERE has_any_tokens(b, 'Foo')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_Text_MultiToken) {
  // Multi-token text via segmentation -> bare-string @@ produces ByTerms
  // with min_match=1 (OR semantics).
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected, "SELECT * FROM foo WHERE has_any_tokens(b, 'Foo Bar')",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddTermsFilter<std::string_view>(
    not_filter, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT has_any_tokens(b, ['Foo', 'Bar'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"baz"}, std::string_view{"qux"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_any_tokens(b, ['Foo', 'Bar']) AND "
               "has_any_tokens(b, ['Baz', 'Qux'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_OredWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermsFilter<std::string_view>(
    or_filter, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AddTermsFilter<std::string_view>(
    or_filter, 1, {std::string_view{"baz"}, std::string_view{"qux"}});
  AssertFilter(expected,
               "SELECT * FROM foo WHERE has_any_tokens(b, ['Foo', 'Bar']) OR "
               "has_any_tokens(b, ['Baz', 'Qux'])",
               columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}})
    .boost(0.5f);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE (has_any_tokens(b, ['Foo', 'Bar']))::boost(0.5)",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_HasAnyToken_TextWithMinMatch_BoostCast) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  {
    auto& terms = expected.add<irs::ByTerms>();
    *terms.mutable_field() = MakeFieldName<std::string_view>(1);
    auto& opts = *terms.mutable_options();
    opts.min_match = 2;
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}));
    opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{"bar"}));
    static_cast<irs::FilterWithBoost&>(terms).boost(4.0f);
  }
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (has_any_tokens(b, 'Foo Bar', "
               "2))::boost(4.0)",
               columns, true, SegmentationAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// Cross-cutting: mixed predicates, complex composition, error paths
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_PredicateMix_PhraseAndLevenshtein) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown"});
  AddEditDistanceFilter(expected, 1, "test", 2);
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown') AND "
    "levenshtein_matches(category, 'test', 2)",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PredicateMix_OrOfDifferentPredicates) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddPhraseFilter(or_filter, 1, {"quick", "brown"});
  AddTermsFilter<std::string_view>(
    or_filter, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown') OR "
    "has_any_tokens(category, ['Foo', 'Bar'])",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PredicateMix_NotOfAnd) {
  // DuckDB does not apply De Morgan here -- the bound expression
  // arrives as `NOT (A AND B)` and the filter builder mirrors that
  // shape with a Not over an And.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& inner_and = AddFilter<irs::And>(not_filter);
  AddPhraseFilter(inner_and, 1, {"quick", "brown"});
  AddPhraseFilter(inner_and, 1, {"red", "fox"});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE NOT (phrase_matches(category, 'quick brown') "
    "AND phrase_matches(category, 'red fox'))",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_PredicateMix_AndOfThree) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "category"}};
  irs::And expected;
  AddPhraseFilter(expected, 1, {"quick", "brown"});
  AddEditDistanceFilter(expected, 1, "test", 2);
  AddTermsFilter<std::string_view>(
    expected, 1, {std::string_view{"foo"}, std::string_view{"bar"}});
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE phrase_matches(category, 'quick brown') AND "
    "levenshtein_matches(category, 'test', 2) AND "
    "has_any_tokens(category, ['Foo', 'Bar'])",
    columns, true, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Predicate_NonColumnFirstArg) {
  // First arg is a constant, not a column ref -- the @@ handler rejects
  // it via "@@ requires a column reference on one side".
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE phrase_matches('not a column', 'foo')",
               columns, false, SegmentationAnalyzerProvider, "@@");
}

// ===========================================================================
// DuckDB built-in claims (contains / starts_with / ^@ / ends_with /
// suffix / regexp_matches / regexp_like). All require a keyword-
// analyzed column; lower to `col @@ ts_*(...)`. Each asserts the
// produced filter equals what `col @@ ts_*` would build directly.
// ===========================================================================

// ---------------------------------------------------------------------------
// contains
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_Contains_eq_AtAtTsLikeWrapped) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo%");
  AssertFilter(expected, "SELECT * FROM foo WHERE contains(b, 'foo')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_Contains_EscapesSpecialChars) {
  // '50%' should become LIKE '%50\%%' so the user-literal `%` is
  // matched, not interpreted as a wildcard.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%50\\%%");
  AssertFilter(expected, "SELECT * FROM foo WHERE contains(b, '50%')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_Contains_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo%");
  AddLikeFilter(expected, 1, "%bar%");
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE contains(b, 'foo') AND contains(b, 'bar')",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Contains_OredWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddLikeFilter(or_filter, 1, "%foo%");
  AddLikeFilter(or_filter, 1, "%bar%");
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE contains(b, 'foo') OR contains(b, 'bar')", columns,
    true);
}

TEST_F(SearchFilterBuilderTest, test_Contains_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddLikeFilter(not_filter, 1, "%foo%");
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT contains(b, 'foo')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Contains_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo%").boost(2.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (contains(b, 'foo'))::boost(2.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_Contains_DeclinesNonKeywordAnalyzer) {
  // Non-keyword analyzer -> claim silently declines so DuckDB evaluates
  // the predicate per row.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{}, "SELECT * FROM foo WHERE contains(b, 'foo')",
               columns, false, SegmentationAnalyzerProvider);
}

TEST_F(SearchFilterBuilderTest, test_Contains_DeclinesListShape) {
  // contains([1,2,3]::INT[], 2) -- LIST shape; our claim declines.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AssertFilter(expected,
               "SELECT * FROM foo WHERE contains([1, 2, 3]::INT[], 2)", columns,
               false);
}

TEST_F(SearchFilterBuilderTest, test_Contains_DeclinesNonConstant) {
  // contains(b, b) -- pattern is a column ref, not constant; declines.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{}, "SELECT * FROM foo WHERE contains(b, b)", columns,
               false);
}

// ---------------------------------------------------------------------------
// starts_with / ^@
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_StartsWith_eq_AtAtTsStartsWith) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, "foo");
  AssertFilter(expected, "SELECT * FROM foo WHERE starts_with(b, 'foo')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_StartsWith_OperatorAlias) {
  // `b ^@ 'foo'` lowers to the same function as `starts_with`.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, "foo");
  AssertFilter(expected, "SELECT * FROM foo WHERE b ^@ 'foo'", columns, true);
}

TEST_F(SearchFilterBuilderTest, test_StartsWith_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, "foo");
  AddPrefixFilter(expected, 1, "ba");
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE starts_with(b, 'foo') AND starts_with(b, 'ba')",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_StartsWith_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddPrefixFilter(not_filter, 1, "foo");
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT starts_with(b, 'foo')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_StartsWith_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddPrefixFilter(expected, 1, "foo").boost(3.0f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (starts_with(b, 'foo'))::boost(3.0)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_StartsWith_DeclinesNonKeywordAnalyzer) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{}, "SELECT * FROM foo WHERE starts_with(b, 'foo')",
               columns, false, SegmentationAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// ends_with / suffix
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_EndsWith_eq_AtAtTsLikeAnchored) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo");
  AssertFilter(expected, "SELECT * FROM foo WHERE ends_with(b, 'foo')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_EndsWith_AliasSuffix) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%foo");
  AssertFilter(expected, "SELECT * FROM foo WHERE suffix(b, 'foo')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_EndsWith_EscapesSpecialChars) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%a\\_b");
  AssertFilter(expected, "SELECT * FROM foo WHERE ends_with(b, 'a_b')", columns,
               true);
}

TEST_F(SearchFilterBuilderTest, test_EndsWith_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddLikeFilter(not_filter, 1, "%foo");
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT ends_with(b, 'foo')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_EndsWith_DeclinesNonKeywordAnalyzer) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{}, "SELECT * FROM foo WHERE ends_with(b, 'foo')",
               columns, false, SegmentationAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// regexp_matches / regexp_like
// ---------------------------------------------------------------------------

// DuckDB's regex_range_filter optimizer rewrites simple regex patterns
// (like `^foo`, `bar$`) into starts_with / LIKE before our claim sees
// them. Use patterns with character classes / quantifiers that survive
// the optimizer so we test the real ts_regexp claim path.
TEST_F(SearchFilterBuilderTest, test_RegexpMatches_eq_AtAtTsRegexp) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, "[a-z]+[0-9]+");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE regexp_matches(b, '[a-z]+[0-9]+')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_RegexpMatches_AliasRegexpLike) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, "[a-z]+[0-9]+");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE regexp_like(b, '[a-z]+[0-9]+')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_RegexpMatches_AndedWithSelf) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, "[a-z]+");
  AddRegexpFilter(expected, 1, "[0-9]+");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE regexp_matches(b, '[a-z]+') AND "
               "regexp_matches(b, '[0-9]+')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_RegexpMatches_Negated) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddRegexpFilter(not_filter, 1, "[a-z]+[0-9]+");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT regexp_matches(b, '[a-z]+[0-9]+')",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_RegexpMatches_BoostCastWraps) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddRegexpFilter(expected, 1, "[a-z]+[0-9]+").boost(1.5f);
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (regexp_matches(b, "
               "'[a-z]+[0-9]+'))::boost(1.5)",
               columns, true);
}

TEST_F(SearchFilterBuilderTest, test_RegexpMatches_DeclinesThreeArg) {
  // 3-arg form (with options) -- not claimed.
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE regexp_matches(b, '[a-z]+', 'i')",
               columns, false);
}

TEST_F(SearchFilterBuilderTest, test_RegexpMatches_DeclinesNonKeywordAnalyzer) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  AssertFilter(irs::And{},
               "SELECT * FROM foo WHERE regexp_matches(b, '[a-z]+[0-9]+')",
               columns, false, SegmentationAnalyzerProvider);
}

// ---------------------------------------------------------------------------
// Cross-cutting: mixing built-ins with each other and with sugar
// ---------------------------------------------------------------------------

TEST_F(SearchFilterBuilderTest, test_BuiltinMix_ContainsAndStartsWith) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  AddLikeFilter(expected, 1, "%bar%");
  AddPrefixFilter(expected, 1, "foo");
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE contains(b, 'bar') AND starts_with(b, 'foo')",
    columns, true);
}

TEST_F(SearchFilterBuilderTest, test_BuiltinMix_OrEndsWithRegexp) {
  std::vector<ColumnSpec> columns{
    {.id = 1, .type = duckdb::LogicalType::VARCHAR, .name = "b"}};
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddLikeFilter(or_filter, 1, "%bar");
  AddRegexpFilter(or_filter, 1, "[a-z]+[0-9]+");
  AssertFilter(expected,
               "SELECT * FROM foo WHERE ends_with(b, 'bar') OR "
               "regexp_matches(b, '[a-z]+[0-9]+')",
               columns, true);
}

}  // namespace
