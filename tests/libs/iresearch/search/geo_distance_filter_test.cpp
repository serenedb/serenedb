////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <set>

#include "formats/column/test_cs_helpers.hpp"
#include "geo_test_helpers.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/geo_filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "s2/s2point_region.h"
#include "s2/s2polygon.h"
#include "search/filter_test_case_base.hpp"
#include "search_fields.hpp"
#include "tests_shared.hpp"

namespace {

using namespace sdb::geo;
using namespace irs;
using namespace irs::tests;

inline constexpr irs::field_id kName = 1;
inline constexpr irs::field_id kGeo = 2;
// Stand-ins used by filter-only ctor/equal/boost cases that previously
// passed bare strings ("field", "field1") rather than indexing data.
inline constexpr irs::field_id kFieldFieldId = 3;
inline constexpr irs::field_id kField1FieldId = 4;

struct CustomSort final : public irs::ScorerBase<void> {
  static constexpr std::string_view type_name() noexcept {
    return "custom_sort";
  }

  struct Scorer : public irs::ScoreOperator {
    Scorer(const CustomSort& sort, const irs::ScoreContext& ctx)
      : document_attrs(ctx.doc_attrs),
        stats(ctx.stats),
        segment_reader(ctx.segment),
        sort(sort) {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      ASSERT_EQ(MergeType, irs::ScoreMergeType::Noop);
      if (sort.scorer_score) {
        sort.scorer_score(this, res, n);
      }
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    const irs::AttributeProvider& document_attrs;
    const irs::byte_type* stats;
    const irs::NormProvider& segment_reader;
    const CustomSort& sort;
  };

  void collect(irs::byte_type* stats, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final {
    if (collector_finish) {
      collector_finish(stats, field, term);
    }
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (_prepare_scorer) {
      _prepare_scorer(ctx);
    }

    return irs::ScoreFunction::Make<CustomSort::Scorer>(*this, ctx);
  }

  std::function<void(irs::byte_type*, const irs::FieldCollector*,
                     const irs::TermCollector*)>
    collector_finish;
  std::function<void(const irs::ScoreContext& ctx)> _prepare_scorer;  // NOLINT
  std::function<void(const irs::ScoreOperator*, irs::score_t*, size_t n)>
    scorer_score;

  static ptr make();
  CustomSort() = default;
};

}  // namespace

TEST(GeoDistanceFilterTest, options) {
  const S2RegionTermIndexer::Options s2opts;
  const GeoDistanceFilterOptions opts;
  ASSERT_TRUE(opts.prefix.empty());
  ASSERT_EQ(0., opts.range.min);
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.min_type);
  ASSERT_EQ(0., opts.range.max);
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.max_type);
  ASSERT_EQ(S2Point{}, opts.origin);
  ASSERT_EQ(s2opts.level_mod(), opts.options.level_mod());
  ASSERT_EQ(s2opts.min_level(), opts.options.min_level());
  ASSERT_EQ(s2opts.max_level(), opts.options.max_level());
  ASSERT_EQ(s2opts.max_cells(), opts.options.max_cells());
  ASSERT_EQ(s2opts.marker(), opts.options.marker());
  ASSERT_EQ(s2opts.index_contains_points_only(),
            opts.options.index_contains_points_only());
  ASSERT_EQ(s2opts.optimize_for_space(), opts.options.optimize_for_space());
}

TEST(GeoDistanceFilterTest, ctor) {
  GeoDistanceFilter q;
  ASSERT_EQ(irs::Type<GeoDistanceFilter>::id(), q.type());
  ASSERT_EQ(irs::field_limits::invalid(), q.field_id());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
#ifndef SDB_DEV
  ASSERT_EQ(GeoDistanceFilterOptions{}, q.options());
#endif
}

TEST(GeoDistanceFilterTest, equal) {
  GeoDistanceFilter q;
  q.mutable_options()->origin = S2Point{1., 0., 0.};
  q.mutable_options()->range.min = 5000.;
  q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q.mutable_options()->range.max = 7000.;
  q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  *q.mutable_field_id() = kFieldFieldId;

  {
    GeoDistanceFilter q1;
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_EQ(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.boost(1.5);
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_EQ(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.boost(1.5);
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kField1FieldId;

    ASSERT_NE(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_NE(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 6000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_NE(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_NE(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.mutable_options()->origin = S2Point{1., 0., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q1.mutable_options()->range.max = 6000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_NE(q, q1);
  }

  {
    GeoDistanceFilter q1;
    q1.mutable_options()->origin = S2Point{0., 1., 0.};
    q1.mutable_options()->range.min = 5000.;
    q1.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q1.mutable_options()->range.max = 7000.;
    q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q1.mutable_field_id() = kFieldFieldId;

    ASSERT_NE(q, q1);
  }
}

TEST(GeoDistanceFilterTest, boost) {
  {
    GeoDistanceFilter q;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(-41.69642, 77.91159).ToPoint();
    q.mutable_options()->range.min = 5000.;
    q.mutable_options()->options.set_max_cells(50);
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    *q.mutable_field_id() = kFieldFieldId;
    q.mutable_options()->store_field_id = kGeo;

    ::tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  {
    GeoDistanceFilter q;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(-41.69642, 77.91159).ToPoint();
    q.mutable_options()->range.min = 5000.;
    q.mutable_options()->options.set_max_cells(150);
    q.mutable_options()->options.set_min_level(15);
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max = 5500.;
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q.mutable_field_id() = kFieldFieldId;
    q.mutable_options()->store_field_id = kGeo;

    ::tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  {
    irs::score_t boost = 1.5f;
    GeoDistanceFilter q;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(-41.69642, 77.91159).ToPoint();
    q.mutable_options()->range.min = 5000.;
    q.mutable_options()->options.set_max_cells(50);
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    *q.mutable_field_id() = kFieldFieldId;
    q.mutable_options()->store_field_id = kGeo;
    q.boost(boost);

    ::tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(boost, prepared.Query(0)->Boost());
  }

  {
    irs::score_t boost = 1.5f;
    GeoDistanceFilter q;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(-41.69642, 77.91159).ToPoint();
    q.mutable_options()->options.set_max_cells(50);
    q.mutable_options()->options.set_min_level(15);
    q.mutable_options()->range.min = 5000.;
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max = 6000.;
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    *q.mutable_field_id() = kFieldFieldId;
    q.mutable_options()->store_field_id = kGeo;
    q.boost(boost);

    ::tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(boost, prepared.Query(0)->Boost());
  }
}

TEST(GeoDistanceFilterTest, query) {
  auto docs = irs::tests::ParseGeoDocs(R"([
    { "name": "A", "geometry": { "type": "Point", "coordinates": [ 37.615895, 55.7039   ] } },
    { "name": "B", "geometry": { "type": "Point", "coordinates": [ 37.615315, 55.703915 ] } },
    { "name": "C", "geometry": { "type": "Point", "coordinates": [ 37.61509, 55.703537  ] } },
    { "name": "D", "geometry": { "type": "Point", "coordinates": [ 37.614183, 55.703806 ] } },
    { "name": "E", "geometry": { "type": "Point", "coordinates": [ 37.613792, 55.704405 ] } },
    { "name": "F", "geometry": { "type": "Point", "coordinates": [ 37.614956, 55.704695 ] } },
    { "name": "G", "geometry": { "type": "Point", "coordinates": [ 37.616297, 55.704831 ] } },
    { "name": "H", "geometry": { "type": "Point", "coordinates": [ 37.617053, 55.70461  ] } },
    { "name": "I", "geometry": { "type": "Point", "coordinates": [ 37.61582, 55.704459  ] } },
    { "name": "J", "geometry": { "type": "Point", "coordinates": [ 37.614634, 55.704338 ] } },
    { "name": "K", "geometry": { "type": "Point", "coordinates": [ 37.613121, 55.704193 ] } },
    { "name": "L", "geometry": { "type": "Point", "coordinates": [ 37.614135, 55.703298 ] } },
    { "name": "M", "geometry": { "type": "Point", "coordinates": [ 37.613663, 55.704002 ] } },
    { "name": "N", "geometry": { "type": "Point", "coordinates": [ 37.616522, 55.704235 ] } },
    { "name": "O", "geometry": { "type": "Point", "coordinates": [ 37.615508, 55.704172 ] } },
    { "name": "P", "geometry": { "type": "Point", "coordinates": [ 37.614629, 55.704081 ] } },
    { "name": "Q", "geometry": { "type": "Point", "coordinates": [ 37.610235, 55.709754 ] } },
    { "name": "R", "geometry": { "type": "Point", "coordinates": [ 37.605,    55.707917 ] } },
    { "name": "S", "geometry": { "type": "Point", "coordinates": [ 37.545776, 55.722083 ] } },
    { "name": "T", "geometry": { "type": "Point", "coordinates": [ 37.559509, 55.715895 ] } },
    { "name": "U", "geometry": { "type": "Point", "coordinates": [ 37.701645, 55.832144 ] } },
    { "name": "V", "geometry": { "type": "Point", "coordinates": [ 37.73735,  55.816715 ] } },
    { "name": "W", "geometry": { "type": "Point", "coordinates": [ 37.75589,  55.798193 ] } },
    { "name": "X", "geometry": { "type": "Point", "coordinates": [ 37.659073, 55.843711 ] } },
    { "name": "Y", "geometry": { "type": "Point", "coordinates": [ 37.778549, 55.823659 ] } },
    { "name": "Z", "geometry": { "type": "Point", "coordinates": [ 37.729797, 55.853733 ] } },
    { "name": "1", "geometry": { "type": "Point", "coordinates": [ 37.608261, 55.784682 ] } },
    { "name": "2", "geometry": { "type": "Point", "coordinates": [ 37.525177, 55.802825 ] } }
  ])");

  irs::MemoryDirectory dir;
  irs::DirectoryReader reader;

  // index data
  {
    constexpr auto kFormatId = "1_5simd";
    auto codec = irs::formats::Get(kFormatId);
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    GeoField geo_field;
    geo_field.field_name = "geometry";
    geo_field.id = kGeo;
    StringField name_field;
    name_field.field_name = "name";
    name_field.id = kName;
    {
      auto segment0 = writer->GetBatch();
      auto segment1 = writer->GetBatch();
      {
        size_t i = 0;
        for (const auto& doc_entry : docs) {
          geo_field.value = doc_entry.geometry;
          name_field.value = doc_entry.name;

          auto doc = (i++ % 2 ? segment0 : segment1).Insert();
          ASSERT_TRUE(doc.Insert(name_field));
          ASSERT_TRUE(doc.Insert(geo_field));
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(),
                                   name_field);
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kGeo, doc.DocId(),
                                   geo_field);
        }
      }
      segment1.Commit();
      segment0.Commit();
    }
    writer->RefreshCommit();
    reader = writer->GetSnapshot();
  }

  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(2U, reader->size());
  ASSERT_EQ(docs.size(), reader->docs_count());
  ASSERT_EQ(docs.size(), reader->live_docs_count());

  auto execute_query = [&reader](GeoDistanceFilter q,
                                 const std::vector<irs::CostAttr::Type>& costs,
                                 size_t at_least = 0) {
    std::set<std::string> actual_results;

    auto optimized = ::tests::Optimized(std::move(q));

    struct MaxMemoryCounter final : irs::IResourceManager {
      void Reset() noexcept {
        current = 0;
        max = 0;
      }

      void Increase(size_t value) final {
        current += value;
        max = std::max(max, current);
      }

      void Decrease(size_t value) noexcept final { current -= value; }

      size_t current{0};
      size_t max{0};
    };

    MaxMemoryCounter counter;
    std::optional<::tests::PreparedFilter> prepared{std::in_place, *optimized,
                                                    *reader, nullptr, counter};
    auto expected_cost = costs.begin();
    for (size_t i = 0; auto& segment : *reader) {
      const auto* column = segment.Column(kName);
      EXPECT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto it = prepared->Execute(i);
      EXPECT_NE(nullptr, it);
      auto seek_it = prepared->Execute(i);
      EXPECT_NE(nullptr, seek_it);
      auto* cost = irs::get<irs::CostAttr>(*it);
      EXPECT_NE(nullptr, cost);

      EXPECT_NE(expected_cost, costs.end());
      EXPECT_EQ(*expected_cost, cost->estimate());
      ++expected_cost;

      if (irs::doc_limits::eof(it->value())) {
        ++i;
        continue;
      }

      EXPECT_FALSE(irs::doc_limits::valid(it->value()));
      while (!irs::doc_limits::eof(it->advance())) {
        auto doc_id = it->value();
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_FALSE(values.IsNull(doc_id));

        actual_results.emplace(
          irs::tests::ReadStoredStr<std::string>(values, doc_id));
      }
      EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      EXPECT_TRUE(irs::doc_limits::eof(seek_it->seek(it->value())));

      {
        auto it = prepared->Execute(i);
        EXPECT_NE(nullptr, it);

        while (!irs::doc_limits::eof(it->advance())) {
          const auto doc_id = it->value();
          auto seek_it = prepared->Execute(i);
          EXPECT_NE(nullptr, seek_it);
          EXPECT_EQ(doc_id, seek_it->seek(doc_id));
          do {
            if (!values.IsNull(seek_it->value())) {
              EXPECT_NE(
                actual_results.end(),
                actual_results.find(irs::tests::ReadStoredStr<std::string>(
                  values, seek_it->value())));
            }
          } while (!irs::doc_limits::eof(seek_it->advance()));
          EXPECT_TRUE(irs::doc_limits::eof(seek_it->value()));
        }
        EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      }
      ++i;
    }
    EXPECT_EQ(expected_cost, costs.end());

    prepared.reset();
    EXPECT_EQ(counter.current, 0);
    EXPECT_GE(counter.max, at_least);

    return actual_results;
  };

  {
    const std::set<std::string> expected{"Q", "R"};

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70892, 37.607768).ToPoint();
    auto& range = q.mutable_options()->range;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 300;

    ASSERT_EQ(expected, execute_query(q, {2, 2}, 1));
  }

  {
    const std::set<std::string> expected{"Q"};

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.709754, 37.610235).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.max = 0;
    range.max_type = irs::BoundType::Inclusive;
    range.min = 0;

    ASSERT_EQ(expected, execute_query(q, {2, 0}));
  }

  {
    const std::set<std::string> expected{};

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.709754, 37.610235).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Exclusive;
    range.max = 0;
    range.max_type = irs::BoundType::Inclusive;
    range.min = 0;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    const std::set<std::string> expected{};

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.709754, 37.610235).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.max = 0;
    range.max_type = irs::BoundType::Exclusive;
    range.min = 0;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    const std::set<std::string> expected{};

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.709754, 37.610235).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Exclusive;
    range.max = 0;
    range.max_type = irs::BoundType::Exclusive;
    range.min = 0;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.709754, 37.610235).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Unbounded;
    range.max = 0;
    range.max_type = irs::BoundType::Unbounded;
    range.min = 0;

    ASSERT_EQ(expected,
              execute_query(q, {expected.size() / 2, expected.size() / 2}));
  }

  {
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      if (doc_entry.name == "Q") {
        continue;
      }
      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.709754, 37.610235).ToPoint();
    auto& range = q.mutable_options()->range;
    range.max_type = irs::BoundType::Unbounded;
    range.min_type = irs::BoundType::Exclusive;
    range.min = 0;

    ASSERT_EQ(expected, execute_query(q, {14, 14}));
  }

  {
    sdb::geo::ShapeContainer lhs, rhs;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      irs::tests::FromJson(docs[7].geometry).value(), lhs, cache,
      sdb::geo::coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        irs::tests::FromJson(doc_entry.geometry).value(), rhs, cache,
        sdb::geo::coding::Options::Invalid, nullptr));
      const auto dist = lhs.distanceFromCentroid(rhs.centroid());
      if (dist < 100 || dist > 2000) {
        continue;
      }

      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = 100;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 2000;

    ASSERT_EQ(expected, execute_query(q, {18, 18}));
  }

  {
    sdb::geo::ShapeContainer lhs, rhs;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      irs::tests::FromJson(docs[7].geometry).value(), lhs, cache,
      sdb::geo::coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        irs::tests::FromJson(doc_entry.geometry).value(), rhs, cache,
        sdb::geo::coding::Options::Invalid, nullptr));
      const auto dist = lhs.distanceFromCentroid(rhs.centroid());
      if (dist >= 2000) {
        continue;
      }

      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = -100;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 2000;

    ASSERT_EQ(expected, execute_query(q, {18, 18}));
  }

  {
    std::set<std::string> expected;

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = 100;
    range.max_type = irs::BoundType::Inclusive;
    range.max = -2000;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    std::set<std::string> expected;

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Unbounded;
    range.max_type = irs::BoundType::Inclusive;
    range.max = -2000;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();

    ASSERT_EQ(expected,
              execute_query(q, {expected.size() / 2, expected.size() / 2}));
  }

  {
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = -100;
    range.max_type = irs::BoundType::Unbounded;

    ASSERT_EQ(expected,
              execute_query(q, {expected.size() / 2, expected.size() / 2}));
  }

  {
    std::set<std::string> expected;

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = -100;
    range.max_type = irs::BoundType::Inclusive;
    range.max = -2000;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    sdb::geo::ShapeContainer lhs, rhs;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      irs::tests::FromJson(docs[7].geometry).value(), lhs, cache,
      sdb::geo::coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        irs::tests::FromJson(doc_entry.geometry).value(), rhs, cache,
        sdb::geo::coding::Options::Invalid, nullptr));
      const auto dist = lhs.distanceFromCentroid(rhs.centroid());
      if (dist <= 2000) {
        continue;
      }

      expected.emplace(doc_entry.name);
    }

    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Exclusive;
    range.min = 2000;
    range.max_type = irs::BoundType::Unbounded;
    range.max = 2000;

    ASSERT_EQ(expected, execute_query(q, {28, 28}));
  }

  {
    std::set<std::string> expected;
    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = 2000;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 100;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }

  {
    std::set<std::string> expected;
    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70461, 37.617053).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = 2000;
    range.max_type = irs::BoundType::Exclusive;
    range.max = 2000;

    ASSERT_EQ(expected, execute_query(q, {0, 0}));
  }
}

TEST(GeoDistanceFilterTest, checkScorer) {
  auto docs = irs::tests::ParseGeoDocs(R"([
    { "name": "A", "geometry": { "type": "Point", "coordinates": [ 37.615895, 55.7039   ] } },
    { "name": "B", "geometry": { "type": "Point", "coordinates": [ 37.615315, 55.703915 ] } },
    { "name": "C", "geometry": { "type": "Point", "coordinates": [ 37.61509, 55.703537  ] } },
    { "name": "D", "geometry": { "type": "Point", "coordinates": [ 37.614183, 55.703806 ] } },
    { "name": "E", "geometry": { "type": "Point", "coordinates": [ 37.613792, 55.704405 ] } },
    { "name": "F", "geometry": { "type": "Point", "coordinates": [ 37.614956, 55.704695 ] } },
    { "name": "G", "geometry": { "type": "Point", "coordinates": [ 37.616297, 55.704831 ] } },
    { "name": "H", "geometry": { "type": "Point", "coordinates": [ 37.617053, 55.70461  ] } },
    { "name": "I", "geometry": { "type": "Point", "coordinates": [ 37.61582, 55.704459  ] } },
    { "name": "J", "geometry": { "type": "Point", "coordinates": [ 37.614634, 55.704338 ] } },
    { "name": "K", "geometry": { "type": "Point", "coordinates": [ 37.613121, 55.704193 ] } },
    { "name": "L", "geometry": { "type": "Point", "coordinates": [ 37.614135, 55.703298 ] } },
    { "name": "M", "geometry": { "type": "Point", "coordinates": [ 37.613663, 55.704002 ] } },
    { "name": "N", "geometry": { "type": "Point", "coordinates": [ 37.616522, 55.704235 ] } },
    { "name": "O", "geometry": { "type": "Point", "coordinates": [ 37.615508, 55.704172 ] } },
    { "name": "P", "geometry": { "type": "Point", "coordinates": [ 37.614629, 55.704081 ] } },
    { "name": "Q", "geometry": { "type": "Point", "coordinates": [ 37.610235, 55.709754 ] } },
    { "name": "R", "geometry": { "type": "Point", "coordinates": [ 37.605,    55.707917 ] } },
    { "name": "S", "geometry": { "type": "Point", "coordinates": [ 37.545776, 55.722083 ] } },
    { "name": "T", "geometry": { "type": "Point", "coordinates": [ 37.559509, 55.715895 ] } },
    { "name": "U", "geometry": { "type": "Point", "coordinates": [ 37.701645, 55.832144 ] } },
    { "name": "V", "geometry": { "type": "Point", "coordinates": [ 37.73735,  55.816715 ] } },
    { "name": "W", "geometry": { "type": "Point", "coordinates": [ 37.75589,  55.798193 ] } },
    { "name": "X", "geometry": { "type": "Point", "coordinates": [ 37.659073, 55.843711 ] } },
    { "name": "Y", "geometry": { "type": "Point", "coordinates": [ 37.778549, 55.823659 ] } },
    { "name": "Z", "geometry": { "type": "Point", "coordinates": [ 37.729797, 55.853733 ] } },
    { "name": "1", "geometry": { "type": "Point", "coordinates": [ 37.608261, 55.784682 ] } },
    { "name": "2", "geometry": { "type": "Point", "coordinates": [ 37.525177, 55.802825 ] } }
  ])");

  irs::MemoryDirectory dir;
  irs::DirectoryReader reader;

  // index data
  {
    constexpr auto kFormatId = "1_5simd";
    auto codec = irs::formats::Get(kFormatId);
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    GeoField geo_field;
    geo_field.field_name = "geometry";
    geo_field.id = kGeo;
    StringField name_field;
    name_field.field_name = "name";
    name_field.id = kName;
    {
      auto segment0 = writer->GetBatch();
      auto segment1 = writer->GetBatch();
      {
        size_t i = 0;
        for (const auto& doc_entry : docs) {
          geo_field.value = doc_entry.geometry;
          name_field.value = doc_entry.name;

          auto doc = (i++ % 2 ? segment0 : segment1).Insert();
          ASSERT_TRUE(doc.Insert(name_field));
          ASSERT_TRUE(doc.Insert(geo_field));
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(),
                                   name_field);
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kGeo, doc.DocId(),
                                   geo_field);
        }
      }
      segment1.Commit();
      segment0.Commit();
    }
    writer->RefreshCommit();
    reader = writer->GetSnapshot();
  }

  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(2, reader->size());
  ASSERT_EQ(docs.size(), reader->docs_count());
  ASSERT_EQ(docs.size(), reader->live_docs_count());

  DocIterator* cur_it = nullptr;
  auto execute_query = [&](const irs::Filter& q, const irs::Scorer& ord) {
    std::map<std::string, irs::score_t> actual_results;

    ::tests::PreparedFilter prepared{q, *reader, &ord};
    for (size_t i = 0; auto& segment : *reader) {
      const auto* column = segment.Column(kName);
      EXPECT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto it = prepared.Execute(i);
      EXPECT_NE(nullptr, it);
      auto seek_it = prepared.Execute(i);
      EXPECT_NE(nullptr, seek_it);
      auto* cost = irs::get<irs::CostAttr>(*it);
      EXPECT_NE(nullptr, cost);

      if (irs::doc_limits::eof(it->value())) {
        ++i;
        continue;
      }

      const auto score = it->PrepareScore({
        .scorer = &ord,
        .segment = &segment,
      });
      EXPECT_FALSE(score.IsDefault());
      const auto& seek_score = seek_it->PrepareScore({
        .scorer = &ord,
        .segment = &segment,
      });
      EXPECT_FALSE(seek_score.IsDefault());

      EXPECT_FALSE(irs::doc_limits::valid(it->value()));

      cur_it = it.get();
      while (!irs::doc_limits::eof(it->advance())) {
        const auto doc_id = it->value();
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_FALSE(values.IsNull(doc_id));

        irs::score_t score_value;
        score.Score(&score_value, 1);
        irs::score_t seek_score_value;
        seek_score.Score(&seek_score_value, 1);

        EXPECT_EQ(score_value, seek_score_value);

        actual_results.emplace(
          irs::tests::ReadStoredStr<std::string>(values, doc_id),
          std::move(score_value));
      }
      EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      EXPECT_TRUE(irs::doc_limits::eof(seek_it->seek(it->value())));

      {
        auto it = prepared.Execute(i);
        EXPECT_NE(nullptr, it);

        while (!irs::doc_limits::eof(it->advance())) {
          const auto doc_id = it->value();
          auto seek_it = prepared.Execute(i);
          EXPECT_NE(nullptr, seek_it);
          EXPECT_EQ(doc_id, seek_it->seek(doc_id));
          do {
            if (!values.IsNull(seek_it->value())) {
              EXPECT_NE(
                actual_results.end(),
                actual_results.find(irs::tests::ReadStoredStr<std::string>(
                  values, seek_it->value())));
            }
          } while (!irs::doc_limits::eof(seek_it->advance()));
          EXPECT_TRUE(irs::doc_limits::eof(seek_it->value()));
        }
        EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      }
      ++i;
    }

    return actual_results;
  };

  {
    GeoDistanceFilter q;
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70892, 37.607768).ToPoint();
    auto& range = q.mutable_options()->range;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 300;

    size_t collector_finish_count = 0;
    uint64_t collector_field_docs = 0;
    size_t scorer_score_count = 0;
    size_t prepare_scorer_count = 0;

    ::CustomSort sort;

    sort.collector_finish = [&](irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // geo filter exercises field collector but not term collector
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(nullptr, term);
      collector_field_docs += field->docs_with_field;
    };
    sort._prepare_scorer = [&](const irs::ScoreContext& ctx) {
      EXPECT_EQ(q.Boost(), ctx.boost);
      ++prepare_scorer_count;
    };
    sort.scorer_score = [&](const irs::ScoreOperator* ctx, irs::score_t* res,
                            size_t n) {
      ASSERT_TRUE(res);
      ASSERT_TRUE(cur_it);
      ASSERT_EQ(1, n);
      ++scorer_score_count;
      *res = static_cast<float>(cur_it->value());
    };

    const std::map<std::string, irs::score_t> expected{{"Q", 9}, {"R", 9}};

    ASSERT_EQ(expected, execute_query(q, sort));
    ASSERT_EQ(1, collector_finish_count);
    ASSERT_GT(collector_field_docs, 0u);  // field collector ran on segments
    ASSERT_EQ(4, scorer_score_count);
  }

  {
    GeoDistanceFilter q;
    q.boost(1.5f);
    q.mutable_options()->store_field_id = kGeo;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70892, 37.607768).ToPoint();
    auto& range = q.mutable_options()->range;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 300;

    size_t collector_finish_count = 0;
    uint64_t collector_field_docs = 0;
    size_t scorer_score_count = 0;
    size_t prepare_scorer_count = 0;

    CustomSort sort;

    sort.collector_finish = [&](irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // geo filter exercises field collector but not term collector
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(nullptr, term);
      collector_field_docs += field->docs_with_field;
    };
    sort._prepare_scorer = [&](const irs::ScoreContext& ctx) {
      EXPECT_EQ(q.Boost(), ctx.boost);
      ++prepare_scorer_count;
    };
    sort.scorer_score = [&](const irs::ScoreOperator* ctx, irs::score_t* res,
                            size_t n) {
      ASSERT_TRUE(res);
      ASSERT_EQ(1, n);
      ++scorer_score_count;
      *res = static_cast<float>(9);
    };

    const std::map<std::string, irs::score_t> expected{{"Q", 9}, {"R", 9}};

    ASSERT_EQ(expected, execute_query(q, sort));
    ASSERT_EQ(1, collector_finish_count);
    ASSERT_GT(collector_field_docs, 0u);  // field collector ran on segments
    ASSERT_EQ(4, scorer_score_count);
  }
}
