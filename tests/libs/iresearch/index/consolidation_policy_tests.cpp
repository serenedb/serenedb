////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/composite_reader_impl.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "tests_shared.hpp"

namespace {

[[maybe_unused]] void PrintConsolidation(
  const irs::IndexReader& reader, const irs::ConsolidationPolicy& policy) {
  struct LessT {
    bool operator()(const irs::SubReader* lhs,
                    const irs::SubReader* rhs) const {
      auto& lhs_meta = lhs->Meta();
      auto& rhs_meta = rhs->Meta();
      return lhs_meta.byte_size == rhs_meta.byte_size
               ? lhs_meta.name < rhs_meta.name
               : lhs_meta.byte_size < rhs_meta.byte_size;
    }
  };
  irs::Consolidation candidates;
  irs::ConsolidatingSegments consolidating_segments;

  size_t i = 0;
  while (true) {
    candidates.clear();
    policy(candidates, reader, consolidating_segments);

    if (candidates.empty()) {
      break;
    }

    std::set<const irs::SubReader*, LessT> sorted_candidates(
      candidates.begin(), candidates.end(), LessT());

    std::cerr << "Consolidation " << i++ << ": ";
    for (auto* segment : sorted_candidates) {
      auto& meta = segment->Meta();
      std::cerr << meta.byte_size << " ("
                << double_t(meta.live_docs_count) / meta.docs_count << "), ";
    }
    std::cerr << "\n";

    // register candidates for consolidation
    for (const auto* candidate : candidates) {
      consolidating_segments.emplace(candidate->Meta().name);
    }
  }
}

void AssertCandidates(const irs::IndexReader& reader,
                      const std::vector<size_t>& expected_candidates,
                      const irs::Consolidation& actual_candidates) {
  ASSERT_EQ(expected_candidates.size(), actual_candidates.size());

  for (const size_t expected_candidate_idx : expected_candidates) {
    const auto& expected_candidate = reader[expected_candidate_idx];
    ASSERT_NE(actual_candidates.end(),
              std::find(actual_candidates.begin(), actual_candidates.end(),
                        &expected_candidate));
  }
}

class SubReaderMock final : public irs::SubReader {
 public:
  explicit SubReaderMock(const irs::SegmentInfo meta) : _meta{meta} {}

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  const SubReaderMock& operator*() const noexcept { return *this; }

  // Live & deleted docs

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  // Returns an iterator over live documents in current segment.
  irs::DocIterator::ptr docs_iterator() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  irs::DocIterator::ptr mask(irs::DocIterator::ptr&& it) const final {
    EXPECT_FALSE(true);
    return std::move(it);
  }

  // Inverted index

  irs::FieldIterator::ptr fields() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Returns corresponding term_reader by the specified field name.
  const irs::TermReader* field(std::string_view) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Columnstore

  irs::ColumnIterator::ptr columns() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::ColumnReader* column(irs::field_id) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::ColumnReader* column(std::string_view) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::ColumnReader* sort() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

 private:
  irs::SegmentInfo _meta;
};

class IndexReaderMock final
  : public irs::CompositeReaderImpl<std::vector<SubReaderMock>> {
 public:
  explicit IndexReaderMock(const irs::IndexMeta& meta)
    : IndexReaderMock{Init{meta}} {}

 private:
  struct Init {
    explicit Init(const irs::IndexMeta& meta) {
      readers.reserve(meta.segments.size());

      for (const auto& segment : meta.segments) {
        readers.emplace_back(segment.meta);
        docs_count += segment.meta.docs_count;
        live_docs_count += segment.meta.live_docs_count;
      }
    }

    std::vector<SubReaderMock> readers;
    uint64_t docs_count{};
    uint64_t live_docs_count{};
  };

  explicit IndexReaderMock(Init&& init) noexcept
    : irs::CompositeReaderImpl<std::vector<SubReaderMock>>{
        std::move(init.readers), init.live_docs_count, init.docs_count} {}
};

void AddSegment(irs::IndexMeta& meta, std::string_view name,
                irs::doc_id_t docs_count, irs::doc_id_t live_docs_count,
                size_t size) {
  auto& segment = meta.segments.emplace_back().meta;
  segment.name = name;
  segment.docs_count = docs_count;
  segment.live_docs_count = live_docs_count;
  segment.byte_size = size;
}

}  // namespace

TEST(ConsolidationTierTest, MaxConsolidationSize) {
  irs::IndexMeta meta;
  for (size_t i = 0; i < 22; ++i) {
    AddSegment(meta, std::to_string(i), 1, 1, 1);
  }
  IndexReaderMock reader{meta};

  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = std::numeric_limits<size_t>::max();
    options.min_segments = 1;
    options.max_segments_bytes = 10;

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments_bytes, candidates.size());
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments_bytes, candidates.size());
    }

    // 3rd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(reader.size() - 2 * options.max_segments_bytes,
                candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // invalid options: max_segments_bytes == 0
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = std::numeric_limits<size_t>::max();
    options.min_segments = 1;
    options.max_segments_bytes = 0;

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // all segments are too big
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}

TEST(ConsolidationTierTest, EmptyMeta) {
  irs::IndexMeta meta;
  IndexReaderMock reader{meta};

  irs::index_utils::ConsolidateTier options;
  options.floor_segment_bytes = 1;
  options.max_segments = 10;
  options.min_segments = 1;
  options.max_segments_bytes = std::numeric_limits<size_t>::max();

  irs::ConsolidatingSegments consolidating_segments;
  auto policy = irs::index_utils::MakePolicy(options);
  irs::Consolidation candidates;
  policy(candidates, reader, consolidating_segments);
  ASSERT_TRUE(candidates.empty());
}

TEST(ConsolidationTierTest, EmptyConsolidatingSegment) {
  irs::IndexMeta meta;
  AddSegment(meta, "empty", 1, 0, 1);
  IndexReaderMock reader{meta};

  irs::index_utils::ConsolidateTier options;
  options.floor_segment_bytes = 1;
  options.max_segments = 10;
  options.min_segments = 1;
  options.max_segments_bytes = std::numeric_limits<size_t>::max();

  irs::ConsolidatingSegments consolidating_segments{reader[0].Meta().name};
  auto policy = irs::index_utils::MakePolicy(options);
  irs::Consolidation candidates;
  policy(candidates, reader, consolidating_segments);
  ASSERT_TRUE(candidates.empty());  // skip empty consolidating segments
}

TEST(ConsolidationTierTest, EmptySegment) {
  irs::IndexMeta meta;
  AddSegment(meta, "empty", 0, 0, 1);
  IndexReaderMock reader{meta};

  irs::index_utils::ConsolidateTier options;
  options.floor_segment_bytes = 1;
  options.max_segments = 10;
  options.min_segments = 1;
  options.max_segments_bytes = std::numeric_limits<size_t>::max();

  irs::ConsolidatingSegments consolidating_segments{reader[0].Meta().name};
  auto policy = irs::index_utils::MakePolicy(options);
  irs::Consolidation candidates;
  policy(candidates, reader, consolidating_segments);
  ASSERT_TRUE(candidates.empty());  // skip empty segments
}

TEST(ConsolidationTierTest, MaxConsolidationCount) {
  // generate meta
  irs::IndexMeta meta;
  for (size_t i = 0; i < 22; ++i) {
    AddSegment(meta, std::to_string(i), 1, 1, 1);
  }
  IndexReaderMock reader{meta};

  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = 10;
    options.min_segments = 1;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 3rd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(reader.size() - 2 * options.max_segments, candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // max_segments == std::numeric_limits<size_t>::max()
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = std::numeric_limits<size_t>::max();
    options.min_segments = 1;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(reader.size(), candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // invalid options: max_segments == 0
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = 0;
    options.min_segments = 1;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // invalid options: floor_segments_bytes == 0
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 0;
    options.max_segments = 10;
    options.min_segments = 3;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}

TEST(ConsolidationTierTest, MinConsolidationCount) {
  // generate meta
  irs::IndexMeta meta;
  for (size_t i = 0; i < 22; ++i) {
    AddSegment(meta, std::to_string(i), 1, 1, 1);
  }
  IndexReaderMock reader{meta};

  // min_segments == 3
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = 10;
    options.min_segments = 3;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // invalid options: min_segments == 1
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = 10;
    options.min_segments = 0;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 3rd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(reader.size() - 2 * options.max_segments, candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // invalid options: min_segments > max_segments
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = 10;
    options.min_segments = std::numeric_limits<size_t>::max();
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(options.max_segments, candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // invalid options: min_segments > max_segments
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 1;
    options.max_segments = std::numeric_limits<size_t>::max();
    options.min_segments = std::numeric_limits<size_t>::max();
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // can't find anything
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}

TEST(ConsolidationTierTest, ConsolidationFloor) {
  // generate meta
  irs::IndexMeta meta;
  {
    size_t i = 0;
    for (; i < 5; ++i) {
      AddSegment(meta, std::to_string(i), 1, 1, 2 * i);
    }
    for (; i < 22; ++i) {
      AddSegment(meta, std::to_string(i), 1, 1, 2 * i);
    }
  }
  IndexReaderMock reader{meta};

  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = 8;
    options.max_segments = reader.size();
    options.min_segments = 1;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(5, candidates.size());

      for (size_t i = 0; i < candidates.size(); ++i) {
        ASSERT_NE(candidates.end(),
                  std::find(candidates.begin(), candidates.end(), &reader[i]));
      }
    }

    // 2nd tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(reader.size() - 5, candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // enormous floor value, treat all segments as equal
  {
    irs::index_utils::ConsolidateTier options;
    options.floor_segment_bytes = std::numeric_limits<uint32_t>::max();
    options.max_segments = std::numeric_limits<size_t>::max();
    options.min_segments = 1;
    options.max_segments_bytes = std::numeric_limits<size_t>::max();

    irs::ConsolidatingSegments consolidating_segments;
    auto policy = irs::index_utils::MakePolicy(options);

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
      ASSERT_EQ(reader.size(), candidates.size());
    }

    // last empty tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}

TEST(ConsolidationTierTest, PreferSegmentsWithRemovals) {
  // generate meta
  irs::IndexMeta meta;
  AddSegment(meta, "0", 10, 10, 10);
  AddSegment(meta, "1", 10, 10, 10);
  AddSegment(meta, "2", 11, 10, 11);
  AddSegment(meta, "3", 11, 10, 11);
  IndexReaderMock reader{meta};

  // ensure policy prefers segments with removals
  irs::index_utils::ConsolidateTier options;
  options.floor_segment_bytes = 1;
  options.max_segments = 2;
  options.min_segments = 1;
  options.max_segments_bytes = std::numeric_limits<size_t>::max();

  irs::ConsolidatingSegments consolidating_segments;
  auto policy = irs::index_utils::MakePolicy(options);

  const std::vector<std::vector<size_t>> expected_tiers{{2, 3}, {0, 1}};

  for (auto& expected_tier : expected_tiers) {
    irs::Consolidation candidates;
    policy(candidates, reader, consolidating_segments);
    AssertCandidates(reader, expected_tier, candidates);
    candidates.clear();
    policy(candidates, reader, consolidating_segments);
    AssertCandidates(reader, expected_tier, candidates);
    // register candidates for consolidation
    for (const auto* candidate : candidates) {
      consolidating_segments.emplace(candidate->Meta().name);
    }
  }

  // no more segments to consolidate
  {
    irs::Consolidation candidates;
    policy(candidates, reader, consolidating_segments);
    ASSERT_TRUE(candidates.empty());
  }
}

TEST(ConsolidationTierTest, Singleton) {
  irs::index_utils::ConsolidateTier options;
  options.floor_segment_bytes = 1;
  options.max_segments = std::numeric_limits<size_t>::max();
  options.min_segments = 1;
  options.max_segments_bytes = std::numeric_limits<size_t>::max();
  auto policy = irs::index_utils::MakePolicy(options);

  // singleton consolidation without removals
  {
    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 150);
    IndexReaderMock reader{meta};

    // avoid having singletone merges without removals
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // singleton consolidation with removals
  {
    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 99, 150);
    IndexReaderMock reader{meta};

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, {0}, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, {0}, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}

TEST(ConsolidationTierTest, Defaults) {
  irs::index_utils::ConsolidateTier options;
  auto policy = irs::index_utils::MakePolicy(options);

  {
    irs::ConsolidatingSegments consolidating_segments;

    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 150);
    AddSegment(meta, "1", 100, 100, 100);
    AddSegment(meta, "2", 100, 100, 100);
    AddSegment(meta, "3", 100, 100, 100);
    AddSegment(meta, "4", 100, 100, 100);
    IndexReaderMock reader{meta};

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, {0, 1, 2, 3, 4}, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, {0, 1, 2, 3, 4}, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 150);
    AddSegment(meta, "1", 100, 100, 100);
    AddSegment(meta, "2", 100, 100, 100);
    AddSegment(meta, "3", 100, 100, 100);
    AddSegment(meta, "4", 100, 100, 100);
    AddSegment(meta, "5", 100, 100, 100);
    AddSegment(meta, "6", 100, 100, 100);
    AddSegment(meta, "7", 100, 100, 100);
    AddSegment(meta, "8", 100, 100, 100);
    AddSegment(meta, "9", 100, 100, 100);
    AddSegment(meta, "10", 100, 100, 100);
    IndexReaderMock reader{meta};

    // 1st tier
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}

TEST(ConsolidationTierTest, NoCandidates) {
  irs::index_utils::ConsolidateTier options;
  options.floor_segment_bytes = 2097152;
  options.max_segments_bytes = 4294967296;
  options.min_segments = 5;  // min number of segments per tier to merge at once
  // max number of segments per tier to merge at once
  options.max_segments = 10;
  auto policy = irs::index_utils::MakePolicy(options);

  irs::ConsolidatingSegments consolidating_segments;
  irs::IndexMeta meta;
  AddSegment(meta, "0", 100, 100, 141747);
  AddSegment(meta, "1", 100, 100, 1548373791);
  AddSegment(meta, "2", 100, 100, 1699787770);
  AddSegment(meta, "3", 100, 100, 1861963739);
  AddSegment(meta, "4", 100, 100, 2013404723);
  IndexReaderMock reader{meta};

  irs::Consolidation candidates;
  policy(candidates, reader, consolidating_segments);
  ASSERT_TRUE(candidates.empty());  // candidates too large
}

TEST(ConsolidationTierTest, SkewedSegments) {
  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 2500;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 10);
    AddSegment(meta, "1", 100, 100, 40);
    AddSegment(meta, "2", 100, 100, 60);
    AddSegment(meta, "3", 100, 100, 70);
    AddSegment(meta, "4", 100, 100, 100);
    AddSegment(meta, "5", 100, 100, 150);
    AddSegment(meta, "6", 100, 100, 200);
    AddSegment(meta, "7", 100, 100, 500);
    AddSegment(meta, "8", 100, 100, 750);
    AddSegment(meta, "9", 100, 100, 1100);
    AddSegment(meta, "10", 100, 100, 90);
    AddSegment(meta, "11", 100, 100, 75);
    AddSegment(meta, "12", 100, 100, 1500);
    AddSegment(meta, "13", 100, 100, 10000);
    AddSegment(meta, "14", 100, 100, 5000);
    AddSegment(meta, "15", 100, 100, 1750);
    AddSegment(meta, "16", 100, 100, 690);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 1, 2, 3, 4, 10, 11},
      {5, 6},
      {7, 8, 16},
    };

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 10);
    AddSegment(meta, "1", 100, 100, 100);
    AddSegment(meta, "2", 100, 100, 500);
    AddSegment(meta, "3", 100, 100, 1000);
    AddSegment(meta, "4", 100, 100, 2000);
    AddSegment(meta, "5", 100, 100, 4000);
    AddSegment(meta, "6", 100, 100, 12000);
    AddSegment(meta, "7", 100, 100, 30000);
    AddSegment(meta, "8", 100, 100, 50000);
    AddSegment(meta, "9", 100, 100, 100000);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 1},
      {2, 3},
      {4, 5},
      {6, 7, 8},
    };

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 2;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 10);
    AddSegment(meta, "1", 100, 100, 100);
    AddSegment(meta, "2", 100, 100, 500);
    AddSegment(meta, "3", 100, 100, 1000);
    AddSegment(meta, "4", 100, 100, 2000);
    AddSegment(meta, "5", 100, 100, 4000);
    AddSegment(meta, "6", 100, 100, 12000);
    AddSegment(meta, "7", 100, 100, 30000);
    AddSegment(meta, "8", 100, 100, 50000);
    AddSegment(meta, "9", 100, 100, 100000);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}};

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 3;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 10);
    AddSegment(meta, "1", 100, 100, 100);
    AddSegment(meta, "2", 100, 100, 500);
    AddSegment(meta, "3", 100, 100, 1000);
    AddSegment(meta, "4", 100, 100, 2000);
    AddSegment(meta, "5", 100, 100, 4000);
    AddSegment(meta, "6", 100, 100, 12000);
    AddSegment(meta, "7", 100, 100, 30000);
    AddSegment(meta, "8", 100, 100, 50000);
    AddSegment(meta, "9", 100, 100, 100000);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {2, 3, 4}, {6, 7, 8}
      // no more candidates since 10, 100, 4000, 100000 means exponensial grow
    };

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 10);
    AddSegment(meta, "1", 100, 100, 100);
    AddSegment(meta, "2", 100, 100, 500);
    AddSegment(meta, "3", 100, 100, 1000);
    AddSegment(meta, "4", 100, 100, 2000);
    AddSegment(meta, "5", 100, 100, 4000);
    AddSegment(meta, "6", 100, 100, 12000);
    AddSegment(meta, "7", 100, 100, 30000);
    AddSegment(meta, "8", 100, 100, 50000);
    AddSegment(meta, "9", 100, 100, 100000);
    AddSegment(meta, "10", 100, 100, 51);
    AddSegment(meta, "11", 100, 100, 151);
    AddSegment(meta, "12", 100, 100, 637);
    AddSegment(meta, "13", 100, 100, 351);
    AddSegment(meta, "14", 100, 100, 2351);
    AddSegment(meta, "15", 100, 100, 1351);
    AddSegment(meta, "16", 100, 100, 1351);
    AddSegment(meta, "17", 100, 100, 20);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 10, 17}, {1, 11}, {2, 3, 12, 13, 15, 16}, {4, 14}, {5, 6}, {7, 8},
    };

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 1;
    options.min_score = 0;  // default min score
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 1);
    AddSegment(meta, "1", 100, 100, 9886);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{{0, 1}};

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }
    ASSERT_EQ(reader.size(), consolidating_segments.size());

    // no segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 1;
    options.min_score = 0.001;  // filter out irrelevant merges
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 1);
    AddSegment(meta, "1", 100, 100, 9886);
    IndexReaderMock reader{meta};

    // no segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 1;
    options.min_score = 0;  // default min score
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 1);
    AddSegment(meta, "1", 100, 100, 9886);
    AddSegment(meta, "2", 100, 100, 2);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 2},
    };

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    options.max_segments_bytes = 250000;  // max size of the merge
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 1;
    options.min_score = 0.001;  // filter our irrelevant merges
    auto policy = irs::index_utils::MakePolicy(options);

    irs::ConsolidatingSegments consolidating_segments;
    irs::IndexMeta meta;
    AddSegment(meta, "0", 100, 100, 1);
    AddSegment(meta, "1", 100, 100, 9886);
    AddSegment(meta, "2", 100, 100, 2);
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 2},
    };

    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }

    // no segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    // max size of the merge
    options.max_segments_bytes = std::numeric_limits<size_t>::max();
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::IndexMeta meta;
    {
      constexpr size_t kSizes[] = {
        90,    100,  110,    95,     105,   150,    145,   155,   160,
        165,   1000, 900,    1100,   1150,  950,    10000, 10100, 9900,
        10250, 9800, 110000, 110100, 19900, 110250, 19800};

      for (auto begin = std::begin(kSizes), end = std::end(kSizes);
           begin != end; ++begin) {
        const auto i = std::distance(begin, end);
        AddSegment(meta, std::to_string(i), 100, 100, *begin);
      }
    }
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
      {10, 11, 12, 13, 14},
      {15, 16, 17, 18, 19},
      {22, 24},
      {20, 21, 23},
    };

    irs::ConsolidatingSegments consolidating_segments;
    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }
    ASSERT_EQ(reader.size(), consolidating_segments.size());

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  // enusre policy honors removals
  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    // max size of the merge
    options.max_segments_bytes = std::numeric_limits<size_t>::max();
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::IndexMeta meta;
    {
      constexpr size_t kSizes[] = {
        90,    100,  110,    95,     105,   150,    145,   155,   160,
        165,   1000, 900,    1100,   1150,  950,    10000, 10100, 9900,
        10250, 9800, 110000, 110100, 19900, 110250, 19800,
      };

      for (auto begin = std::begin(kSizes), end = std::end(kSizes);
           begin != end; ++begin) {
        const auto i = std::distance(begin, end);
        AddSegment(meta, std::to_string(i), 100, 100, *begin);
      }

      const_cast<irs::SegmentMeta&>(meta.segments[10].meta).live_docs_count = 1;
    }
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 10},          {1, 2, 3, 4, 5, 6, 7, 8, 9},
      {11, 12, 13, 14}, {15, 16, 17, 18, 19},
      {22, 24},         {20, 21, 23},
    };

    irs::ConsolidatingSegments consolidating_segments;
    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }
    ASSERT_EQ(reader.size(), consolidating_segments.size());

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }

  {
    irs::index_utils::ConsolidateTier options;
    // min number of segments per tier to merge at once
    options.min_segments = 1;
    // max number of segments per tier to merge at once
    options.max_segments = 10;
    // max size of the merge
    options.max_segments_bytes = std::numeric_limits<size_t>::max();
    // smaller segments will be treated as equal to this value
    options.floor_segment_bytes = 50;
    auto policy = irs::index_utils::MakePolicy(options);

    irs::IndexMeta meta;
    {
      constexpr size_t kSizes[] = {
        90,    100,  110,    95,     105,   150,    145,   155,   160,
        165,   1000, 900,    1100,   1150,  950,    10000, 10100, 9900,
        10250, 9800, 110000, 110100, 19900, 110250, 19800,
      };

      for (auto begin = std::begin(kSizes), end = std::end(kSizes);
           begin != end; ++begin) {
        const auto i = std::distance(begin, end);
        AddSegment(meta, std::to_string(i), 100, 100, *begin);
      }

      const_cast<irs::SegmentMeta&>(meta.segments[10].meta).live_docs_count = 1;
    }
    IndexReaderMock reader{meta};

    const std::vector<std::vector<size_t>> expected_tiers{
      {0, 10},          {1, 2, 3, 4, 5, 6, 7, 8, 9},
      {11, 12, 13, 14}, {15, 16, 17, 18, 19},
      {22, 24},         {20, 21, 23},
    };

    irs::ConsolidatingSegments consolidating_segments;
    for (auto& expected_tier : expected_tiers) {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      candidates.clear();
      policy(candidates, reader, consolidating_segments);
      AssertCandidates(reader, expected_tier, candidates);
      // register candidates for consolidation
      for (const auto* candidate : candidates) {
        consolidating_segments.emplace(candidate->Meta().name);
      }
    }
    ASSERT_EQ(reader.size(), consolidating_segments.size());

    // no more segments to consolidate
    {
      irs::Consolidation candidates;
      policy(candidates, reader, consolidating_segments);
      ASSERT_TRUE(candidates.empty());
    }
  }
}
