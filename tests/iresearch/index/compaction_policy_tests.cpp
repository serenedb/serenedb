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

#include <iresearch/index/composite_reader_impl.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "tests_shared.hpp"

namespace {

class SubReaderMock final : public irs::SubReader {
 public:
  explicit SubReaderMock(const irs::SegmentInfo meta) : _meta{meta} {}

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  const SubReaderMock& operator*() const noexcept { return *this; }

  // Live & deleted docs

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  // Returns an iterator over live documents in current segment.
  irs::lead::Node::ptr docs_iterator() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Inverted index

  std::span<const irs::field_id> field_ids() const final {
    EXPECT_FALSE(true);
    return {};
  }

  // Returns corresponding term_reader by the specified field id.
  const irs::TermReader* field(irs::field_id) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  irs::NormReader::ptr norms(irs::field_id) const final {
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

TEST(CompactionPoliciesTest, SegmentsUnderCompactionAreNeverPicked) {
  irs::IndexMeta meta;
  AddSegment(meta, "busy", 10, 10, 1);
  AddSegment(meta, "empty", 10, 0, 1);
  AddSegment(meta, "full", 10, 10, 100);
  IndexReaderMock reader{meta};
  const irs::CompactingSegments compacting_segments{"busy"};
  const irs::CompactionPolicy policies[] = {
    irs::index_utils::MakePolicy(
      irs::index_utils::CompactionBytes{.threshold = 1}),
    irs::index_utils::MakePolicy(
      irs::index_utils::CompactionBytesAccum{.threshold = 1}),
    irs::index_utils::MakePolicy(
      irs::index_utils::CompactionDocsLive{.threshold = 0.1f}),
    irs::index_utils::MakePolicy(
      irs::index_utils::CompactionDocsFill{.threshold = 0.1f}),
  };
  for (size_t p = 0; p != std::size(policies); ++p) {
    irs::Compaction candidates;
    policies[p](candidates, reader, compacting_segments);
    EXPECT_FALSE(candidates.empty()) << "policy " << p;
    for (const auto* candidate : candidates) {
      EXPECT_NE("busy", candidate->Meta().name) << "policy " << p;
    }
  }
}

TEST(CompactionPoliciesTest, FullyDeletedSegmentsArePicked) {
  irs::IndexMeta meta;
  AddSegment(meta, "empty", 10, 0, 1);
  AddSegment(meta, "full", 10, 10, 100);
  IndexReaderMock reader{meta};
  const irs::CompactingSegments compacting_segments;
  const irs::CompactionPolicy policies[] = {
    irs::index_utils::MakePolicy(
      irs::index_utils::CompactionDocsLive{.threshold = 0.1f}),
    irs::index_utils::MakePolicy(
      irs::index_utils::CompactionDocsFill{.threshold = 0.1f}),
  };
  for (size_t p = 0; p != std::size(policies); ++p) {
    irs::Compaction candidates;
    policies[p](candidates, reader, compacting_segments);
    ASSERT_EQ(1U, candidates.size()) << "policy " << p;
    EXPECT_EQ("empty", candidates.front()->Meta().name) << "policy " << p;
  }
}
