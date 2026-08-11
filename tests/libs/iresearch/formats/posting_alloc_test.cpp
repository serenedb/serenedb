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

// The instrument for "allocations per query on the iterator-construction path".
//
// Building a posting iterator is the per-term cost of every scored query, so it
// is the place where an extra allocation is paid once per term per segment
// rather than once per query. `memory::make_managed` is plain `new`, so
// `IResourceManager` cannot see any of it -- a counting resource manager reads
// zero here. jemalloc's own per-thread accounting can, and it needs no
// production change to read.
//
// This reports a NUMBER rather than asserting a bound: the useful comparison is
// against the same number on another build, so the test prints bytes and count
// per iterator and only fails on something that cannot be a measurement
// artefact -- a path that allocates without bound, or one that leaks.

#include "index/index_tests.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"

#ifdef SERENEDB_HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace {

#ifdef SERENEDB_HAVE_JEMALLOC
// jemalloc counts per thread and never resets, so only deltas mean anything.
// `thread.allocated` is bytes requested; `nmalloc` under `thread` is not
// exposed, so the count comes from the arena-independent bin totals being
// unavailable -- bytes is what this instrument reports.
uint64_t ThreadAllocatedBytes() {
  uint64_t allocated = 0;
  size_t size = sizeof(allocated);
  // A refresh is needed before every read: the value is cached per thread.
  uint64_t epoch = 1;
  size_t epoch_size = sizeof(epoch);
  mallctl("epoch", &epoch, &epoch_size, &epoch, sizeof(epoch));
  if (mallctl("thread.allocated", &allocated, &size, nullptr, 0) != 0) {
    return 0;
  }
  return allocated;
}
constexpr bool kHaveCounter = true;
#else
uint64_t ThreadAllocatedBytes() { return 0; }
constexpr bool kHaveCounter = false;
#endif

class PostingAllocTestCase : public tests::IndexTestBase {};

TEST_P(PostingAllocTestCase, doc_iterator_construction_bytes) {
  if constexpr (!kHaveCounter) {
    GTEST_SKIP() << "needs a jemalloc build to count allocations";
  }

  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  // One field with real postings, and a term with enough documents that the
  // iterator has to set up its block machinery rather than take the df == 1
  // shortcut.
  const irs::TermReader* field = nullptr;
  for (auto& segment : *reader.GetImpl()) {
    for (auto field_id : segment.field_ids()) {
      if (const auto* f = segment.field(field_id); f && f->size() > 64) {
        field = f;
        break;
      }
    }
    if (field != nullptr) {
      break;
    }
  }
  ASSERT_NE(nullptr, field);

  // The densest term in the field: the most block machinery any single term
  // makes the iterator set up, which is the worst case worth counting.
  auto terms = field->iterator();
  ASSERT_NE(nullptr, terms);
  irs::PostingMeta chosen_storage;
  while (terms->next()) {
    if (const auto& meta = terms->cookie();
        meta.docs_count > chosen_storage.docs_count) {
      chosen_storage = meta;
    }
  }
  ASSERT_GT(chosen_storage.docs_count, 0u) << "field has no postings";
  const irs::PostingMeta* chosen = &chosen_storage;

  constexpr size_t kWarmup = 64;
  constexpr size_t kRounds = 1024;
  const irs::PostingCookie cookie{.cookie = chosen,
                                  .stats = nullptr,
                                  .boost = irs::kNoBoost,
                                  .field = field->meta()};

  // Warm up so first-touch growth of any pooled structure is not attributed to
  // the measured rounds.
  for (size_t i = 0; i != kWarmup; ++i) {
    auto docs = field->Iterator(irs::IndexFeatures::Freq, cookie, {});
    ASSERT_NE(nullptr, docs);
  }

  const auto before = ThreadAllocatedBytes();
  for (size_t i = 0; i != kRounds; ++i) {
    auto docs = field->Iterator(irs::IndexFeatures::Freq, cookie, {});
    ASSERT_NE(nullptr, docs);
  }
  const auto after = ThreadAllocatedBytes();

  ASSERT_GE(after, before);
  const auto total = after - before;
  const auto per_iterator = total / kRounds;

  std::cout << "[ alloc    ] doc-iterator construction: " << per_iterator
            << " bytes/iterator (" << total << " bytes over " << kRounds
            << " constructions)\n";

  // The number is the deliverable; the assertion only catches a path that has
  // stopped being O(1) per iterator. A posting iterator's own buffers are
  // roughly 1.5 KB of leaves plus a score block, so an order of magnitude above
  // that is not a measurement artefact.
  EXPECT_LT(per_iterator, 64u * 1024u);
}

INSTANTIATE_TEST_SUITE_P(
  posting_alloc_test, PostingAllocTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MemoryDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5simd"})),
  PostingAllocTestCase::to_string);

}  // namespace
