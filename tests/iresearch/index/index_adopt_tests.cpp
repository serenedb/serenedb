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

#include <absl/algorithm/container.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

uint64_t IdOf(std::string_view name) {
  uint64_t id = 0;
  EXPECT_TRUE(irs::ParseSegmentId(name, id)) << name;
  return id;
}

class IndexAdoptTest : public TestBase {
 protected:
  void SetUp() override {
    TestBase::SetUp();
    _path = test_dir() / "adopt";
    std::filesystem::create_directories(_path);
    Open(tests::kOmCreate);
  }

  void TearDown() override {
    _writer.reset();
    _dir.reset();
    _impl.reset();
    TestBase::TearDown();
    std::filesystem::remove_all(_path);
  }

  void Open(tests::OpenMode mode, bool cleanup_on_open = true,
            uint32_t segment_docs_max = 0) {
    _impl = std::make_unique<irs::MMapDirectory>(
      _path, irs::DirectoryAttributes{}, GetResourceManager().options);
    _dir = std::make_unique<tests::DirectoryMock>(*_impl);
    auto options = tests::EnsureWriterDb(tests::CsDefaultWriterOptions());
    options.cleanup_on_open = cleanup_on_open;
    options.segment_docs_max = segment_docs_max;
    _writer = tests::IndexWriter::Make(*_dir, mode, std::move(options));
  }

  // Drops the Directory too, so no in-memory IndexFileRefs survive.
  void Restart(bool cleanup_on_open, uint32_t segment_docs_max = 0) {
    _writer.reset();
    _dir.reset();
    _impl.reset();
    Open(tests::kOmAppend | tests::kOmCreate, cleanup_on_open,
         segment_docs_max);
  }

  static constexpr irs::field_id kNameFieldId = tests::FieldIdFor("name");

  // field.id has to be set: StringField's ctor leaves it invalid and the writer
  // indexes by field id, so a removal by name would match nothing.
  static bool InsertDoc(tests::IndexWriter::Transaction& trx,
                        std::string_view value) {
    tests::StringField field{"name", value};
    field.id = kNameFieldId;
    return tests::InsertFields(trx.Insert(), &field, &field + 1);
  }

  bool Exists(std::string_view name) const {
    bool exists = false;
    return _dir->exists(exists, name) && exists;
  }

  static std::vector<std::string> FilesOf(
    std::span<const irs::IndexWriter::FlushedSegment> flushed) {
    std::vector<std::string> out;
    for (const auto& segment : flushed) {
      absl::c_move(irs::SegmentFiles(segment.meta.name, segment.meta.kinds),
                   std::back_inserter(out));
    }
    return out;
  }

  std::filesystem::path _path;
  std::unique_ptr<irs::MMapDirectory> _impl;
  std::unique_ptr<tests::DirectoryMock> _dir;
  tests::IndexWriter::ptr _writer;
};

TEST_F(IndexAdoptTest, FlushAndFsyncReportsEveryFlushedSegment) {
  Restart(/*cleanup_on_open=*/true, /*segment_docs_max=*/1);

  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(InsertDoc(trx, "value" + std::to_string(i)));
  }

  const auto flushed = trx.FlushAndFsync();
  // 3 docs at 1 doc per segment: two auto-flushes during Insert, plus the tail.
  ASSERT_EQ(3, flushed.size());
  size_t docs = 0;
  for (const auto& segment : flushed) {
    EXPECT_FALSE(segment.meta.name.empty());
    EXPECT_NE(0, segment.meta.kinds);
    docs += segment.meta.docs_count;
    for (const auto& file :
         irs::SegmentFiles(segment.meta.name, segment.meta.kinds)) {
      EXPECT_TRUE(Exists(file)) << file << " reported but absent";
    }
  }
  EXPECT_EQ(3, docs);
  trx.Abort();
}

TEST_F(IndexAdoptTest, ExclusiveSegmentDoesNotResumeAPooledOne) {
  {
    auto pooled = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(pooled, "pooled"));
    ASSERT_TRUE(pooled.Commit());
  }
  // That segment is back on the free-list, still holding `pooled` unflushed.
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "mine"));

  const auto flushed = trx.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  EXPECT_EQ(1, flushed.front().meta.docs_count) << "resumed a pooled segment";
  trx.Abort();
}

TEST_F(IndexAdoptTest, LaterTransactionReusingTheContextLeavesOurSegmentAlone) {
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "mine"));
    const auto flushed = trx.FlushAndFsync();
    ASSERT_EQ(1, flushed.size());
    ASSERT_TRUE(trx.Commit());
  }
  // Non-exclusive, so it pops the context we just released.
  {
    auto later = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(later, "theirs"));
    ASSERT_TRUE(later.Commit());
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  EXPECT_EQ(2, reader.live_docs_count());
  // Two separate segments: the later transaction could not append to ours.
  ASSERT_EQ(2, reader.size());
  for (const auto& segment : reader) {
    EXPECT_EQ(1, segment.docs_count())
      << "a segment absorbed both transactions";
  }
}

TEST_F(IndexAdoptTest, LaterTransactionAbortKeepsOurFlushedSegment) {
  std::vector<std::string> ours;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "mine"));
    const auto flushed = trx.FlushAndFsync();
    ASSERT_EQ(1, flushed.size());
    ours = FilesOf(flushed);
    ASSERT_TRUE(trx.Commit());
  }
  {
    auto later = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(later, "theirs"));
    later.Abort();
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  EXPECT_EQ(1, reader.live_docs_count()) << "our committed document was lost";
  for (const auto& file : ours) {
    EXPECT_TRUE(Exists(file)) << file << " was reclaimed";
  }
}

TEST_F(IndexAdoptTest, CleanupOnOpenDecidesUnreferencedSegmentSurvival) {
  std::vector<std::string> files;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "orphan"));
    files = FilesOf(trx.FlushAndFsync());
    trx.Abort();  // never committed, never published
  }
  ASSERT_FALSE(files.empty());

  Restart(/*cleanup_on_open=*/false);
  for (const auto& file : files) {
    EXPECT_TRUE(Exists(file)) << file << " was reclaimed despite the opt-out";
  }

  Restart(/*cleanup_on_open=*/true);
  for (const auto& file : files) {
    EXPECT_FALSE(Exists(file)) << file << " outlived the default cleanup";
  }
}

TEST_F(IndexAdoptTest, SegmentIdFlooredAboveKeptSegments) {
  std::vector<std::string> kept;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "orphan"));
    kept = FilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_FALSE(kept.empty());

  Restart(/*cleanup_on_open=*/false);
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "fresh"));
  for (const auto& file : FilesOf(trx.FlushAndFsync())) {
    EXPECT_EQ(kept.end(), std::find(kept.begin(), kept.end(), file))
      << file << " reused a kept segment's id";
  }
  trx.Abort();
}

// Eligibility, not promptness: reclaiming is the host's background cleanup, but
// a file still holding a ref is invisible to it.
TEST_F(IndexAdoptTest, AbortLeavesFlushedFilesUnreferenced) {
  std::vector<std::string> files;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "rolled back"));
    files = FilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_FALSE(files.empty());

  irs::directory_utils::RemoveAllUnreferenced(*_dir);
  for (const auto& file : files) {
    EXPECT_FALSE(Exists(file)) << file << " is still pinned after Abort";
  }
}

}  // namespace
namespace {

std::vector<std::string> CommittedNames(tests::IndexWriter& writer) {
  std::vector<std::string> out;
  for (const auto& segment : writer.GetSnapshot()) {
    out.emplace_back(segment.Meta().name);
  }
  std::ranges::sort(out);
  return out;
}

}  // namespace

// --- segment homogeneity ---------------------------------------------------

namespace {

// Two of these compare equal by content and differ only by address, which is
// the whole point of the test below.
class FixedFieldOptions final : public irs::IndexFieldOptions {
 public:
  irs::ColumnOptions GetColumnOptions(irs::field_id) const final { return {}; }
  irs::field_id GetNormColumnId(irs::field_id) const final {
    return irs::field_limits::invalid();
  }
};

}  // namespace

// A segment must never hold documents written under two different field
// configs. The search-table index backfill decides *which segments still lack a
// term field* by asking each segment what fields it carries
// (SubReader::field_ids), and that answer is only sound if a segment is
// homogeneous: a half-indexed segment would report the field present while
// missing postings for the documents written before the config changed, which
// is a silently partial index.
//
// What enforces it is the options gate in UpdateSegment, via
// CompatibleFieldOptions -> IndexFieldOptions::EqualOptions, whose default is
// pointer identity. So a freshly allocated config forces a cut, even in the
// middle of one transaction.
//
// IF THIS TEST FAILS, you have most likely given EqualOptions content-based
// semantics. That is defensible on its own terms: IndexFieldOptions models only
// *encodings* -- compression, IVF, hyperloglog, norms -- and says nothing about
// which fields are term-indexed, so two configs that differ only in their term
// fields legitimately compare equal. The fix is NOT to relax this test. Give
// sdb::search::MergedFieldOptions a monotonic config generation and override
// EqualOptions to compare that, so "equal options" keeps implying "same config"
// for the property the backfill depends on.
TEST_F(IndexAdoptTest, ADifferentFieldConfigCutsANewSegment) {
  Restart(/*cleanup_on_open=*/false);

  const auto first = std::make_shared<const FixedFieldOptions>();
  const auto second = std::make_shared<const FixedFieldOptions>();

  auto trx = _writer->GetBatch();
  trx.SetFieldOptions(first);
  ASSERT_TRUE(InsertDoc(trx, "under_first"));
  trx.SetFieldOptions(second);
  ASSERT_TRUE(InsertDoc(trx, "under_second"));
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(2, CommittedNames(*_writer).size())
    << "docs written under two configs shared one segment, so a segment's "
       "field list no longer describes every doc in it";
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

// The companion: the discriminator really is the config, not merely "two
// inserts". Reusing one config keeps them in a single segment, so the cut above
// is attributable to the options change.
TEST_F(IndexAdoptTest, TheSameFieldConfigKeepsOneSegment) {
  Restart(/*cleanup_on_open=*/false);

  const auto options = std::make_shared<const FixedFieldOptions>();

  auto trx = _writer->GetBatch();
  trx.SetFieldOptions(options);
  ASSERT_TRUE(InsertDoc(trx, "one"));
  trx.SetFieldOptions(options);
  ASSERT_TRUE(InsertDoc(trx, "two"));
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, CommittedNames(*_writer).size());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

// --- segment id as a config boundary ---------------------------------------

namespace {

// Distinguishable from FixedFieldOptions only by address, as there.
class SecondFieldOptions final : public irs::IndexFieldOptions {
 public:
  irs::ColumnOptions GetColumnOptions(irs::field_id) const final { return {}; }
  irs::field_id GetNormColumnId(irs::field_id) const final {
    return irs::field_limits::invalid();
  }
};

}  // namespace

// PROTOTYPE for search-table index-build detection.
//
// The build needs to know which segments were written before its new field
// config, and a segment's field list cannot answer that: merges union field
// sets, and a segment can lack a field merely because none of its documents had
// a value. The segment id counter can, because it is allocated when a segment
// is *acquired* rather than when it becomes visible.
//
// The case that matters is a transaction straddling the publish: it holds an
// uncommitted segment written under the old config -- invisible to every reader
// -- and then cuts a second one under the new config. This pins that the two
// land either side of the boundary read at publish time, so the old one is
// classifiable as stale before anyone can observe it.
TEST_F(IndexAdoptTest, SegmentIdSeparatesConfigsAcrossAStraddlingWrite) {
  Restart(/*cleanup_on_open=*/false);

  static constexpr irs::field_id kExtraFieldId = tests::FieldIdFor("extra");
  const auto before_config = std::make_shared<const FixedFieldOptions>();
  const auto after_config = std::make_shared<const SecondFieldOptions>();

  auto trx = _writer->GetBatch();
  trx.SetFieldOptions(before_config);
  ASSERT_TRUE(InsertDoc(trx, "under_old_config"));

  // The publish point. The segment above is already allocated but uncommitted,
  // so it is below the boundary while being invisible to any reader.
  const auto boundary = _writer->CurrentSegmentId();
  ASSERT_GT(boundary, 0);
  ASSERT_EQ(0, _writer->GetSnapshot().live_docs_count())
    << "the straddler's first segment must still be invisible here";

  trx.SetFieldOptions(after_config);
  {
    tests::StringField name{"name", "under_new_config"};
    name.id = kNameFieldId;
    tests::StringField extra{"extra", "term"};
    extra.id = kExtraFieldId;
    auto doc = trx.Insert();
    ASSERT_TRUE(tests::InsertFields(doc, &name, &name + 1));
    ASSERT_TRUE(tests::InsertFields(doc, &extra, &extra + 1));
  }
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  ASSERT_EQ(2, reader.size()) << "the config change must have cut a segment";
  ASSERT_EQ(2, reader.live_docs_count());

  size_t stale = 0;
  size_t fresh = 0;
  for (const auto& segment : reader) {
    const auto id = IdOf(segment.Meta().name);
    const bool has_extra = segment.field(kExtraFieldId) != nullptr;
    if (id <= boundary) {
      ++stale;
      EXPECT_FALSE(has_extra)
        << "a segment at or below the boundary carried the new config's field";
    } else {
      ++fresh;
      EXPECT_TRUE(has_extra)
        << "a segment above the boundary is missing the new config's field";
    }
  }
  EXPECT_EQ(1, stale);
  EXPECT_EQ(1, fresh);
}

// The other half of the boundary's contract, and the reason a build has to keep
// compaction off the segments below it: a merge's output takes a FRESH id
// (FileName(NextSegmentId())), so merging stale segments produces one that
// classifies as fresh while carrying documents that were never indexed for the
// new field. Detection alone cannot recover from that -- hence a floor, not
// just a per-segment reservation.
TEST_F(IndexAdoptTest, MergingStaleSegmentsProducesAFreshId) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  const auto boundary = _writer->CurrentSegmentId();
  for (const auto& segment : _writer->GetSnapshot()) {
    ASSERT_LE(IdOf(segment.Meta().name), boundary);
  }

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_GT(IdOf((*reader.begin()).Meta().name), boundary)
    << "if a merge kept an input's id, stale segments could be consolidated "
       "during a build and still classify correctly";
}

// --- the compaction floor --------------------------------------------------

TEST_F(IndexAdoptTest, CompactionFloorProtectsStaleAndFreesFresh) {
  Restart(/*cleanup_on_open=*/false);

  // Two segments below the boundary: what a build is going to rewrite.
  for (const auto* value : {"stale_a", "stale_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  auto guard = _writer->ClaimCompactionFloor();
  ASSERT_TRUE(guard.Arm());
  const auto floor = guard.Floor();
  ASSERT_GT(floor, 0);

  // Two more above it: what concurrent inserts produce during the build.
  for (const auto* value : {"fresh_a", "fresh_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_EQ(4, _writer->GetSnapshot().size());

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));
  ASSERT_TRUE(_writer->RefreshCommit());

  // The fresh pair merged; the stale pair is untouched. This is the point of a
  // floor over a blanket stop: a long build must not stop the index from
  // consolidating what is written during it.
  size_t stale = 0;
  size_t fresh = 0;
  for (const auto& segment : _writer->GetSnapshot()) {
    if (IdOf(segment.Meta().name) <= floor) {
      ++stale;
    } else {
      ++fresh;
    }
  }
  EXPECT_EQ(2, stale) << "segments below the floor were consolidated";
  EXPECT_EQ(1, fresh) << "segments above the floor were not consolidated";
  EXPECT_EQ(4, _writer->GetSnapshot().live_docs_count());

  // Released, the stale pair becomes eligible again.
  guard = {};
  ASSERT_TRUE(_writer->Compact(kFullMerge));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(4, _writer->GetSnapshot().live_docs_count());
}

namespace {

// Shaped like the tiered policy the search table actually runs during a build:
// scan every segment, skip the ones it is told are unavailable, stop when its
// budget is full. The budget is what makes this different from the full merge
// above -- a policy that takes everything cannot starve.
irs::CompactionPolicy BudgetedPolicy(size_t budget) {
  return [budget](irs::Compaction& candidates, const irs::IndexReader& reader,
                  const irs::CompactingSegments& unavailable) {
    for (const auto& segment : reader) {
      if (candidates.size() >= budget) {
        break;
      }
      if (unavailable.contains(segment.Meta().name)) {
        continue;
      }
      candidates.emplace_back(&segment);
    }
  };
}

}  // namespace

// The floor hides protected segments from the policy rather than filtering its
// choice afterwards. Filtering afterwards starves a budgeted policy: the
// segments below the floor come first in reader order, so the policy spends
// its whole budget on them, every candidate is vetoed, and the segments
// written during the build never consolidate however long it runs.
TEST_F(IndexAdoptTest, ABudgetedPolicySpendsItsBudgetAboveTheFloor) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"stale_a", "stale_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  auto guard = _writer->ClaimCompactionFloor();
  ASSERT_TRUE(guard.Arm());
  const auto floor = guard.Floor();

  for (const auto* value : {"fresh_a", "fresh_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  // The premise: the protected segments are the ones the policy reaches first,
  // so a budget of 2 is entirely consumed by them unless they are hidden.
  {
    auto snapshot = _writer->GetSnapshot();
    ASSERT_EQ(4, snapshot.size());
    ASSERT_LE(IdOf(snapshot[0].Meta().name), floor);
    ASSERT_LE(IdOf(snapshot[1].Meta().name), floor);
  }

  ASSERT_TRUE(_writer->Compact(BudgetedPolicy(2)));
  // Starvation shows up here first: with the protected segments merely vetoed
  // after selection there are no candidates left, so the compaction queues
  // nothing and there is nothing to publish.
  ASSERT_TRUE(_writer->RefreshCommit())
    << "nothing merged at all: the policy spent its budget below the floor";

  size_t stale = 0;
  size_t fresh = 0;
  for (const auto& segment : _writer->GetSnapshot()) {
    if (IdOf(segment.Meta().name) <= floor) {
      ++stale;
    } else {
      ++fresh;
    }
  }
  EXPECT_EQ(2, stale) << "segments below the floor were consolidated";
  EXPECT_EQ(1, fresh) << "the segments above the floor did not consolidate";
  EXPECT_EQ(4, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AClaimedFloorStopsNewMergesUntilTheRunningOnesFinish) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"running_a", "running_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  const irs::MergeWriter::FlushProgress progress = [] { return true; };
  irs::IndexWriter::CompactionFloorGuard guard;
  {
    auto running = irs::GetBlocking(_writer->MergeAsync(
      kFullMerge, _writer->GetSnapshot(), nullptr, progress, nullptr));
    ASSERT_TRUE(running.reader);

    guard = _writer->ClaimCompactionFloor();
    EXPECT_FALSE(guard.Arm()) << "armed under a merge that holds segments";

    for (const auto* value : {"later_a", "later_b"}) {
      auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
      ASSERT_TRUE(InsertDoc(trx, value));
      ASSERT_TRUE(trx.Commit());
      ASSERT_TRUE(_writer->RefreshCommit());
    }
    const auto later = irs::GetBlocking(_writer->MergeAsync(
      kFullMerge, _writer->GetSnapshot(), nullptr, progress, nullptr));
    EXPECT_FALSE(later.reader) << "a merge started while the floor was claimed";
    EXPECT_FALSE(guard.Arm());
  }

  ASSERT_TRUE(guard.Arm()) << "the floor did not arm once the merge finished";
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(4, snapshot.size());
  for (const auto& segment : snapshot) {
    EXPECT_LE(IdOf(segment.Meta().name), guard.Floor());
  }
}

TEST_F(IndexAdoptTest, CompactionFloorRefusesWhileAMergeIsRunning) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});

  // Arm from inside the merge's progress callback. By then the candidates are
  // registered and the output id has been minted, which is the state that must
  // refuse: a floor set now would sit below that output while the segments it
  // consumed sit below it too, so their contents would hide behind an id that
  // says "written later".
  bool attempted = false;
  bool held = true;
  const irs::MergeWriter::FlushProgress progress = [&] {
    if (!attempted) {
      attempted = true;
      held = _writer->ClaimCompactionFloor().Arm();
    }
    return true;
  };
  ASSERT_TRUE(_writer->Compact(kFullMerge, nullptr, progress));
  ASSERT_TRUE(attempted) << "the progress callback never ran";
  EXPECT_FALSE(held) << "armed a floor while a merge was in flight";

  ASSERT_TRUE(_writer->RefreshCommit());

  // Idle again, it arms -- so the refusal above was the in-flight merge.
  auto guard = _writer->ClaimCompactionFloor();
  EXPECT_TRUE(guard.Arm());
}
