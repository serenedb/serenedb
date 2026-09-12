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

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "basics/duckdb_engine.h"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "tests_shared.hpp"

namespace {

// Counts how many times each file is created, so a test can pin "written once".
class CountingDirectory : public tests::DirectoryMock {
 public:
  explicit CountingDirectory(irs::Directory& impl)
    : tests::DirectoryMock{impl} {}

  irs::IndexOutput::ptr create(std::string_view name) noexcept final {
    auto out = tests::DirectoryMock::create(name);
    if (out) {
      ++_creates[std::string{name}];
    }
    return out;
  }

  size_t Creates(std::string_view name) const {
    const auto it = _creates.find(std::string{name});
    return it == _creates.end() ? 0 : it->second;
  }

 private:
  std::map<std::string, size_t> _creates;
};

class IndexAdoptTest : public TestBase {
 protected:
  void SetUp() override {
    TestBase::SetUp();
    _path = test_dir() / "adopt";
    std::filesystem::create_directories(_path);
    _codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, _codec);
    Open(irs::kOmCreate);
  }

  void TearDown() override {
    _writer.reset();
    _dir.reset();
    _impl.reset();
    TestBase::TearDown();
    std::filesystem::remove_all(_path);
  }

  void Open(irs::OpenMode mode, bool cleanup_on_open = true,
            uint32_t segment_docs_max = 0) {
    _impl = std::make_unique<irs::MMapDirectory>(
      _path, irs::DirectoryAttributes{}, GetResourceManager().options);
    _dir = std::make_unique<CountingDirectory>(*_impl);
    auto options = tests::EnsureWriterDb(tests::CsDefaultWriterOptions());
    options.cleanup_on_open = cleanup_on_open;
    options.segment_docs_max = segment_docs_max;
    _writer = irs::IndexWriter::Make(*_dir, _codec, mode, options);
  }

  // Drops the Directory too, so no in-memory IndexFileRefs survive.
  void Restart(bool cleanup_on_open, uint32_t segment_docs_max = 0) {
    _writer.reset();
    _dir.reset();
    _impl.reset();
    Open(irs::kOmAppend | irs::kOmCreate, cleanup_on_open, segment_docs_max);
  }

  static constexpr irs::field_id kNameFieldId = tests::FieldIdFor("name");

  // field.id has to be set: StringField's ctor leaves it invalid and the writer
  // indexes by field id, so a ByTerm filter would match nothing.
  static bool InsertDoc(irs::IndexWriter::Transaction& trx,
                        std::string_view value) {
    tests::StringField field{"name", value};
    field.id = kNameFieldId;
    return tests::InsertFields(trx.Insert(), &field, &field + 1);
  }

  static irs::Filter::ptr ByName(std::string_view value) {
    auto by_term = std::make_unique<irs::ByTerm>();
    *by_term->mutable_field_id() = kNameFieldId;
    by_term->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
    return by_term;
  }

  bool Exists(std::string_view name) const {
    bool exists = false;
    return _dir->exists(exists, name) && exists;
  }

  static std::vector<std::string> MetaFilesOf(
    std::span<const irs::IndexWriter::FlushedSegment> flushed) {
    std::vector<std::string> out;
    out.reserve(flushed.size());
    for (const auto& segment : flushed) {
      out.emplace_back(segment.filename);
    }
    return out;
  }

  static std::vector<std::string> FilesOf(
    std::span<const irs::IndexWriter::FlushedSegment> flushed) {
    std::vector<std::string> out;
    for (const auto& segment : flushed) {
      out.insert(out.end(), segment.meta.files.begin(),
                 segment.meta.files.end());
    }
    return out;
  }

  std::filesystem::path _path;
  irs::Format::ptr _codec;
  std::unique_ptr<irs::MMapDirectory> _impl;
  std::unique_ptr<CountingDirectory> _dir;
  irs::IndexWriter::ptr _writer;
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
    EXPECT_FALSE(segment.meta.files.empty());
    docs += segment.meta.docs_count;
    for (const auto& file : segment.meta.files) {
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
  std::string our_meta;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "mine"));
    const auto flushed = trx.FlushAndFsync();
    ASSERT_EQ(1, flushed.size());
    our_meta = flushed.front().filename;
    ASSERT_TRUE(trx.Commit(/*last_tick=*/1));
  }
  // Non-exclusive, so it pops the context we just released.
  {
    auto later = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(later, "theirs"));
    ASSERT_TRUE(later.Commit(/*last_tick=*/2));
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
  // And ours was still not rewritten at publish.
  EXPECT_EQ(1, _dir->Creates(our_meta));
}

// SegmentContext::Rollback must cut only the tail past committed_flushed_docs.
TEST_F(IndexAdoptTest, LaterTransactionAbortKeepsOurFlushedSegment) {
  std::string our_meta;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "mine"));
    const auto flushed = trx.FlushAndFsync();
    ASSERT_EQ(1, flushed.size());
    our_meta = flushed.front().filename;
    ASSERT_TRUE(trx.Commit(/*last_tick=*/1));
  }
  {
    auto later = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(later, "theirs"));
    later.Abort();
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  EXPECT_EQ(1, reader.live_docs_count()) << "our committed document was lost";
  EXPECT_TRUE(Exists(our_meta)) << our_meta << " was reclaimed";
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

TEST_F(IndexAdoptTest, AdoptSegmentRepublishesFlushedRows) {
  std::vector<std::string> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept0"));
    ASSERT_TRUE(InsertDoc(trx, "kept1"));
    adopt = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();  // the publish never happened
  }
  ASSERT_EQ(1, adopt.size());

  Restart(/*cleanup_on_open=*/false);
  ASSERT_EQ(0, _writer->GetSnapshot().live_docs_count());

  for (const auto& meta_file : adopt) {
    ASSERT_TRUE(_writer->AdoptSegment(meta_file, _codec, /*tick=*/7));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());

  // And they are genuinely durable now, not just live in this writer.
  Restart(/*cleanup_on_open=*/true);
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AdoptSegmentTickOrdersAgainstRemoval) {
  std::vector<std::string> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    adopt = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }

  Restart(/*cleanup_on_open=*/false);
  for (const auto& meta_file : adopt) {
    ASSERT_TRUE(_writer->AdoptSegment(meta_file, _codec, /*tick=*/10));
  }
  {
    // Removal below the adopted tick: the segment is newer, so it survives.
    auto trx = _writer->GetBatch();
    trx.Remove(ByName("kept"));
    ASSERT_TRUE(trx.Commit(/*last_tick=*/5));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
}

// A removal masks an adopted segment iff `segment tick <= removal tick`.
TEST_F(IndexAdoptTest, AdoptTickDecidesRemovalMasking) {
  std::vector<std::string> before;
  std::vector<std::string> after;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "target"));
    before = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "target"));
    after = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_EQ(1, before.size());
  ASSERT_EQ(1, after.size());
  const std::string survivor_meta = after.front();
  ASSERT_NE(survivor_meta, before.front());

  Restart(/*cleanup_on_open=*/false);
  // One removal at tick 20; one segment below it, one above.
  ASSERT_TRUE(_writer->AdoptSegment(before.front(), _codec, /*tick=*/19));
  ASSERT_TRUE(_writer->AdoptSegment(after.front(), _codec, /*tick=*/21));
  {
    auto trx = _writer->GetBatch();
    trx.Remove(ByName("target"));
    ASSERT_TRUE(trx.Commit(/*last_tick=*/20));
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  // Masking its only document drops the segment at 19 outright.
  auto reader = _writer->GetSnapshot();
  EXPECT_EQ(1, reader.live_docs_count());
  ASSERT_EQ(1, reader.size());
  // `_N.V.sm` -> `_N`: the survivor is the one adopted above the removal.
  EXPECT_TRUE(survivor_meta.starts_with(reader.begin()->Meta().name + "."))
    << survivor_meta << " vs " << reader.begin()->Meta().name;
}

// Replay of DELETE x, SEGMENT re-inserting x, DELETE y: `x` must survive. The
// record tick (20) would sit below both rebased removals and delete it again;
// `commit_tick - queries + removals_before` lands in manifest order.
TEST_F(IndexAdoptTest, AdoptTickFollowsManifestPositionNotRecordTick) {
  std::vector<std::string> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "x"));
    adopt = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_EQ(1, adopt.size());

  Restart(/*cleanup_on_open=*/false);

  // One replay transaction, ops in manifest order, exactly as recovery does.
  constexpr uint64_t kMaxTick = 30;
  auto trx = _writer->GetBatch();
  uint64_t queries_before_segment = 0;
  trx.Remove(ByName("x"));  // record 1
  queries_before_segment = trx.GetQueries();
  // record 2: the segment is only stashed here; its tick needs the final query
  // count, so the adopt happens after the sweep.
  trx.Remove(ByName("y"));  // record 3

  const uint64_t queries = trx.GetQueries();
  ASSERT_EQ(2, queries);
  ASSERT_EQ(1, queries_before_segment);
  const uint64_t first_tick = kMaxTick - queries;
  ASSERT_TRUE(_writer->AdoptSegment(adopt.front(), _codec,
                                    first_tick + queries_before_segment));
  ASSERT_TRUE(trx.Commit(kMaxTick));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the re-inserted document was masked by the delete that preceded it";
}

// Existing writers keep writing it at publish: nothing sets `meta_on_disk`
// unless FlushAndFsync ran.
TEST_F(IndexAdoptTest, EarlyFlushedMetaIsWrittenOnce) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "kept"));
  const auto flushed = trx.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  const std::string meta_file = flushed.front().filename;
  ASSERT_FALSE(meta_file.empty());
  EXPECT_EQ(1, _dir->Creates(meta_file)) << "FlushAndFsync wrote it";

  ASSERT_TRUE(trx.Commit(/*last_tick=*/1));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());

  EXPECT_EQ(1, _dir->Creates(meta_file))
    << "the publish rewrote a meta that was already on disk unchanged";
}

// The skip above must not swallow this case: the meta genuinely changed.
TEST_F(IndexAdoptTest, MetaIsRewrittenWhenARemovalMasksTheSegment) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "doomed"));
  ASSERT_TRUE(InsertDoc(trx, "kept"));
  const auto flushed = trx.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  const std::string early_meta = flushed.front().filename;
  ASSERT_TRUE(trx.Commit(/*last_tick=*/1));

  {
    auto remover = _writer->GetBatch();
    remover.Remove(ByName("doomed"));
    ASSERT_TRUE(remover.Commit(/*last_tick=*/2));
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  // The early file is still the one FlushAndFsync made; the masked meta went to
  // a bumped version, so the publish did write.
  EXPECT_EQ(1, _dir->Creates(early_meta));
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  const auto& published = _writer->GetSnapshot().begin()->Meta();
  EXPECT_GT(published.version, 0u) << "a masked segment must bump its version";
}

// The meta file needs its own ref: the segment reader pins meta.files only, and
// unlike Import adoption does not create the file.
TEST_F(IndexAdoptTest, AdoptedSegmentSurvivesCleanupBeforePublish) {
  std::vector<std::string> adopt;
  std::vector<std::string> data_files;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    const auto flushed = trx.FlushAndFsync();
    adopt = MetaFilesOf(flushed);
    data_files = FilesOf(flushed);
    trx.Abort();
  }
  ASSERT_EQ(1, adopt.size());
  ASSERT_FALSE(data_files.empty());

  Restart(/*cleanup_on_open=*/false);
  ASSERT_TRUE(_writer->AdoptSegment(adopt.front(), _codec, /*tick=*/7));

  // Before the commit that publishes it.
  irs::directory_utils::RemoveAllUnreferenced(*_dir);
  EXPECT_TRUE(Exists(adopt.front()))
    << adopt.front() << " (meta file) was reclaimed before publish";
  for (const auto& file : data_files) {
    EXPECT_TRUE(Exists(file)) << file << " was reclaimed before publish";
  }

  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
}

// Only a null codec is rejected: one that resolves but differs from this
// writer's is legal, since segments carry their own as in the index meta.
TEST_F(IndexAdoptTest, AdoptSegmentRejectsUnresolvableCodec) {
  std::vector<std::string> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    adopt = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_EQ(1, adopt.size());

  Restart(/*cleanup_on_open=*/false);
  // What formats::Get hands back for a name this build no longer knows.
  EXPECT_FALSE(_writer->AdoptSegment(adopt.front(), nullptr, /*tick=*/1));

  _writer->RefreshCommit();
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count())
    << "a rejected segment was published anyway";

  // The rejection is local to that segment -- the writer stays usable.
  auto trx = _writer->GetBatch();
  ASSERT_TRUE(InsertDoc(trx, "after"));
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
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

// --- ReplaceSegments -------------------------------------------------------
//
// The host writes replacement segments itself (FlushAndFsync) and then swaps
// them in for the ones they supersede. Both halves have to land in one index
// meta generation, or a reader sees the sources without the replacements (rows
// vanish) or both (rows duplicated).

namespace {

std::vector<std::string> CommittedNames(irs::IndexWriter& writer) {
  std::vector<std::string> out;
  for (const auto& segment : writer.GetSnapshot()) {
    out.emplace_back(segment.Meta().name);
  }
  std::ranges::sort(out);
  return out;
}

std::vector<std::string_view> Views(const std::vector<std::string>& in) {
  return {in.begin(), in.end()};
}

}  // namespace

TEST_F(IndexAdoptTest, ReplaceSegmentsSwapsInOneGeneration) {
  Restart(/*cleanup_on_open=*/false);

  // Two committed segments, one doc each.
  for (const auto* value : {"old_a", "old_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  ASSERT_EQ(2, _writer->GetSnapshot().live_docs_count());

  // The replacement: written and fsynced, not yet part of the index.
  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto flushed = build.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  const auto replacement = MetaFilesOf(flushed);
  build.Abort();  // the swap adopts it; this transaction must not commit it

  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count())
    << "a flushed-but-unadopted segment must not be visible";

  ASSERT_TRUE(_writer->ReplaceSegments(Views(sources), Views(replacement),
                                       _codec, /*tick=*/10));
  ASSERT_TRUE(_writer->RefreshCommit());

  // One generation: sources gone, replacement in, in the same published meta.
  const auto after = CommittedNames(*_writer);
  ASSERT_EQ(1, after.size());
  EXPECT_EQ(flushed.front().meta.name, after.front());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the sources' rows are still reachable, so both halves were published";
}

TEST_F(IndexAdoptTest, ReplaceSegmentsAppliesRemovalsAboveTheAdoptTick) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "doomed"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(1, sources.size());

  // The rebuild copies the row, from a snapshot that predates the delete.
  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "doomed"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  // A delete lands while the rebuild is in flight, at a higher tick.
  auto del = _writer->GetBatch();
  del.Remove(ByName("doomed"));
  ASSERT_TRUE(del.Commit(20));

  // Adopted at the tick the sources were read at, so the pending removal is
  // above it and must reach the replacement.
  ASSERT_TRUE(_writer->ReplaceSegments(Views(sources), Views(replacement),
                                       _codec, /*tick=*/10));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count())
    << "the row came back through the adopted segment";
}

// A source that is no longer in the index is what a concurrent DELETE of every
// row the build was rebuilding looks like: a removal taking a segment's last
// live doc masks the whole segment out instead of giving it a docs_mask. So the
// swap proceeds -- the sources that are still there are masked, the replacement
// is adopted -- rather than failing a CREATE INDEX over an ordinary delete.
TEST_F(IndexAdoptTest, ReplaceSegmentsToleratesAVanishedSource) {
  Restart(/*cleanup_on_open=*/false);

  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "kept"));
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(1, sources.size());

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  // One source still in the index, one gone.
  std::vector<std::string> replaced = sources;
  replaced.emplace_back("_ffffffff");
  ASSERT_TRUE(_writer->ReplaceSegments(Views(replaced), Views(replacement),
                                       _codec, /*tick=*/10));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  const auto after = CommittedNames(*_writer);
  ASSERT_EQ(1, after.size());
  EXPECT_NE(after.front(), sources.front())
    << "the live source was masked and the replacement adopted";
}

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
  ASSERT_TRUE(trx.Commit(10));
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
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, CommittedNames(*_writer).size());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

// --- segment id as a config boundary ---------------------------------------

namespace {

// Segment names are "_<decimal>" (irs::FileName(uint64_t)).
uint64_t SegmentIdOf(std::string_view name) {
  EXPECT_FALSE(name.empty());
  EXPECT_EQ('_', name.front());
  return std::stoull(std::string{name.substr(1)});
}

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
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  ASSERT_EQ(2, reader.size()) << "the config change must have cut a segment";
  ASSERT_EQ(2, reader.live_docs_count());

  size_t stale = 0;
  size_t fresh = 0;
  for (const auto& segment : reader) {
    const auto id = SegmentIdOf(segment.Meta().name);
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
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  const auto boundary = _writer->CurrentSegmentId();
  for (const auto& segment : _writer->GetSnapshot()) {
    ASSERT_LE(SegmentIdOf(segment.Meta().name), boundary);
  }

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_GT(SegmentIdOf((*reader.begin()).Meta().name), boundary)
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
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  auto guard = _writer->ArmCompactionFloor();
  ASSERT_TRUE(guard.Held());
  const auto floor = guard.Floor();
  ASSERT_GT(floor, 0);

  // Two more above it: what concurrent inserts produce during the build.
  for (const auto* value : {"fresh_a", "fresh_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(20));
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
    if (SegmentIdOf(segment.Meta().name) <= floor) {
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
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  auto guard = _writer->ArmCompactionFloor();
  ASSERT_TRUE(guard.Held());
  const auto floor = guard.Floor();

  for (const auto* value : {"fresh_a", "fresh_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  // The premise: the protected segments are the ones the policy reaches first,
  // so a budget of 2 is entirely consumed by them unless they are hidden.
  {
    auto snapshot = _writer->GetSnapshot();
    ASSERT_EQ(4, snapshot.size());
    ASSERT_LE(SegmentIdOf(snapshot[0].Meta().name), floor);
    ASSERT_LE(SegmentIdOf(snapshot[1].Meta().name), floor);
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
    if (SegmentIdOf(segment.Meta().name) <= floor) {
      ++stale;
    } else {
      ++fresh;
    }
  }
  EXPECT_EQ(2, stale) << "segments below the floor were consolidated";
  EXPECT_EQ(1, fresh) << "the segments above the floor did not consolidate";
  EXPECT_EQ(4, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, CompactionFloorRefusesWhileAMergeIsRunning) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
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
      held = _writer->ArmCompactionFloor().Held();
    }
    return true;
  };
  ASSERT_TRUE(_writer->Compact(kFullMerge, nullptr, nullptr, progress));
  ASSERT_TRUE(attempted) << "the progress callback never ran";
  EXPECT_FALSE(held) << "armed a floor while a merge was in flight";

  ASSERT_TRUE(_writer->RefreshCommit());

  // Idle again, it arms -- so the refusal above was the in-flight merge.
  auto guard = _writer->ArmCompactionFloor();
  EXPECT_TRUE(guard.Held());
  // And it is not reentrant while held.
  EXPECT_FALSE(_writer->ArmCompactionFloor().Held());
}

// --- why a build must adopt before it aborts --------------------------------
//
// Transaction::Abort resets the segment context, and SegmentContext::Reset ends
// in dir.clear_refs(): the transaction's own references to the files it
// flushed are released. A cleanup pass between that and the adoption would then
// be free to unlink them. ReplaceSegments takes its own references, so calling
// it FIRST keeps the files pinned across the abort.

TEST_F(IndexAdoptTest, ReplaceBeforeAbortSurvivesCleanup) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "old"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto flushed = build.FlushAndFsync();
  const auto replacement = MetaFilesOf(flushed);
  const auto files = FilesOf(flushed);

  // The order the build uses: reference through adoption, then abort.
  ASSERT_TRUE(_writer->ReplaceSegments(Views(sources), Views(replacement),
                                       _codec, irs::writer_limits::kMinTick));
  build.Abort();
  irs::directory_utils::RemoveAllUnreferenced(*_dir);

  for (const auto& file : files) {
    EXPECT_TRUE(Exists(file)) << file << " was reclaimed before the commit";
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  EXPECT_EQ(flushed.front().meta.name, CommittedNames(*_writer).front());
}

TEST_F(IndexAdoptTest, AbortBeforeReplaceLosesTheFilesToCleanup) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "old"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto flushed = build.FlushAndFsync();
  const auto replacement = MetaFilesOf(flushed);
  const auto files = FilesOf(flushed);

  // The wrong order, kept as documentation of the hazard rather than as a
  // contract: after Abort nothing references the flushed files.
  build.Abort();
  irs::directory_utils::RemoveAllUnreferenced(*_dir);

  bool any_gone = false;
  for (const auto& file : files) {
    any_gone |= !Exists(file);
  }
  EXPECT_TRUE(any_gone)
    << "if the files survived, Abort no longer releases refs and the "
       "ordering note in search_table_backfill.cpp can be dropped";
  // And the swap correctly refuses rather than adopting a segment whose files
  // are gone.
  EXPECT_FALSE(_writer->ReplaceSegments(Views(sources), Views(replacement),
                                        _codec, irs::writer_limits::kMinTick));
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the original row must still be there";
}
