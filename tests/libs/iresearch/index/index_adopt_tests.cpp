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
    return trx.Insert().Insert(&field, &field + 1);
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
