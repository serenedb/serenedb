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

// Covers the primitives a host WAL needs to treat a flushed-but-unpublished
// segment as a durable artifact it can reference and later re-attach:
//   Transaction::FlushAndFsync   -- serialize + fsync, report the metadata
//   GetBatch(exclusive_segment)  -- segments hold only this transaction's docs
//   IndexWriterOptions::cleanup_on_open == false
//                                -- survive the open that would unlink them,
//                                   and never reissue their ids
//   IndexWriter::AdoptSegment    -- publish them again after a restart
//
// "Restart" here is: destroy the writer AND the Directory, so no in-memory
// IndexFileRefs survive, then rebuild both over the same path.

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
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
#include "iresearch/utils/type_id.hpp"
#include "tests_shared.hpp"

namespace {

// Only "1_5simd" is registered, so a codec-mismatch case needs a stub. Nothing
// but type() is ever reached: AdoptSegment compares it before touching any
// file.
struct ForeignFormatTag {};

class ForeignFormat final : public irs::Format {
 public:
  irs::IndexMetaWriter::ptr get_index_meta_writer() const final {
    return nullptr;
  }
  irs::IndexMetaReader::ptr get_index_meta_reader() const final {
    return nullptr;
  }
  irs::SegmentMetaWriter::ptr get_segment_meta_writer() const final {
    return nullptr;
  }
  irs::SegmentMetaReader::ptr get_segment_meta_reader() const final {
    return nullptr;
  }
  irs::PostingsWriter::ptr get_postings_writer(
    bool /*compaction*/, irs::IResourceManager& /*rm*/) const final {
    return nullptr;
  }
  irs::PostingsReader::ptr get_postings_reader() const final { return nullptr; }
  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<ForeignFormatTag>::id();
  }
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
    TestBase::TearDown();
    std::filesystem::remove_all(_path);
  }

  void Open(irs::OpenMode mode, bool cleanup_on_open = true,
            uint32_t segment_docs_max = 0) {
    _dir = std::make_unique<irs::MMapDirectory>(
      _path, irs::DirectoryAttributes{}, GetResourceManager().options);
    auto options = tests::EnsureWriterDb(tests::CsDefaultWriterOptions());
    options.cleanup_on_open = cleanup_on_open;
    options.segment_docs_max = segment_docs_max;
    _writer = irs::IndexWriter::Make(*_dir, _codec, mode, options);
  }

  // Drops every in-memory reference to the directory's files, then reopens --
  // the state a process restart leaves behind.
  void Restart(bool cleanup_on_open, uint32_t segment_docs_max = 0) {
    _writer.reset();
    _dir.reset();
    Open(irs::kOmAppend | irs::kOmCreate, cleanup_on_open, segment_docs_max);
  }

  static constexpr irs::field_id kNameFieldId = tests::FieldIdFor("name");

  // One indexed doc, so a flushed segment has real files behind it. The writer
  // indexes by field id, and StringField's ctor leaves it invalid, so it has to
  // be set here or a ByTerm filter on kNameFieldId matches nothing.
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

  // The metadata a host WAL would record, recovered into what AdoptSegment
  // takes. The filename is left empty: AdoptSegment writes the segment meta.
  static std::vector<irs::IndexSegment> AsAdoptable(
    std::span<const irs::IndexWriter::FlushedSegment> flushed) {
    std::vector<irs::IndexSegment> out;
    out.reserve(flushed.size());
    for (const auto& segment : flushed) {
      out.emplace_back().meta = segment.meta;
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
  std::unique_ptr<irs::MMapDirectory> _dir;
  irs::IndexWriter::ptr _writer;
};

// FlushAndFsync must report EVERY segment the transaction flushed, not just the
// one it serialized: a transaction over segment_docs_max auto-flushes
// mid-insert, so earlier segments are already on disk by the time it runs.
// Reporting only the last would leave the rest out of the caller's WAL record.
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

// An exclusive segment never resumes a pooled one, so its whole flushed set is
// the caller's own data -- a resumed segment would also carry the previous
// transaction's documents, which that transaction's own record already covers.
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

// cleanup_on_open == false is what lets a flushed-but-unpublished segment
// survive the open that would otherwise unlink it; the default reclaims it.
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

// Keeping unreferenced segments means their ids are still live. The committed
// meta's seg_counter sits below them, so without a floor the next segment would
// be created right on top of a survivor.
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

// The point of the whole exercise: rows flushed but never published come back
// after a restart, without re-encoding them.
TEST_F(IndexAdoptTest, AdoptSegmentRepublishesFlushedRows) {
  std::vector<irs::IndexSegment> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept0"));
    ASSERT_TRUE(InsertDoc(trx, "kept1"));
    adopt = AsAdoptable(trx.FlushAndFsync());
    trx.Abort();  // the publish never happened
  }
  ASSERT_EQ(1, adopt.size());

  Restart(/*cleanup_on_open=*/false);
  ASSERT_EQ(0, _writer->GetSnapshot().live_docs_count());

  for (auto& segment : adopt) {
    ASSERT_TRUE(_writer->AdoptSegment(std::move(segment), /*tick=*/7));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());

  // And they are genuinely durable now, not just live in this writer.
  Restart(/*cleanup_on_open=*/true);
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

// A removal reaches an adopted segment only when `tick <= query.tick`, which is
// what orders a replayed delete against replayed inserts. Adopting at a tick
// above the removal's must leave the documents alone.
TEST_F(IndexAdoptTest, AdoptSegmentTickOrdersAgainstRemoval) {
  std::vector<irs::IndexSegment> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    adopt = AsAdoptable(trx.FlushAndFsync());
    trx.Abort();
  }

  Restart(/*cleanup_on_open=*/false);
  for (auto& segment : adopt) {
    ASSERT_TRUE(_writer->AdoptSegment(std::move(segment), /*tick=*/10));
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

// The rule a host must place adopt ticks by: a removal masks an adopted segment
// iff `segment tick <= removal tick`. Two segments straddling one removal's
// tick must therefore end up one masked, one live -- so whichever tick space
// the host picks, it has to put a segment below exactly the removals that
// should mask it.
TEST_F(IndexAdoptTest, AdoptTickDecidesRemovalMasking) {
  std::vector<irs::IndexSegment> before;
  std::vector<irs::IndexSegment> after;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "target"));
    before = AsAdoptable(trx.FlushAndFsync());
    trx.Abort();
  }
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "target"));
    after = AsAdoptable(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_EQ(1, before.size());
  ASSERT_EQ(1, after.size());
  const std::string survivor = after.front().meta.name;
  ASSERT_NE(survivor, before.front().meta.name);

  Restart(/*cleanup_on_open=*/false);
  // One removal at tick 20; one segment below it, one above.
  ASSERT_TRUE(_writer->AdoptSegment(std::move(before.front()), /*tick=*/19));
  ASSERT_TRUE(_writer->AdoptSegment(std::move(after.front()), /*tick=*/21));
  {
    auto trx = _writer->GetBatch();
    trx.Remove(ByName("target"));
    ASSERT_TRUE(trx.Commit(/*last_tick=*/20));
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  // The one at 19 is masked; masking its only document drops the segment
  // outright, so what remains must be exactly the one adopted above the
  // removal.
  auto reader = _writer->GetSnapshot();
  EXPECT_EQ(1, reader.live_docs_count());
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(survivor, reader.begin()->Meta().name);
}

// The scheme RunSearchTableRecovery places adopt ticks by, on the case that
// distinguishes it from adopting at the record's tick.
//
// Replay of: DELETE x (record 1), SEGMENT inserting x (record 2), DELETE y
// (record 3). `x` must survive -- it was re-inserted after its delete -- and
// the only signal saying so is where the segment sits relative to the first
// removal. Adopting at the record tick (20) would put it below both rebased
// removals (which cluster just under the commit tick) and delete `x` again.
// Adopting at `commit_tick - queries + removals_seen_so_far` puts it above the
// first removal and below the second, which is exactly its manifest position.
TEST_F(IndexAdoptTest, AdoptTickFollowsManifestPositionNotRecordTick) {
  std::vector<irs::IndexSegment> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "x"));
    adopt = AsAdoptable(trx.FlushAndFsync());
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
  ASSERT_TRUE(_writer->AdoptSegment(std::move(adopt.front()),
                                    first_tick + queries_before_segment));
  ASSERT_TRUE(trx.Commit(kMaxTick));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the re-inserted document was masked by the delete that preceded it";
}

// A segment claiming a codec this writer did not produce cannot be read with
// the writer's format, so adoption has to fail rather than read it with the
// wrong one. Reporting it is the caller's job -- only the caller knows which
// durable record claimed these documents.
TEST_F(IndexAdoptTest, AdoptSegmentRejectsForeignCodec) {
  std::vector<irs::IndexSegment> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    adopt = AsAdoptable(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_EQ(1, adopt.size());

  Restart(/*cleanup_on_open=*/false);
  adopt.front().meta.codec = std::make_shared<const ForeignFormat>();
  ASSERT_NE(adopt.front().meta.codec->type(), _codec->type());
  EXPECT_FALSE(_writer->AdoptSegment(std::move(adopt.front()), /*tick=*/1));

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

// Aborting must leave the flushed files unreferenced. Reclaiming them is the
// host's background cleanup, but a file still holding a ref is invisible to it,
// so eligibility is the part that has to hold here.
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
