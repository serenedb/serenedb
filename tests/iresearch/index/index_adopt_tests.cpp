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
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <duckdb/common/fault_points.hpp>
#include <filesystem>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <limits>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <thread>
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

void AddGroup(irs::DocRemoval& removal, uint64_t segment,
              irs::DocumentMaskBuilder docs) {
  removal.groups.push_back(
    {.segment = segment, .docs = std::move(docs).Finish()});
}

// Counts how many times each file is created, so a test can pin "written once".
class CountingDirectory : public tests::DirectoryMock {
 public:
  explicit CountingDirectory(irs::Directory& impl)
    : tests::DirectoryMock{impl} {}

  irs::IndexOutput::ptr create(std::string_view name) final {
    auto out = tests::DirectoryMock::create(name);
    ++_creates[std::string{name}];
    return out;
  }

  size_t Creates(std::string_view name) const {
    const auto it = _creates.find(std::string{name});
    return it == _creates.end() ? 0 : it->second;
  }

  bool sync(std::span<const std::string_view> files) noexcept final {
    for (const auto file : files) {
      _synced.emplace_back(file);
    }
    return tests::DirectoryMock::sync(files);
  }

  std::vector<std::string> TakeSynced() { return std::exchange(_synced, {}); }

 private:
  std::map<std::string, size_t> _creates;
  std::vector<std::string> _synced;
};

class IndexAdoptTest : public TestBase {
 protected:
  void SetUp() override {
    TestBase::SetUp();
    _path = test_dir() / "adopt";
    std::filesystem::create_directories(_path);
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
    _writer = irs::IndexWriter::Make(*_dir, mode, std::move(options));
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
  // indexes by field id, so a removal by name would match nothing.
  static bool InsertDoc(irs::IndexWriter::Transaction& trx,
                        std::string_view value) {
    tests::StringField field{"name", value};
    field.id = kNameFieldId;
    return tests::InsertFields(trx.Insert(), &field, &field + 1);
  }

  static absl::flat_hash_map<std::string, size_t> LiveDocsByName(
    const irs::DirectoryReader& reader) {
    absl::flat_hash_map<std::string, size_t> counts;
    for (const auto& segment : reader) {
      const auto* field = segment.field(kNameFieldId);
      if (!field) {
        continue;
      }
      const auto masked = segment.MaskedDocs();
      for (auto terms = field->iterator(); terms->next();) {
        auto& count = counts[irs::ViewCast<char>(terms->value())];
        field->ReadDocs(terms->value(), [&](irs::doc_id_t doc) {
          count += !masked.Contains(doc);
          return true;
        });
      }
    }
    return counts;
  }

  std::shared_ptr<const irs::DocRemoval> ByName(std::string_view value) const {
    return tests::RemovalOf(*_writer, {kNameFieldId, {std::string{value}}});
  }

  std::shared_ptr<const irs::DocRemoval> ByNames(
    std::span<const std::string> values) const {
    return tests::RemovalOf(*_writer,
                            {kNameFieldId, {values.begin(), values.end()}});
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
    _writer->AdoptSegment(meta_file, /*tick=*/7);
  }
  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
  const auto synced = _dir->TakeSynced();
  ASSERT_EQ(1, synced.size());
  EXPECT_TRUE(synced.front().starts_with("pending_segments_"));

  // And they are genuinely durable now, not just live in this writer.
  Restart(/*cleanup_on_open=*/true);
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AdoptedSegmentBesideACompactionIsNotResynced) {
  Restart(false);

  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  std::vector<std::string> adopt;
  std::vector<std::string> adopted_files;
  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "adopted"));
    const auto flushed = trx.FlushAndFsync();
    adopt = MetaFilesOf(flushed);
    adopted_files = FilesOf(flushed);
    trx.Abort();
  }
  ASSERT_EQ(1, adopt.size());

  Restart(false);

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));
  _writer->AdoptSegment(adopt.front(), 20);

  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(2, snapshot.size());
  EXPECT_EQ(3, snapshot.live_docs_count());

  std::vector<std::string> expected;
  for (const auto& segment : snapshot.Meta().index_meta.segments) {
    if (segment.filename != adopt.front()) {
      expected.emplace_back(segment.filename);
      expected.insert(expected.end(), segment.meta.files.begin(),
                      segment.meta.files.end());
    }
  }
  auto synced = _dir->TakeSynced();
  ASSERT_FALSE(synced.empty());
  EXPECT_TRUE(synced.back().starts_with("pending_segments_"));
  synced.pop_back();
  std::ranges::sort(expected);
  std::ranges::sort(synced);
  EXPECT_EQ(expected, synced);
  for (const auto& file : adopted_files) {
    EXPECT_EQ(synced.end(), std::ranges::find(synced, file));
  }
}

TEST_F(IndexAdoptTest, AdoptedSegmentSurvivesAnEarlierTruncate) {
  std::vector<std::string> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    adopt = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }

  Restart(/*cleanup_on_open=*/false);
  for (const auto& meta_file : adopt) {
    _writer->AdoptSegment(meta_file, /*tick=*/10);
  }
  {
    auto trx = _writer->GetBatch();
    trx.RemoveAll();
    ASSERT_TRUE(trx.Commit(/*last_tick=*/5));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AdoptTickDecidesWhatATruncateDrops) {
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
  _writer->AdoptSegment(before.front(), /*tick=*/19);
  _writer->AdoptSegment(after.front(), /*tick=*/21);
  {
    auto trx = _writer->GetBatch();
    trx.RemoveAll();
    ASSERT_TRUE(trx.Commit(/*last_tick=*/20));
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  auto reader = _writer->GetSnapshot();
  EXPECT_EQ(1, reader.live_docs_count());
  ASSERT_EQ(1, reader.size());
  EXPECT_TRUE(survivor_meta.starts_with(reader.begin()->Meta().name + "."))
    << survivor_meta << " vs " << reader.begin()->Meta().name;
}

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
  trx.RemoveAll();  // record 1
  queries_before_segment = trx.GetQueries();
  // record 2: the segment is only stashed here; its tick needs the final query
  // count, so the adopt happens after the sweep.
  trx.Remove(std::make_shared<const irs::DocRemoval>());  // record 3

  const uint64_t queries = trx.GetQueries();
  ASSERT_EQ(2, queries);
  ASSERT_EQ(1, queries_before_segment);
  const uint64_t first_tick = kMaxTick - queries;
  _writer->AdoptSegment(adopt.front(), first_tick + queries_before_segment);
  ASSERT_TRUE(trx.Commit(kMaxTick));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the adopted segment was dropped by the truncate that preceded it";
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
  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());

  EXPECT_EQ(1, _dir->Creates(meta_file))
    << "the publish rewrote a meta that was already on disk unchanged";
  const auto synced = _dir->TakeSynced();
  ASSERT_EQ(1, synced.size());
  EXPECT_TRUE(synced.front().starts_with("pending_segments_"));
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
  ASSERT_TRUE(_writer->RefreshCommit());

  {
    auto remover = _writer->GetBatch();
    remover.Remove(ByName("doomed"));
    ASSERT_TRUE(remover.Commit(/*last_tick=*/2));
  }
  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  // The early file is still the one FlushAndFsync made; the masked meta went to
  // a bumped version, so the publish did write.
  EXPECT_EQ(1, _dir->Creates(early_meta));
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  const auto& published = _writer->GetSnapshot().begin()->Meta();
  EXPECT_GT(published.version, 0u) << "a masked segment must bump its version";

  const auto snapshot = _writer->GetSnapshot();
  const auto synced = _dir->TakeSynced();
  ASSERT_EQ(2, synced.size());
  EXPECT_EQ(snapshot.Meta().index_meta.segments.front().filename, synced[0]);
  EXPECT_TRUE(synced[1].starts_with("pending_segments_"));
}

TEST_F(IndexAdoptTest, RemovalFromALargeMaskSyncsOnlyTheNewMeta) {
  constexpr size_t kDocs = 12000;
  constexpr size_t kRounds = 4;
  uint64_t tick = 1;
  {
    auto trx = _writer->GetBatch();
    for (size_t i = 0; i != kDocs; ++i) {
      ASSERT_TRUE(InsertDoc(trx, absl::StrCat("d", i)));
    }
    ASSERT_TRUE(trx.Commit(tick));
  }
  ASSERT_TRUE(_writer->RefreshCommit());

  for (size_t round = 0; round != kRounds; ++round) {
    {
      auto remover = _writer->GetBatch();
      for (size_t i = round; i < kDocs; i += 2 * kRounds) {
        remover.Remove(ByName(absl::StrCat("d", i)));
        ++tick;
      }
      ASSERT_TRUE(remover.Commit(++tick));
    }
    _dir->TakeSynced();
    ASSERT_TRUE(_writer->RefreshCommit());

    const auto snapshot = _writer->GetSnapshot();
    ASSERT_EQ(1, snapshot.size());
    const auto& segment = snapshot.Meta().index_meta.segments.front();
    EXPECT_EQ((round + 1) * kDocs / (2 * kRounds),
              segment.meta.docs_count - segment.meta.live_docs_count);
    EXPECT_TRUE(absl::c_none_of(
      segment.meta.files,
      [](std::string_view file) { return file.ends_with(".sm"); }))
      << "round " << round;

    const auto synced = _dir->TakeSynced();
    ASSERT_EQ(2, synced.size()) << "round " << round;
    EXPECT_EQ(segment.filename, synced[0]);
    EXPECT_TRUE(synced[1].starts_with("pending_segments_"));
  }
}

// The meta file needs its own ref: the segment reader pins meta.files only, and
// adoption does not create the file.
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
  _writer->AdoptSegment(adopt.front(), /*tick=*/7);

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

TEST_F(IndexAdoptTest, AdoptSegmentRejectsAnUnreadableMeta) {
  std::vector<std::string> adopt;
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    adopt = MetaFilesOf(trx.FlushAndFsync());
    trx.Abort();
  }
  ASSERT_EQ(1, adopt.size());

  Restart(/*cleanup_on_open=*/false);
  ASSERT_NE(nullptr, _dir->create(adopt.front()));
  ASSERT_SQL_ERROR(_writer->AdoptSegment(adopt.front(), /*tick=*/1),
                   ERRCODE_INDEX_CORRUPTED);

  _writer->RefreshCommit();
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count())
    << "a rejected segment was published anyway";

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

void Replace(irs::IndexWriter& writer, const irs::DirectoryReader& snapshot,
             const std::vector<std::string>& replaced,
             const std::vector<std::string>& adopted,
             std::vector<std::vector<irs::DocMap>> absorbed = {}) {
  std::vector<const irs::SubReader*> segments;
  for (const auto& name : replaced) {
    const irs::SubReader* found = nullptr;
    for (const auto& segment : snapshot) {
      if (segment.Meta().name == name) {
        found = &segment;
      }
    }
    ASSERT_TRUE(found) << "no segment " << name;
    segments.push_back(found);
  }
  absorbed.resize(adopted.size());
  std::vector<irs::IndexWriter::Replacement> replacements;
  replacements.reserve(adopted.size());
  for (size_t i = 0; i < adopted.size(); ++i) {
    replacements.push_back(
      {.meta_file = adopted[i], .absorbed = std::move(absorbed[i])});
  }
  writer.ReplaceSegments(segments, std::move(replacements));
}

const irs::SegmentMeta& MetaOf(const irs::DirectoryReader& snapshot,
                               std::string_view name) {
  static const irs::SegmentMeta kMissing;
  for (const auto& segment : snapshot.Meta().index_meta.segments) {
    if (segment.meta.name == name) {
      return segment.meta;
    }
  }
  ADD_FAILURE() << "no segment " << name;
  return kMissing;
}

size_t MappedInto(const std::vector<irs::DocMap>& maps,
                  std::string_view target) {
  const auto id = IdOf(target);
  return absl::c_count_if(maps,
                          [&](const auto& map) { return map.target == id; });
}

std::shared_ptr<irs::DocMapsSlot> SlotOf(const irs::DirectoryReader& snapshot,
                                         std::string_view name) {
  for (const auto& segment : snapshot) {
    if (segment.Meta().name == name) {
      return irs::utils::downCast<irs::SegmentReader>(segment)
        .GetImpl()
        ->DocMaps();
    }
  }
  ADD_FAILURE() << "no segment " << name;
  return nullptr;
}

std::vector<irs::DocMap> DocMapsOf(const irs::IndexWriter& writer,
                                   const irs::DirectoryReader& snapshot,
                                   const std::vector<std::string>& segments) {
  irs::DocRemoval removal;
  for (const auto& segment : segments) {
    removal.groups.push_back({.segment = IdOf(segment),
                              .docs = irs::DocumentMaskBuilder{}.Finish(),
                              .maps = SlotOf(snapshot, segment)});
  }
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  std::vector<irs::DocMap> maps;
  writer.VisitDocMaps(removals, [&](const std::vector<irs::DocMap>& list) {
    maps.insert(maps.end(), list.begin(), list.end());
  });
  return maps;
}

irs::DocMap Takes(const irs::DirectoryReader& snapshot, std::string_view source,
                  irs::doc_id_t rank_begin, irs::doc_id_t rank_end) {
  return {.source = IdOf(source),
          .mask = MetaOf(snapshot, source).docs_mask,
          .rank_begin = rank_begin,
          .rank_end = rank_end,
          .base = irs::doc_limits::min()};
}

irs::DocMap TakesAll(const irs::DirectoryReader& snapshot,
                     std::string_view source) {
  return Takes(
    snapshot, source, 0,
    static_cast<irs::doc_id_t>(MetaOf(snapshot, source).live_docs_count));
}

std::shared_ptr<const irs::DocRemoval> Positions(
  const std::map<std::string, std::vector<irs::doc_id_t>>& docs) {
  auto removal = std::make_shared<irs::DocRemoval>();
  for (const auto& [name, segment_docs] : docs) {
    irs::DocumentMaskBuilder mask;
    for (const auto doc : segment_docs) {
      mask.Add(doc);
    }
    AddGroup(*removal, IdOf(name), std::move(mask));
  }
  return removal;
}

uint32_t IndexOf(const irs::DirectoryReader& snapshot, std::string_view name) {
  for (uint32_t index = 0; index != snapshot.size(); ++index) {
    if (snapshot[index].Meta().name == name) {
      return index;
    }
  }
  ADD_FAILURE() << "no segment " << name;
  return 0;
}

std::shared_ptr<const irs::DocRemoval> Positions(
  const irs::DirectoryReader& snapshot,
  const std::map<std::string, std::vector<irs::doc_id_t>>& docs) {
  irs::DocRemovalBuilder builder;
  for (const auto& [name, segment_docs] : docs) {
    const auto index = IndexOf(snapshot, name);
    for (const auto doc : segment_docs) {
      builder.Add(index, doc);
    }
  }
  return std::move(builder).Finish(snapshot);
}

std::shared_ptr<const irs::DocRemoval> FirstDocsEverywhere(
  irs::doc_id_t count) {
  auto removal = std::make_shared<irs::DocRemoval>();
  for (uint64_t segment = 0; segment < 64; ++segment) {
    irs::DocumentMaskBuilder docs;
    docs.AddRange(irs::doc_limits::min(), irs::doc_limits::min() + count);
    AddGroup(*removal, segment, std::move(docs));
  }
  return removal;
}

const irs::CompactionPolicy& FullMerge() {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  return kPolicy;
}

}  // namespace

TEST_F(IndexAdoptTest, DocRemovalMasksOnlyItsSegment) {
  for (const auto* value : {"old_a", "old_b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());

  auto del = _writer->GetBatch();
  del.Remove(Positions({{sources.front(), {1}}}));
  ASSERT_TRUE(del.Commit(20));
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  EXPECT_EQ(std::vector<std::string>{sources.back()}, CommittedNames(*_writer));
}

TEST_F(IndexAdoptTest, DocRemovalOfAReplacedSegmentReachesItsReplacement) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "doomed"));
  ASSERT_TRUE(InsertDoc(seed, "kept"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(1, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "doomed"));
  ASSERT_TRUE(InsertDoc(build, "kept"));
  const auto flushed = build.FlushAndFsync();
  const auto replacement = MetaFilesOf(flushed);
  const std::string name{flushed.front().meta.name};
  build.Abort();

  auto del = _writer->GetBatch();
  del.Remove(Positions(snapshot, {{sources.front(), {1}}}));
  ASSERT_TRUE(del.Commit(20));

  Replace(*_writer, snapshot, sources, replacement,
          {{TakesAll(snapshot, sources.front())}});
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(std::vector<std::string>{name}, CommittedNames(*_writer));
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  auto survivor = _writer->GetBatch();
  survivor.Remove(ByName("kept"));
  ASSERT_TRUE(survivor.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOfAReplacedSegmentMissesAReplacementOfNoRows) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "kept"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(1, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "kept"));
  const auto flushed = build.FlushAndFsync();
  const auto replacement = MetaFilesOf(flushed);
  const std::string name{flushed.front().meta.name};
  build.Abort();

  auto del = _writer->GetBatch();
  del.Remove(Positions({{sources.front(), {1}}}));
  ASSERT_TRUE(del.Commit(20));

  Replace(*_writer, snapshot, sources, replacement);
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(std::vector<std::string>{name}, CommittedNames(*_writer));
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalMasksOnlyTheListedDocs) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  for (const auto* value : {"a", "b", "c", "d"}) {
    ASSERT_TRUE(InsertDoc(trx, value));
  }
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto names = CommittedNames(*_writer);
  ASSERT_EQ(1, names.size());

  auto del = _writer->GetBatch();
  del.Remove(Positions({{names.front(), {4, 2}}}));
  ASSERT_TRUE(del.Commit(20));
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(2, snapshot.live_docs_count());
  auto mask = snapshot[0].MaskedDocs();
  EXPECT_FALSE(mask.Contains(1));
  EXPECT_TRUE(mask.Contains(2));
  EXPECT_FALSE(mask.Contains(3));
  EXPECT_TRUE(mask.Contains(4));
}

TEST_F(IndexAdoptTest, DocRemovalOfMaskedDocsChangesNothing) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  for (const auto* value : {"a", "b", "c"}) {
    ASSERT_TRUE(InsertDoc(trx, value));
  }
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto names = CommittedNames(*_writer);
  for (const uint64_t tick : {20, 30}) {
    auto del = _writer->GetBatch();
    del.Remove(Positions({{names.front(), {2}}}));
    ASSERT_TRUE(del.Commit(tick));
    _writer->RefreshCommit();
  }
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOnAPendingCompactionIsRemapped) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));

  auto del = _writer->GetBatch();
  del.Remove(Positions(_writer->GetSnapshot(),
                       {{sources.front(), {1}}, {sources.back(), {2}}}));
  ASSERT_TRUE(del.Commit(20));
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(2, snapshot.live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalInFlightDuringACompactionIsRemapped) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "1")));
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "2")));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());

  auto del = _writer->GetBatch();
  del.Remove(Positions(_writer->GetSnapshot(),
                       {{sources.front(), {2}}, {sources.back(), {1}}}));
  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));
  ASSERT_TRUE(del.Commit(20));
  ASSERT_TRUE(_writer->RefreshCommit());

  ASSERT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("a1"));
  survivors.Remove(ByName("b2"));
  ASSERT_TRUE(survivors.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOfAMergedAwaySegmentIsRemapped) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto merged = CommittedNames(*_writer);
  ASSERT_EQ(1, merged.size());
  ASSERT_NE(sources.front(), merged.front());

  auto del = _writer->GetBatch();
  del.Remove(
    Positions(snapshot, {{sources.front(), {1}}, {sources.back(), {1}}}));
  ASSERT_TRUE(del.Commit(20));
  _writer->RefreshCommit();

  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalIsRemappedThroughTwoMerges) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "1")));
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "2")));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto first_merged = CommittedNames(*_writer);
  ASSERT_EQ(1, first_merged.size());
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "c1"));
    ASSERT_TRUE(trx.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto between = _writer->GetSnapshot();
  const auto second_sources = CommittedNames(*_writer);
  ASSERT_EQ(2, second_sources.size());
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto merged = CommittedNames(*_writer);
  ASSERT_EQ(1, merged.size());
  EXPECT_EQ(2, MappedInto(DocMapsOf(*_writer, snapshot, sources),
                          first_merged.front()));
  EXPECT_EQ(2, MappedInto(DocMapsOf(*_writer, between, second_sources),
                          merged.front()));
  EXPECT_EQ(3, DocMapsOf(*_writer, snapshot, sources).size())
    << "the sources' maps lead to the second merge through the first";

  auto del = _writer->GetBatch();
  del.Remove(
    Positions(snapshot, {{sources.front(), {2}}, {sources.back(), {1}}}));
  ASSERT_TRUE(del.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());

  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("a1"));
  survivors.Remove(ByName("b2"));
  survivors.Remove(ByName("c1"));
  ASSERT_TRUE(survivors.Commit(40));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOfADocDeletedBeforeTwoMergesChangesNothing) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "1")));
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "2")));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();
  {
    auto trx = _writer->GetBatch();
    trx.Remove(ByName("a2"));
    ASSERT_TRUE(trx.Commit(15));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "c1"));
    ASSERT_TRUE(trx.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  ASSERT_EQ(1, CommittedNames(*_writer).size());
  ASSERT_EQ(4, _writer->GetSnapshot().live_docs_count());

  auto del = _writer->GetBatch();
  del.Remove(Positions(snapshot, {{sources.front(), {2}}}));
  ASSERT_TRUE(del.Commit(30));
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(4, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocMapsLastAsLongAsASnapshotOfTheirSource) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  auto old = _writer->GetSnapshot();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto merged = CommittedNames(*_writer).front();
  ASSERT_EQ(2, MappedInto(DocMapsOf(*_writer, old, sources), merged));
  const std::weak_ptr<irs::DocMapsSlot> maps = SlotOf(old, sources.front());

  const auto commit = [&](std::string_view value) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  for (const auto* value : {"f1", "f2", "f3"}) {
    commit(value);
  }
  EXPECT_EQ(2, DocMapsOf(*_writer, old, sources).size())
    << "the older snapshot can still name the sources";
  EXPECT_FALSE(maps.expired());

  old = {};
  EXPECT_TRUE(maps.expired()) << "no snapshot can name the sources anymore";
  commit("f4");

  auto del = _writer->GetBatch();
  del.Remove(Positions({{sources.front(), {1}}}));
  ASSERT_TRUE(del.Commit(30));
  _writer->RefreshCommit();
  EXPECT_EQ(6, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, CompactionDuringABegunCommitFindsNoRemovals) {
  const auto commit = [&](std::string_view value) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  commit("a");
  commit("b");
  const auto sources = CommittedNames(*_writer);
  const auto before = _writer->GetSnapshot();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto merged = CommittedNames(*_writer).front();
  ASSERT_EQ(2, MappedInto(DocMapsOf(*_writer, before, sources), merged));
  commit("c");

  auto pending = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(pending, "d"));
  ASSERT_TRUE(pending.Commit(20));
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  EXPECT_EQ(2, snapshot.size());
  EXPECT_EQ(4, snapshot.live_docs_count());
  for (const auto& segment : snapshot) {
    EXPECT_EQ(nullptr, segment.docs_mask()) << segment.Meta().name;
  }
}

TEST_F(IndexAdoptTest,
       AStaleRemovalStopsAtTheMergeCandidateItsCommitPublishes) {
  const auto commit = [&](std::string_view value) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  commit("a");
  commit("b");
  const auto sources = CommittedNames(*_writer);
  const auto before = _writer->GetSnapshot();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  commit("c");

  auto pending = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(pending, "d"));
  ASSERT_TRUE(pending.Commit(20));
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  auto stale = _writer->GetBatch();
  stale.Remove(Positions(before, {{sources.front(), {1}}}));
  ASSERT_TRUE(stale.Commit(30));
  _writer->RefreshCommit();
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());

  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("b"));
  survivors.Remove(ByName("c"));
  survivors.Remove(ByName("d"));
  ASSERT_TRUE(survivors.Commit(40));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocMapsLastWhileARemovalNamesTheirSource) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  auto removal = Positions(_writer->GetSnapshot(), {{sources.back(), {1}}});
  const std::weak_ptr<const irs::DocMapsSlot> maps =
    removal->groups.front().maps;
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto commit = [&](std::string_view value) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  commit("c");
  commit("d");
  ASSERT_FALSE(maps.expired()) << "the removal names the source";
  EXPECT_EQ(1, maps.lock()->maps.size());

  auto del = _writer->GetBatch();
  del.Remove(std::move(removal));
  ASSERT_TRUE(del.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());
  commit("e");
  EXPECT_TRUE(maps.expired()) << "the applied removal let its source go";

  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("a"));
  survivors.Remove(ByName("c"));
  survivors.Remove(ByName("d"));
  survivors.Remove(ByName("e"));
  ASSERT_TRUE(survivors.Commit(40));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AnAbortedCommitDropsTheDocMapsOfItsMerge) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  EXPECT_TRUE(DocMapsOf(*_writer, snapshot, sources).empty())
    << "a merge of the open flush commits with every record that names it";
  ASSERT_TRUE(_writer->RefreshBegin());
  EXPECT_EQ(2, DocMapsOf(*_writer, snapshot, sources).size())
    << "a merge whose commit has begun commits before the open flush";
  _writer->RefreshAbort();
  EXPECT_TRUE(DocMapsOf(*_writer, snapshot, sources).empty());
  EXPECT_EQ(sources, CommittedNames(*_writer));

  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto merged = CommittedNames(*_writer);
  ASSERT_EQ(1, merged.size());
  EXPECT_EQ(2,
            MappedInto(DocMapsOf(*_writer, snapshot, sources), merged.front()));
}

TEST_F(IndexAdoptTest, DocRemovalSkipsTheSegmentsOfItsOwnCommit) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "before"));
  trx.Remove(FirstDocsEverywhere(2));
  ASSERT_TRUE(InsertDoc(trx, "after"));
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
  auto del = _writer->GetBatch();
  del.Remove(ByName("after"));
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalReachesItsSegmentOnceCommitted) {
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "first"));
    ASSERT_TRUE(InsertDoc(trx, "second"));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "third"));
  trx.Remove(FirstDocsEverywhere(1));
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, EmptyDocRemovalKeepsTheSegment) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "a"));
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto before = _writer->GetSnapshot()[0].Meta().version;

  auto del = _writer->GetBatch();
  del.Remove(Positions({}));
  ASSERT_TRUE(del.Commit(20));
  _writer->RefreshCommit();
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(before, snapshot[0].Meta().version);
  EXPECT_EQ(1, snapshot.live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalRemapSkipsMergedAwayAndOutOfRangeDocs) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "1")));
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "2")));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("a1"));
    ASSERT_TRUE(del.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));

  auto del = _writer->GetBatch();
  del.Remove(Positions(_writer->GetSnapshot(),
                       {{sources.front(), {1, 2, 99}}, {sources.back(), {2}}}));
  ASSERT_TRUE(del.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(1, snapshot.live_docs_count());
  auto survivor = _writer->GetBatch();
  survivor.Remove(ByName("b1"));
  ASSERT_TRUE(survivor.Commit(40));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

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
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(2, snapshot.live_docs_count());

  // The replacement: written and fsynced, not yet part of the index.
  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto flushed = build.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  const auto replacement = MetaFilesOf(flushed);
  const std::string replacement_name = flushed.front().meta.name;
  build.Abort();  // the swap adopts it; this transaction must not commit it

  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count())
    << "a flushed-but-unadopted segment must not be visible";

  _dir->TakeSynced();
  Replace(*_writer, snapshot, sources, replacement);
  ASSERT_TRUE(_writer->RefreshCommit());

  // One generation: sources gone, replacement in, in the same published meta.
  const auto after = CommittedNames(*_writer);
  ASSERT_EQ(1, after.size());
  EXPECT_EQ(replacement_name, after.front());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the sources' rows are still reachable, so both halves were published";
  const auto synced = _dir->TakeSynced();
  ASSERT_EQ(1, synced.size());
  EXPECT_TRUE(synced.front().starts_with("pending_segments_"));
}

// A removal a refresh has already consumed is not pending any more, and the
// rebuilt segment would publish carrying the deleted row. The swap carries what
// the source lost since the build's snapshot into the rows the replacement took
// from it, in the generation that adopts it -- there is no published state
// where the row is back.
TEST_F(IndexAdoptTest, ReplaceSegmentsCarriesRemovalsAppliedSinceTheSnapshot) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "doomed"));
  ASSERT_TRUE(InsertDoc(seed, "kept"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(1, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  // The rebuild copies both rows, off a snapshot that predates the delete.
  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "doomed"));
  ASSERT_TRUE(InsertDoc(build, "kept"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  // The delete lands and a refresh consumes it: applied to the source, no
  // longer pending.
  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("doomed"));
    ASSERT_TRUE(del.Commit(20));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  ASSERT_EQ(1, _writer->GetSnapshot().live_docs_count());

  _dir->TakeSynced();
  Replace(*_writer, snapshot, sources, replacement,
          {{TakesAll(snapshot, sources.front())}});
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the deleted row came back with the adopted segment";

  const auto after = _writer->GetSnapshot();
  ASSERT_EQ(1, after.size());
  const auto synced = _dir->TakeSynced();
  ASSERT_EQ(2, synced.size());
  EXPECT_EQ(after.Meta().index_meta.segments.front().filename, synced[0]);
  EXPECT_TRUE(synced[1].starts_with("pending_segments_"));
}

TEST_F(IndexAdoptTest, ReplaceSegmentsCarriesRemovalsIntoTheirWindowsOnly) {
  Restart(/*cleanup_on_open=*/false);

  auto bystander = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(bystander, "twin"));
  ASSERT_TRUE(bystander.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto bystanders = CommittedNames(*_writer);
  ASSERT_EQ(1, bystanders.size());

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  for (const auto* value : {"a1", "a2", "a3", "a4"}) {
    ASSERT_TRUE(InsertDoc(seed, value));
  }
  ASSERT_TRUE(seed.Commit(20));
  ASSERT_TRUE(_writer->RefreshCommit());
  std::vector<std::string> sources;
  for (auto& name : CommittedNames(*_writer)) {
    if (name != bystanders.front()) {
      sources.push_back(std::move(name));
    }
  }
  ASSERT_EQ(1, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  std::vector<std::string> replacements;
  for (const auto& values : {std::array{"a1", "a2"}, std::array{"a3", "a4"}}) {
    auto build = _writer->GetBatch(/*exclusive_segment=*/true);
    for (const auto* value : values) {
      ASSERT_TRUE(InsertDoc(build, value));
    }
    for (auto& meta : MetaFilesOf(build.FlushAndFsync())) {
      replacements.push_back(std::move(meta));
    }
    build.Abort();
  }
  ASSERT_EQ(2, replacements.size());

  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("a3"));
    ASSERT_TRUE(del.Commit(30));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  auto pending = _writer->GetBatch();
  pending.Remove(Positions(snapshot, {{sources.front(), {1}}}));
  ASSERT_TRUE(pending.Commit(40));

  Replace(*_writer, snapshot, sources, replacements,
          {{Takes(snapshot, sources.front(), 0, 2)},
           {Takes(snapshot, sources.front(), 2, 4)}});
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());
  const auto after = CommittedNames(*_writer);
  EXPECT_NE(after.end(), std::ranges::find(after, bystanders.front()));
  EXPECT_EQ(after.end(), std::ranges::find(after, sources.front()));
  auto survivors = _writer->GetBatch();
  for (const auto* value : {"twin", "a2", "a4"}) {
    survivors.Remove(ByName(value));
  }
  ASSERT_TRUE(survivors.Commit(50));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, ReplaceSegmentsRefusesABegunCommit) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "source"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(1, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  auto pending = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(pending, "pending"));
  ASSERT_TRUE(pending.Commit(20));
  ASSERT_TRUE(_writer->RefreshBegin());

  ASSERT_SQL_ERROR(Replace(*_writer, snapshot, sources, replacement),
                   ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);

  _writer->RefreshCommit();
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
  const auto after = CommittedNames(*_writer);
  EXPECT_NE(after.end(), std::ranges::find(after, sources.front()));
}

// A source that is no longer in the index is what a concurrent DELETE of every
// row the build was rebuilding looks like: a removal taking a segment's last
// live doc masks the whole segment out instead of giving it a docs_mask. So the
// swap proceeds -- the sources that are still there are masked, the replacement
// is adopted -- rather than failing a CREATE INDEX over an ordinary delete.
TEST_F(IndexAdoptTest, ReplaceSegmentsToleratesAVanishedSource) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"kept", "gone"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "rebuilt"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("gone"));
    ASSERT_TRUE(del.Commit(20));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  ASSERT_EQ(1, CommittedNames(*_writer).size()) << "one source is gone";

  Replace(*_writer, snapshot, sources, replacement);
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  const auto after = CommittedNames(*_writer);
  ASSERT_EQ(1, after.size());
  EXPECT_EQ(sources.end(), std::ranges::find(sources, after.front()))
    << "the live source was masked and the replacement adopted";
}

TEST_F(IndexAdoptTest, ReplaceSegmentsMasksTheRowsOfASourceThatVanished) {
  Restart(/*cleanup_on_open=*/false);

  auto seed = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(seed, "a1"));
  ASSERT_TRUE(InsertDoc(seed, "a2"));
  ASSERT_TRUE(seed.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto first = CommittedNames(*_writer);
  ASSERT_EQ(1, first.size());
  auto other = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(other, "b1"));
  ASSERT_TRUE(other.Commit(20));
  ASSERT_TRUE(_writer->RefreshCommit());
  std::vector<std::string> sources = first;
  for (auto& name : CommittedNames(*_writer)) {
    if (name != first.front()) {
      sources.push_back(std::move(name));
    }
  }
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  for (const auto* value : {"a1", "a2", "b1"}) {
    ASSERT_TRUE(InsertDoc(build, value));
  }
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("a1"));
    del.Remove(ByName("a2"));
    ASSERT_TRUE(del.Commit(30));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  ASSERT_EQ(1, CommittedNames(*_writer).size()) << "the first source is gone";

  auto absorbed = TakesAll(snapshot, sources.back());
  absorbed.base = irs::doc_limits::min() + 2;
  Replace(*_writer, snapshot, sources, replacement,
          {{TakesAll(snapshot, sources.front()), std::move(absorbed)}});
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  auto survivor = _writer->GetBatch();
  survivor.Remove(ByName("b1"));
  ASSERT_TRUE(survivor.Commit(40));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest,
       ReplaceSegmentsTakingNothingFromAVanishedSourceMasksNothing) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"a1", "b1"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "b1"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("a1"));
    ASSERT_TRUE(del.Commit(20));
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  ASSERT_EQ(1, CommittedNames(*_writer).size()) << "the first source is gone";

  Replace(*_writer, snapshot, sources, replacement,
          {{Takes(snapshot, sources.front(), 0, 0),
            TakesAll(snapshot, sources.back())}});
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the vanished source gave the replacement no rows to mask";
  auto survivor = _writer->GetBatch();
  survivor.Remove(ByName("b1"));
  ASSERT_TRUE(survivor.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, ReplaceSegmentsOfNothingChangesNothing) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "kept"));
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto before = CommittedNames(*_writer);

  Replace(*_writer, _writer->GetSnapshot(), {}, {});
  _writer->RefreshCommit();

  EXPECT_EQ(before, CommittedNames(*_writer));
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, ReplaceSegmentsWithoutSourcesAddsTheReplacement) {
  Restart(/*cleanup_on_open=*/false);

  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(trx, "kept"));
  ASSERT_TRUE(trx.Commit(10));
  ASSERT_TRUE(_writer->RefreshCommit());

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "added"));
  const auto replacement = MetaFilesOf(build.FlushAndFsync());
  build.Abort();

  Replace(*_writer, _writer->GetSnapshot(), {}, replacement);
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(2, CommittedNames(*_writer).size());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, ARemovalAndAReplacementLandInOneCommit) {
  Restart(/*cleanup_on_open=*/false);

  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(build, "b"));
  const auto flushed = build.FlushAndFsync();
  const auto replacement = MetaFilesOf(flushed);
  const std::string replacement_name = flushed.front().meta.name;
  build.Abort();

  auto del = _writer->GetBatch();
  del.Remove(Positions(snapshot, {{sources.back(), {1}}}));
  ASSERT_TRUE(del.Commit(20));
  Replace(*_writer, snapshot, {sources.back()}, replacement,
          {{TakesAll(snapshot, sources.back())}});
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(std::vector<std::string>{sources.front()}, CommittedNames(*_writer))
    << "the replacement " << replacement_name
    << " lost its only row to the removal of the row it replaced";
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  auto survivor = _writer->GetBatch();
  survivor.Remove(ByName("a"));
  ASSERT_TRUE(survivor.Commit(30));
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AReaderReopenedWithoutChangesKeepsTheDocMaps) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  auto before = _writer->GetSnapshot();
  _writer->RefreshCommit({.reopen_reader = true});
  auto reopened = _writer->GetSnapshot();
  ASSERT_NE(before.GetImpl(), reopened.GetImpl());
  before = {};
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto merged = CommittedNames(*_writer).front();
  const auto commit = [&](std::string_view value) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(20));
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  commit("c");
  EXPECT_EQ(2, MappedInto(DocMapsOf(*_writer, reopened, sources), merged))
    << "the reopened reader can still name the sources";
  const std::weak_ptr<irs::DocMapsSlot> maps =
    SlotOf(reopened, sources.front());
  reopened = {};
  EXPECT_TRUE(maps.expired());
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

TEST_F(IndexAdoptTest,
       AMetaRewrittenForADeleteIsSyncedBesideAPendingCompaction) {
  for (const auto* value : {"merged_a", "merged_b", "untouched"}) {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "doomed"));
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_EQ(4, _writer->GetSnapshot().size());

  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "in_flight"));
    ASSERT_TRUE(trx.Commit(15));
  }
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_EQ(irs::CompactionError::Pending,
            _writer->Compact(BudgetedPolicy(2)).error);
  _writer->RefreshCommit();

  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("doomed"));
    ASSERT_TRUE(del.Commit(20));
  }

  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(4, snapshot.size());
  EXPECT_EQ(5, snapshot.live_docs_count());

  const auto synced = _dir->TakeSynced();
  size_t rewritten = 0;
  for (const auto& segment : snapshot.Meta().index_meta.segments) {
    if (segment.meta.docs_mask) {
      ++rewritten;
      EXPECT_NE(synced.end(), std::ranges::find(synced, segment.filename))
        << segment.filename << " was written by this commit but never synced";
    }
  }
  EXPECT_EQ(1, rewritten);
}

TEST_F(IndexAdoptTest, AFinishedCompactionRewrittenForADeleteIsSyncedOnce) {
  constexpr size_t kDocs = 5000;
  std::vector<std::string> evens;
  std::vector<std::string> ones;
  {
    auto trx = _writer->GetBatch(true);
    for (size_t i = 0; i != kDocs; ++i) {
      auto name = absl::StrCat("d", i);
      ASSERT_TRUE(InsertDoc(trx, name));
      if (i % 2 == 0) {
        evens.push_back(std::move(name));
      } else if (i % 4 == 1) {
        ones.push_back(std::move(name));
      }
    }
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "other"));
    ASSERT_TRUE(trx.Commit(10));
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_EQ(2, _writer->GetSnapshot().size());

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});

  bool removed = false;
  const irs::MergeWriter::FlushProgress progress = [&] {
    if (!removed) {
      removed = true;
      auto del = _writer->GetBatch();
      del.Remove(ByNames(evens));
      EXPECT_TRUE(del.Commit(20));
      EXPECT_TRUE(_writer->RefreshCommit());
    }
    return true;
  };
  ASSERT_EQ(irs::CompactionError::Ok,
            _writer->Compact(kFullMerge, nullptr, progress).error);
  ASSERT_TRUE(removed) << "the progress callback never ran";

  {
    auto del = _writer->GetBatch();
    del.Remove(ByNames(ones));
    ASSERT_TRUE(del.Commit(30));
  }

  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(kDocs / 4 + 1, snapshot.live_docs_count());

  const auto& compacted = snapshot.Meta().index_meta.segments.front();
  ASSERT_NE(nullptr, compacted.meta.docs_mask);
  for (const auto& file : compacted.meta.files) {
    EXPECT_FALSE(file.ends_with(".sm")) << file << " is a chain link";
  }

  auto synced = _dir->TakeSynced();
  ASSERT_FALSE(synced.empty());
  EXPECT_TRUE(synced.back().starts_with("pending_segments_"));
  synced.pop_back();
  std::vector<std::string> expected{compacted.meta.files.begin(),
                                    compacted.meta.files.end()};
  expected.emplace_back(compacted.filename);
  std::ranges::sort(expected);
  std::ranges::sort(synced);
  EXPECT_EQ(expected, synced);
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
  ASSERT_TRUE(_writer->Compact(kFullMerge, nullptr, progress));
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
  const std::string replacement_name = flushed.front().meta.name;

  // The order the build uses: reference through adoption, then abort.
  Replace(*_writer, _writer->GetSnapshot(), sources, replacement);
  build.Abort();
  irs::directory_utils::RemoveAllUnreferenced(*_dir);

  for (const auto& file : files) {
    EXPECT_TRUE(Exists(file)) << file << " was reclaimed before the commit";
  }
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  EXPECT_EQ(replacement_name, CommittedNames(*_writer).front());
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
  ASSERT_SQL_ERROR(
    Replace(*_writer, _writer->GetSnapshot(), sources, replacement),
    ERRCODE_UNDEFINED_FILE);
  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count())
    << "the original row must still be there";
}

#ifdef SDB_FAULT_INJECTION
TEST_F(IndexAdoptTest, ACommitJoinsTheFlushItsPooledSegmentIsPinnedTo) {
  constexpr std::string_view kSwitch = "pause_irs_commit_after_flush_switch";
  {
    auto seed = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(seed, "seed"));
    ASSERT_TRUE(seed.Commit(1));
  }
  auto pooled = _writer->GetBatch();
  ASSERT_TRUE(InsertDoc(pooled, "commit"));
  auto exclusive = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(exclusive, "commit"));

  ASSERT_TRUE(duckdb::FaultPoints::Arm(kSwitch));
  std::thread refresh{[&] { EXPECT_TRUE(_writer->RefreshCommit()); }};
  absl::Cleanup release = [&] {
    duckdb::FaultPoints::Arm(absl::StrCat(kSwitch, ":release"));
    refresh.join();
    duckdb::FaultPoints::Clear();
  };
  while (!duckdb::FaultPoints::Disarm(absl::StrCat(kSwitch, ":parked"))) {
    std::this_thread::yield();
  }

  irs::IndexWriter::Transaction* commit[] = {&pooled, &exclusive};
  irs::IndexWriter::Transaction::RegisterFlush(commit);
  ASSERT_TRUE(pooled.Commit(2));
  ASSERT_TRUE(exclusive.Commit(2));
  std::move(release).Invoke();

  const auto counts = LiveDocsByName(_writer->GetSnapshot());
  EXPECT_EQ(1, counts.at("seed"));
  EXPECT_EQ(2, counts.at("commit"))
    << "the refresh flushed one transaction of the commit without the other";
}

TEST_F(IndexAdoptTest, TransactionsListedBeforeThePooledOneJoinItsFlush) {
  constexpr std::string_view kSwitch = "pause_irs_commit_after_flush_switch";
  {
    auto seed = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(seed, "seed"));
    ASSERT_TRUE(seed.Commit(1));
  }
  auto exclusive = _writer->GetBatch(/*exclusive_segment=*/true);
  ASSERT_TRUE(InsertDoc(exclusive, "commit"));
  auto idle = _writer->GetBatch();
  auto pooled = _writer->GetBatch();
  ASSERT_TRUE(InsertDoc(pooled, "commit"));

  ASSERT_TRUE(duckdb::FaultPoints::Arm(kSwitch));
  std::thread refresh{[&] { EXPECT_TRUE(_writer->RefreshCommit()); }};
  absl::Cleanup release = [&] {
    duckdb::FaultPoints::Arm(absl::StrCat(kSwitch, ":release"));
    refresh.join();
    duckdb::FaultPoints::Clear();
  };
  while (!duckdb::FaultPoints::Disarm(absl::StrCat(kSwitch, ":parked"))) {
    std::this_thread::yield();
  }

  irs::IndexWriter::Transaction* commit[] = {&exclusive, &idle, &pooled};
  irs::IndexWriter::Transaction::RegisterFlush(commit);
  ASSERT_TRUE(exclusive.Commit(2));
  ASSERT_TRUE(pooled.Commit(2));
  std::move(release).Invoke();

  const auto counts = LiveDocsByName(_writer->GetSnapshot());
  EXPECT_EQ(1, counts.at("seed"));
  EXPECT_EQ(2, counts.at("commit"))
    << "the transaction listed before the pooled one stayed in the next flush";
}
#endif

TEST_F(IndexAdoptTest, RefreshesRacingCommitsNeverSplitOne) {
  constexpr size_t kWriters = 4;
  constexpr size_t kCommits = 300;
  std::atomic<bool> writing{true};
  std::atomic<size_t> split{0};
  std::thread refresher{[&] {
    while (writing.load(std::memory_order_relaxed)) {
      _writer->RefreshCommit();
      for (const auto& [name, count] : LiveDocsByName(_writer->GetSnapshot())) {
        if (count != 2) {
          split.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  }};
  std::vector<std::thread> writers;
  for (size_t w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (size_t n = 0; n < kCommits; ++n) {
        const auto name = absl::StrCat("w", w, "-", n);
        auto pooled = _writer->GetBatch();
        auto exclusive = _writer->GetBatch(/*exclusive_segment=*/true);
        EXPECT_TRUE(InsertDoc(pooled, name));
        if (n % 4 == 0) {
          std::this_thread::sleep_for(std::chrono::microseconds{200});
        }
        EXPECT_TRUE(InsertDoc(exclusive, name));
        irs::IndexWriter::Transaction* commit[] = {&pooled, &exclusive};
        irs::IndexWriter::Transaction::RegisterFlush(commit);
        const auto tick = _writer->NextTick(1);
        EXPECT_TRUE(pooled.Commit(tick));
        EXPECT_TRUE(exclusive.Commit(tick));
      }
    });
  }
  for (auto& writer : writers) {
    writer.join();
  }
  writing.store(false, std::memory_order_relaxed);
  refresher.join();
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(0, split.load()) << "a refresh published part of a commit";
  const auto counts = LiveDocsByName(_writer->GetSnapshot());
  EXPECT_EQ(kWriters * kCommits, counts.size());
  for (const auto& [name, count] : counts) {
    EXPECT_EQ(2, count) << name;
  }
}
