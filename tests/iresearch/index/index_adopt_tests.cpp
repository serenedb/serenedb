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
#include <iresearch/utils/async.hpp>
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
      const auto* masked = segment.docs_mask();
      for (auto terms = field->iterator(); terms->next();) {
        auto& count = counts[irs::ViewCast<char>(terms->value())];
        field->ReadDocs(terms->value(), [&](irs::doc_id_t doc) {
          count += !(masked && masked->Contains(doc));
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

// SegmentContext::Rollback must cut only the tail past committed_flushed_docs.
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

std::vector<std::string> CommittedNames(irs::IndexWriter& writer) {
  std::vector<std::string> out;
  for (const auto& segment : writer.GetSnapshot()) {
    out.emplace_back(segment.Meta().name);
  }
  std::ranges::sort(out);
  return out;
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
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());

  auto del = _writer->GetBatch();
  del.Remove(Positions({{sources.front(), {1}}}));
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  EXPECT_EQ(1, _writer->GetSnapshot().live_docs_count());
  EXPECT_EQ(std::vector<std::string>{sources.back()}, CommittedNames(*_writer));
}

TEST_F(IndexAdoptTest, DocRemovalMasksOnlyTheListedDocs) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  for (const auto* value : {"a", "b", "c", "d"}) {
    ASSERT_TRUE(InsertDoc(trx, value));
  }
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto names = CommittedNames(*_writer);
  ASSERT_EQ(1, names.size());

  auto del = _writer->GetBatch();
  del.Remove(Positions({{names.front(), {4, 2}}}));
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(2, snapshot.live_docs_count());
  const auto* mask = snapshot[0].docs_mask();
  ASSERT_NE(nullptr, mask);
  EXPECT_FALSE(mask->Contains(1));
  EXPECT_TRUE(mask->Contains(2));
  EXPECT_FALSE(mask->Contains(3));
  EXPECT_TRUE(mask->Contains(4));
}

TEST_F(IndexAdoptTest, DocRemovalOfMaskedDocsChangesNothing) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  for (const auto* value : {"a", "b", "c"}) {
    ASSERT_TRUE(InsertDoc(trx, value));
  }
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto names = CommittedNames(*_writer);
  for (size_t round = 0; round != 2; ++round) {
    auto del = _writer->GetBatch();
    del.Remove(Positions({{names.front(), {2}}}));
    ASSERT_TRUE(del.Commit());
    _writer->RefreshCommit();
  }
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOnAPendingCompactionIsRemapped) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(del.Commit());
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
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  ASSERT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("a1"));
  survivors.Remove(ByName("b2"));
  ASSERT_TRUE(survivors.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOfAMergedAwaySegmentIsRemapped) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(del.Commit());
  _writer->RefreshCommit();

  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalIsRemappedThroughTwoMerges) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "1")));
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "2")));
    ASSERT_TRUE(trx.Commit());
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
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());

  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("a1"));
  survivors.Remove(ByName("b2"));
  survivors.Remove(ByName("c1"));
  ASSERT_TRUE(survivors.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocRemovalOfADocDeletedBeforeTwoMergesChangesNothing) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "1")));
    ASSERT_TRUE(InsertDoc(trx, absl::StrCat(value, "2")));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  const auto snapshot = _writer->GetSnapshot();
  {
    auto trx = _writer->GetBatch();
    trx.Remove(ByName("a2"));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, "c1"));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(_writer->RefreshCommit());
  ASSERT_EQ(1, CommittedNames(*_writer).size());
  ASSERT_EQ(4, _writer->GetSnapshot().live_docs_count());

  auto del = _writer->GetBatch();
  del.Remove(Positions(snapshot, {{sources.front(), {2}}}));
  ASSERT_TRUE(del.Commit());
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(4, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocMapsLastAsLongAsASnapshotOfTheirSource) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(del.Commit());
  _writer->RefreshCommit();
  EXPECT_EQ(6, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, CompactionDuringABegunCommitFindsNoRemovals) {
  const auto commit = [&](std::string_view value) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(pending.Commit());
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
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(pending.Commit());
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  auto stale = _writer->GetBatch();
  stale.Remove(Positions(before, {{sources.front(), {1}}}));
  ASSERT_TRUE(stale.Commit());
  _writer->RefreshCommit();
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());

  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("b"));
  survivors.Remove(ByName("c"));
  survivors.Remove(ByName("d"));
  ASSERT_TRUE(survivors.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, DocMapsLastWhileARemovalNamesTheirSource) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  commit("c");
  commit("d");
  ASSERT_FALSE(maps.expired()) << "the removal names the source";
  EXPECT_EQ(1, maps.lock()->maps.size());

  auto del = _writer->GetBatch();
  del.Remove(std::move(removal));
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());
  commit("e");
  EXPECT_TRUE(maps.expired()) << "the applied removal let its source go";

  auto survivors = _writer->GetBatch();
  survivors.Remove(ByName("a"));
  survivors.Remove(ByName("c"));
  survivors.Remove(ByName("d"));
  survivors.Remove(ByName("e"));
  ASSERT_TRUE(survivors.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AnAbortedCommitDropsTheDocMapsOfItsMerge) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(trx.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  const auto before = _writer->GetSnapshot()[0].Meta().version;

  auto del = _writer->GetBatch();
  del.Remove(Positions({}));
  ASSERT_TRUE(del.Commit());
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
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  const auto sources = CommittedNames(*_writer);
  ASSERT_EQ(2, sources.size());
  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("a1"));
    ASSERT_TRUE(del.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  ASSERT_TRUE(_writer->Compact(kFullMerge));

  auto del = _writer->GetBatch();
  del.Remove(Positions(_writer->GetSnapshot(),
                       {{sources.front(), {1, 2, 99}}, {sources.back(), {2}}}));
  ASSERT_TRUE(del.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(1, snapshot.live_docs_count());
  auto survivor = _writer->GetBatch();
  survivor.Remove(ByName("b1"));
  ASSERT_TRUE(survivor.Commit());
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(0, _writer->GetSnapshot().live_docs_count());
}

TEST_F(IndexAdoptTest, AReaderReopenedWithoutChangesKeepsTheDocMaps) {
  for (const auto* value : {"a", "b"}) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
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
    ASSERT_TRUE(trx.Commit());
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
  ASSERT_TRUE(trx.Commit());
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
    ASSERT_TRUE(trx.Commit());
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
    EXPECT_LE(SegmentIdOf(segment.Meta().name), guard.Floor());
  }
}

TEST_F(IndexAdoptTest,
       AMetaRewrittenForADeleteIsSyncedBesideAPendingCompaction) {
  for (const auto* value : {"merged_a", "merged_b", "untouched"}) {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, value));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "doomed"));
    ASSERT_TRUE(InsertDoc(trx, "kept"));
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  ASSERT_EQ(4, _writer->GetSnapshot().size());

  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "in_flight"));
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_EQ(irs::CompactionError::Pending,
            _writer->Compact(BudgetedPolicy(2)).error);
  _writer->RefreshCommit();

  {
    auto del = _writer->GetBatch();
    del.Remove(ByName("doomed"));
    ASSERT_TRUE(del.Commit());
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
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  {
    auto trx = _writer->GetBatch(true);
    ASSERT_TRUE(InsertDoc(trx, "other"));
    ASSERT_TRUE(trx.Commit());
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
      EXPECT_TRUE(del.Commit());
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
    ASSERT_TRUE(del.Commit());
  }

  _dir->TakeSynced();
  ASSERT_TRUE(_writer->RefreshCommit());

  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ(kDocs / 4 + 1, snapshot.live_docs_count());

  const auto& compacted = snapshot.Meta().index_meta.segments.front();
  ASSERT_NE(nullptr, compacted.meta.docs_mask);

  auto synced = _dir->TakeSynced();
  ASSERT_FALSE(synced.empty());
  EXPECT_TRUE(synced.back().starts_with("pending_segments_"));
  synced.pop_back();
  auto expected = irs::SegmentFiles(compacted.meta.name, compacted.meta.kinds);
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

#ifdef SDB_FAULT_INJECTION
TEST_F(IndexAdoptTest, ACommitJoinsTheFlushItsPooledSegmentIsPinnedTo) {
  constexpr std::string_view kSwitch = "pause_irs_commit_after_flush_switch";
  {
    auto seed = _writer->GetBatch();
    ASSERT_TRUE(InsertDoc(seed, "seed"));
    ASSERT_TRUE(seed.Commit());
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
  irs::IndexWriter::Transaction::Stage(commit);
  ASSERT_TRUE(pooled.Commit());
  ASSERT_TRUE(exclusive.Commit());
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
    ASSERT_TRUE(seed.Commit());
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
  irs::IndexWriter::Transaction::Stage(commit);
  ASSERT_TRUE(exclusive.Commit());
  ASSERT_TRUE(pooled.Commit());
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
        irs::IndexWriter::Transaction::Stage(commit);
        EXPECT_TRUE(pooled.Commit());
        EXPECT_TRUE(exclusive.Commit());
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
