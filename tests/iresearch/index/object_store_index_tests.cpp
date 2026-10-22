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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <atomic>
#include <chrono>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/store/index_files.hpp>
#include <iresearch/store/simulated_object_store.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

namespace {

duckdb::DatabaseInstance& Db() {
  return irs::DuckDBEngine::Instance().instance();
}

std::string Root() {
  static std::atomic<uint64_t> count{0};
  return absl::StrCat(irs::SimulatedObjectStore::kPrefix, "bucket/index-",
                      count.fetch_add(1, std::memory_order_relaxed));
}

void WriteSegments(const irs::IndexFiles& dir, size_t segments) {
  tests::JsonDocGenerator gen{TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory};
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                       irs::tests::DefaultWriterOptions());
  for (size_t i = 0; i != segments; ++i) {
    const auto* doc = gen.next();
    ASSERT_NE(nullptr, doc);
    ASSERT_TRUE(tests::Insert(*writer, *doc));
    ASSERT_TRUE(writer->RefreshCommit());
  }
}

void RemoveName(const irs::IndexFiles& dir, std::string_view name) {
  auto writer = irs::IndexWriter::Make(dir, irs::kOmAppend,
                                       irs::tests::DefaultWriterOptions());
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = tests::FieldIdFor("name");
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(name);
  auto ctx = writer->GetBatch();
  ctx.Remove(irs::Filter::ptr{std::move(filter)});
  ASSERT_TRUE(ctx.Commit());
  ASSERT_TRUE(writer->RefreshCommit());
}

irs::ObjectStoreRequests Open(const std::string& root, uint64_t live_docs) {
  auto& store = tests::ObjectStore();
  const irs::IndexFiles restarted{Db(), root};
  duckdb::BlockCache::Get(Db()).Clear();
  store.ResetRequests();
  const irs::DirectoryReader reader{restarted,
                                    irs::tests::DefaultReaderOptions()};
  const auto requests = store.Requests();
  EXPECT_EQ(live_docs, reader.live_docs_count());
  return requests;
}

TEST(ObjectStoreIndexTest, segments_open_without_head_requests) {
  tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, 3);

  const auto requests = Open(root, 3);
  EXPECT_EQ(0, requests.head);
  EXPECT_EQ(1, requests.list);
  EXPECT_EQ(1 + 3 + 3, requests.get);
}

TEST(ObjectStoreIndexTest, prefetched_term_blocks_serve_the_lookups) {
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, 3);
  const irs::IndexFiles restarted{Db(), root};
  const irs::DirectoryReader reader{restarted,
                                    irs::tests::DefaultReaderOptions()};
  duckdb::BlockCache::Get(Db()).Clear();
  store.ResetRequests();

  const auto term = irs::ViewCast<irs::byte_type>(std::string_view{"xyz"});
  irs::ByTerm filter;
  *filter.mutable_field_id() = tests::FieldIdFor("same");
  filter.mutable_options()->term = term;
  for (const auto& segment : reader) {
    EXPECT_TRUE(filter.Prefetch(segment));
  }
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds{30};
  while (store.Requests().get < 3 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  ASSERT_EQ(3, store.Requests().get);

  for (const auto& segment : reader) {
    const auto* terms = segment.field(tests::FieldIdFor("same"));
    ASSERT_NE(nullptr, terms);
    EXPECT_EQ(1, terms->Lookup(term).docs_count);
    EXPECT_FALSE(filter.Prefetch(segment));
  }
  EXPECT_EQ(3, store.Requests().get);
}

TEST(ObjectStoreIndexTest, rewritten_segment_metas_keep_file_infos) {
  tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, 2);
  RemoveName(irs::IndexFiles{Db(), root}, "A");

  const auto requests = Open(root, 1);
  EXPECT_EQ(0, requests.head);
}

TEST(ObjectStoreIndexTest, segments_without_file_infos_open) {
  tests::ObjectStore();
  if (!irs::AddFailurePointDebugging("segment_meta_without_file_info")) {
    GTEST_SKIP() << "built without fault injection";
  }
  absl::Cleanup clear = [] { irs::ClearFailurePointsDebugging(); };
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, 3);
  irs::ClearFailurePointsDebugging();

  const auto old = Open(root, 3);
  EXPECT_EQ(0, old.head);
  EXPECT_LT(1 + 3 + 3, old.get);

  RemoveName(irs::IndexFiles{Db(), root}, "B");
  EXPECT_LT(1 + 3 + 3, Open(root, 2).get);
}

TEST(ObjectStoreIndexTest, segments_open_in_parallel) {
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, 4);
  store.SetLatency(std::chrono::milliseconds{20});
  absl::Cleanup no_latency = [&] {
    store.SetLatency(std::chrono::microseconds{0});
  };

  const auto requests = Open(root, 4);
  EXPECT_EQ(0, requests.head);
  EXPECT_EQ(1 + 4 + 4, requests.get);
  EXPECT_LT(1, requests.max_in_flight);
}

TEST(ObjectStoreIndexTest, writer_opens_segments_in_parallel) {
  constexpr size_t kSegments = 16;
  constexpr std::chrono::milliseconds kLatency{50};
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, kSegments);
  const irs::IndexFiles restarted{Db(), root};
  duckdb::BlockCache::Get(Db()).Clear();
  store.SetLatency(kLatency);
  absl::Cleanup no_latency = [&] {
    store.SetLatency(std::chrono::microseconds{0});
  };

  store.ResetRequests();
  const auto began = std::chrono::steady_clock::now();
  const auto writer = irs::IndexWriter::Make(
    restarted, irs::kOmAppend, irs::tests::DefaultWriterOptions());
  const auto elapsed = std::chrono::steady_clock::now() - began;
  ASSERT_NE(nullptr, writer);
  EXPECT_EQ(kSegments, writer->GetSnapshot().live_docs_count());
  const auto r = store.Requests();
  EXPECT_LT(elapsed, kSegments * kLatency)
    << "get " << r.get << " put " << r.put << " head " << r.head << " list "
    << r.list << " remove " << r.remove << " copy " << r.copy;
}

TEST(ObjectStoreIndexTest, cleanup_removes_files_in_one_request) {
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::IndexFiles{Db(), root}, 4);
  const irs::IndexFiles restarted{Db(), root};
  store.ResetRequests();

  const auto writer = irs::IndexWriter::Make(
    restarted, irs::kOmAppend, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);
  EXPECT_EQ(1, store.Requests().remove);
  bool exists = true;
  ASSERT_TRUE(restarted.Exists(exists, "segments_1"));
  EXPECT_FALSE(exists);
}

TEST(ObjectStoreIndexTest, replace_adopts_segments) {
  tests::ObjectStore();
  const irs::IndexFiles dir{Db(), Root()};
  tests::JsonDocGenerator gen{TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory};
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                       irs::tests::DefaultWriterOptions());
  for (size_t i = 0; i != 2; ++i) {
    ASSERT_TRUE(tests::Insert(*writer, *gen.next()));
    ASSERT_TRUE(writer->RefreshCommit());
  }
  std::vector<std::string> sources;
  for (const auto& segment : writer->GetSnapshot()) {
    sources.emplace_back(segment.Meta().name);
  }

  std::vector<std::string> adopted;
  for (size_t i = 0; i != 4; ++i) {
    const auto* doc = gen.next();
    ASSERT_NE(nullptr, doc);
    auto build = writer->GetBatch(/*exclusive_segment=*/true);
    ASSERT_TRUE(tests::InsertFields(build.Insert(), doc->indexed.begin(),
                                    doc->indexed.end()));
    for (const auto& flushed : build.FlushAndFsync()) {
      adopted.emplace_back(flushed.filename);
    }
    build.Abort();
  }

  const std::vector<std::string_view> replaced{sources.begin(), sources.end()};
  const std::vector<std::string_view> adopting{adopted.begin(), adopted.end()};
  writer->ReplaceSegments(
    replaced, adopting,
    [](irs::IndexWriter::QueryContext::FilterPtr&) { return true; });
  ASSERT_TRUE(writer->RefreshCommit());
  const auto snapshot = writer->GetSnapshot();
  EXPECT_EQ(4, snapshot.size());
  EXPECT_EQ(4, snapshot.live_docs_count());
}

constexpr uint64_t kBlock = duckdb::BlockCache::BLOCK_SIZE;
constexpr uint64_t kBlocks = 16;

irs::IndexInput::ptr WriteUncached(const irs::IndexFiles& dir) {
  const std::vector<irs::byte_type> block(kBlock, 0x5A);
  {
    auto out = dir.Create("blocks");
    for (uint64_t i = 0; i != kBlocks; ++i) {
      out->WriteData(block.data(), block.size());
    }
  }
  duckdb::BlockCache::Get(Db()).Clear();
  auto in = dir.Open("blocks", irs::IOAdvice::NORMAL);
  tests::ObjectStore().ResetRequests();
  return in;
}

TEST(ObjectStoreIndexTest, sequential_reads_fetch_ahead) {
  tests::ObjectStore();
  const irs::IndexFiles dir{Db(), Root()};
  auto in = WriteUncached(dir);
  while (!in->IsEOF()) {
    std::ignore = in->ReadByte();
  }
  EXPECT_EQ(4, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, large_reads_fetch_their_range_at_once) {
  tests::ObjectStore();
  const irs::IndexFiles dir{Db(), Root()};
  auto in = WriteUncached(dir);
  irs::bstring buf(kBlocks * kBlock, 0);
  in->ReadData(buf.data(), buf.size());
  EXPECT_EQ(1, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, random_reads_fetch_only_their_blocks) {
  tests::ObjectStore();
  const irs::IndexFiles dir{Db(), Root()};
  auto in = WriteUncached(dir);
  for (uint64_t block = 0; block < kBlocks; block += 2) {
    in->Seek(block * kBlock);
    std::ignore = in->ReadByte();
  }
  EXPECT_EQ(kBlocks / 2, tests::ObjectStore().Requests().get);

  irs::bstring buf(2 * kBlock + 2, 0);
  in->ReadData(3 * kBlock - 1, buf.data(), buf.size());
  EXPECT_EQ(kBlocks / 2 + 1, tests::ObjectStore().Requests().get);
}

}  // namespace
