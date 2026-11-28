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
#include <absl/crc/crc32c.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/ivf/centroids.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/search/detail/bitset_build.hpp>
#include <iresearch/search/detail/enc_buf.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/queries/posting_heads.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "index_dir.hpp"
#include "store/simulated_object_store.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"
#include "utils/write_helpers.hpp"

namespace {

duckdb::DatabaseInstance& Db() {
  return irs::DuckDBEngine::Instance().instance();
}

void DropCachedBlocks(duckdb::ExternalFileCache& cache) {
  cache.SetEnabled(false);
  cache.SetEnabled(true);
}

std::string Root() {
  static std::atomic<uint64_t> count{0};
  return absl::StrCat(irs::SimulatedObjectStore::kPrefix, "bucket/index-",
                      count.fetch_add(1, std::memory_order_relaxed));
}

TEST(ObjectStoreIndexTest, a_single_block_field_prefetches_nothing) {
  auto& store = tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    for (size_t segment = 0; segment != 3; ++segment) {
      tests::JsonDocGenerator gen{TestBase::resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory};
      for (size_t doc = 0; doc != 8; ++doc) {
        ASSERT_TRUE(tests::Insert(*writer, *gen.next()));
      }
      ASSERT_TRUE(writer->RefreshCommit());
    }
  }
  const auto restarted = dir.Reopened();
  const auto reader =
    tests::OpenReader(restarted, irs::tests::DefaultReaderOptions());
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  store.ResetRequests();

  const auto term = irs::ViewCast<irs::byte_type>(std::string_view{"A"});
  irs::ByTerm filter;
  *filter.mutable_field_id() = tests::FieldIdFor("name");
  filter.mutable_options()->term = term;
  for (const auto& segment : reader) {
    EXPECT_FALSE(filter.Prefetch(segment));
  }
  for (const auto& segment : reader) {
    const auto* terms = segment.field(tests::FieldIdFor("name"));
    ASSERT_NE(nullptr, terms);
    EXPECT_EQ(1, terms->Lookup(term).docs_count);
  }
  EXPECT_EQ(0, store.Requests().get);
}

TEST(ObjectStoreIndexTest, prefetched_term_blocks_serve_the_lookups) {
  constexpr size_t kNames = 4096;
  auto& store = tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    for (size_t segment = 0; segment != 3; ++segment) {
      auto trx = writer->GetBatch();
      for (size_t i = 0; i != kNames; ++i) {
        tests::StringField field{"name", absl::StrCat("name-", i)};
        field.id = tests::FieldIdFor("name");
        ASSERT_TRUE(tests::InsertFields(trx.Insert(), &field, &field + 1));
      }
      ASSERT_TRUE(trx.Commit());
      ASSERT_TRUE(writer->RefreshCommit());
    }
  }
  const auto restarted = dir.Reopened();
  const auto reader =
    tests::OpenReader(restarted, irs::tests::DefaultReaderOptions());
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  store.ResetRequests();

  const auto term =
    irs::ViewCast<irs::byte_type>(std::string_view{"name-2048"});
  irs::ByTerm filter;
  *filter.mutable_field_id() = tests::FieldIdFor("name");
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
    const auto* terms = segment.field(tests::FieldIdFor("name"));
    ASSERT_NE(nullptr, terms);
    EXPECT_EQ(1, terms->Lookup(term).docs_count);
    EXPECT_FALSE(filter.Prefetch(segment));
  }
  EXPECT_EQ(3, store.Requests().get);
}

TEST(ObjectStoreIndexTest, released_segments_go_in_one_request) {
  auto& store = tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  tests::JsonDocGenerator gen{TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory};
  auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
  for (size_t i = 0; i != 4; ++i) {
    const auto* doc = gen.next();
    ASSERT_NE(nullptr, doc);
    ASSERT_TRUE(tests::Insert(*writer, *doc));
    ASSERT_TRUE(writer->RefreshCommit());
  }
  const auto sources = dir.Files().size();
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  ASSERT_TRUE(writer->RefreshCommit());
  ASSERT_EQ(1, dir.Committed().size());
  store.ResetRequests();

  EXPECT_EQ(sources, dir.RemoveReleased());
  EXPECT_EQ(1, store.Requests().remove);
  EXPECT_EQ(4, writer->GetSnapshot().live_docs_count());
}

constexpr uint64_t kBlock = duckdb::ExternalFileCache::DEFAULT_BLOCK_SIZE;
constexpr uint64_t kPieceSize = 4 * 1024 * 1024;
constexpr uint64_t kBlocks = 16;

duckdb::shared_ptr<duckdb::InputFile> WriteUncached(
  const tests::IndexDir& dir) {
  const std::vector<irs::byte_type> block(kBlock, 0x5A);
  {
    auto out = dir.Create("blocks");
    for (uint64_t i = 0; i != kBlocks; ++i) {
      out->WriteData(block.data(), block.size());
    }
    out->Close();
  }
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  auto file = dir.Open("blocks", duckdb::MappedFileAccess::NORMAL);
  tests::ObjectStore().ResetRequests();
  return file;
}

TEST(ObjectStoreIndexTest, sequential_reads_fetch_ahead) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  const auto file = WriteUncached(dir);
  duckdb::InputFileReader in{*file};
  while (in.Position() < file->GetSize()) {
    std::ignore = in.Read<irs::byte_type>();
  }
  EXPECT_EQ(4, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, large_reads_fetch_their_range_at_once) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  const auto file = WriteUncached(dir);
  duckdb::InputFileReader in{*file};
  irs::bstring buf(kBlocks * kBlock, 0);
  in.ReadData(buf.data(), buf.size());
  EXPECT_EQ(1, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, large_reads_fetch_their_range_past_capped_hints) {
  constexpr uint64_t kHinted = duckdb::ExternalFileCache::MAX_FILE_IN_FLIGHT;
  constexpr uint64_t kRead = kBlocks * kBlock;
  auto& store = tests::ObjectStore();
  auto& cache = duckdb::ExternalFileCache::Get(Db());
  const tests::IndexDir dir{Db(), Root()};
  {
    const std::vector<irs::byte_type> block(kBlock, 0x5A);
    auto out = dir.Create("large");
    for (uint64_t written = 0; written != kHinted + kRead; written += kBlock) {
      out->WriteData(block.data(), block.size());
    }
    out->Close();
  }
  DropCachedBlocks(cache);
  const auto file = dir.Open("large", duckdb::MappedFileAccess::NORMAL);
  store.SetLatency(std::chrono::milliseconds{100});
  absl::Cleanup restore = [&] {
    store.SetLatency(std::chrono::microseconds{0});
  };
  store.ResetRequests();
  file->Prefetch(0, kHinted);
  duckdb::InputFileReader in{*file};
  in.Seek(kHinted);
  irs::bstring buf(kRead, 0);
  in.ReadData(buf.data(), buf.size());
  EXPECT_EQ(irs::bstring(kRead, 0x5A), buf);
  irs::bstring hinted(kHinted, 0);
  in.Seek(0);
  in.ReadData(hinted.data(), hinted.size());
  EXPECT_EQ(kHinted / kPieceSize + 1, store.Requests().get);
}

struct Rows {
  std::span<const uint64_t> rows;
  size_t size() const noexcept { return rows.size(); }
  uint64_t operator[](size_t i) const noexcept { return rows[i]; }
};

int64_t Scrambled(uint64_t row) {
  return static_cast<int64_t>(row * 0x9E3779B97F4A7C15ULL);
}

TEST(ObjectStoreIndexTest, collected_column_blocks_serve_gathers) {
  constexpr uint32_t kRgSize = 64 * 1024;
  constexpr uint64_t kRows = 4 * kRgSize;
  constexpr duckdb::idx_t kDim = 3;
  constexpr irs::field_id kPlain = 1;
  constexpr irs::field_id kStruct = 2;
  constexpr irs::field_id kArray = 3;
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);
  const auto atype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::BIGINT, kDim);
  const auto valid = [](uint64_t row) { return row % 7 != 0; };

  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    irs::ColWriter writer{Db(), dir.Tracker("seg"), TestAlloc()};
    auto& plain =
      writer.OpenColumn(kPlain, duckdb::LogicalType::BIGINT,
                        {.row_group_size = kRgSize,
                         .compression = irs::codecs::CodecType::Uncompressed});
    auto& structs =
      writer.OpenColumn(kStruct, stype, {.row_group_size = kRgSize});
    auto& arrays =
      writer.OpenColumn(kArray, atype, {.row_group_size = kRgSize});
    for (uint64_t pos = 0; pos != kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector p{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      duckdb::Vector s{stype, STANDARD_VECTOR_SIZE};
      duckdb::Vector a{atype, STANDARD_VECTOR_SIZE};
      auto& entries = duckdb::StructVector::GetEntries(s);
      auto& child = duckdb::ArrayVector::GetChildMutable(a);
      auto* pd = duckdb::FlatVector::GetDataMutable<int64_t>(p);
      auto* sa = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
      auto* sb =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(entries[1]);
      auto* ad = duckdb::FlatVector::GetDataMutable<int64_t>(child);
      auto& pv = duckdb::FlatVector::ValidityMutable(p);
      auto& sv = duckdb::FlatVector::ValidityMutable(s);
      auto& av = duckdb::FlatVector::ValidityMutable(a);
      pv.Reset(STANDARD_VECTOR_SIZE);
      sv.Reset(STANDARD_VECTOR_SIZE);
      av.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k != STANDARD_VECTOR_SIZE; ++k) {
        const auto row = pos + k;
        pd[k] = Scrambled(row);
        sa[k] = Scrambled(row + 1);
        sb[k] = duckdb::StringVector::AddString(entries[1],
                                                absl::StrCat("v", row % 1000));
        for (duckdb::idx_t i = 0; i != kDim; ++i) {
          ad[k * kDim + i] = Scrambled(row * kDim + i);
        }
        if (!valid(row)) {
          pv.SetInvalid(k);
          sv.SetInvalid(k);
          av.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(p, STANDARD_VECTOR_SIZE);
      duckdb::FlatVector::SetSize(s, STANDARD_VECTOR_SIZE);
      duckdb::FlatVector::SetSize(a, STANDARD_VECTOR_SIZE);
      plain.Append(p, STANDARD_VECTOR_SIZE);
      structs.Append(s, STANDARD_VECTOR_SIZE);
      arrays.Append(a, STANDARD_VECTOR_SIZE);
    }
    writer.Commit(0);
  }
  const irs::ColReader reader = irs::tests::OpenColReader(dir, "seg");
  const auto* plain = reader.Column(kPlain);
  ASSERT_NE(nullptr, plain);
  ASSERT_TRUE(plain->HasValidity());

  const std::vector<uint64_t> rows{kRgSize + 10, kRgSize + 30000,
                                   2 * kRgSize + 5};
  std::vector<duckdb::FileRange> extents;
  plain->CollectBlocks(rows.front(), rows.back(), extents);
  std::vector<duckdb::FileRange> expected;
  for (const auto* column : {plain, plain->Validity()}) {
    const auto blocks = column->DataBlocks();
    for (size_t block = 0; block != blocks.size(); ++block) {
      if (column->DataBlockFirstRow(block + 1) > rows.front() &&
          column->DataBlockFirstRow(block) <= rows.back() &&
          blocks[block].byte_size != 0) {
        expected.push_back({.offset = blocks[block].file_offset,
                            .size = blocks[block].byte_size});
      }
    }
  }
  ASSERT_LT(4, expected.size());
  const auto key = [](const duckdb::FileRange& e) {
    return std::pair{e.offset, e.size};
  };
  ASSERT_EQ(expected.size(), extents.size());
  for (size_t i = 0; i != extents.size(); ++i) {
    EXPECT_EQ(key(expected[i]), key(extents[i])) << i;
  }

  for (const auto id : {kStruct, kArray}) {
    const auto* column = reader.Column(id);
    ASSERT_NE(nullptr, column);
    column->CollectBlocks(rows.front(), rows.back(), extents);
  }
  auto& cache = duckdb::ExternalFileCache::Get(Db());
  DropCachedBlocks(cache);
  for (auto& extent : extents) {
    reader.File()->Prefetch({&extent, 1});
  }
  const auto prefetched = cache.GetStatistics().misses;

  irs::ReadContext ctx{reader, TestAlloc()};
  const duckdb::LogicalType btype{duckdb::LogicalTypeId::BIGINT};
  for (const auto& [id, type] :
       {std::pair{kPlain, btype}, std::pair{kStruct, stype},
        std::pair{kArray, atype}}) {
    const auto& column = *reader.Column(id);
    auto state = column.InitScan(ctx);
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    irs::column_internal::GatherRows(column, state, Rows{rows}, out, 0);
    if (id == kPlain) {
      out.Flatten(rows.size());
      const auto* data = duckdb::FlatVector::GetData<int64_t>(out);
      const auto& validity = duckdb::FlatVector::Validity(out);
      for (size_t i = 0; i != rows.size(); ++i) {
        ASSERT_EQ(valid(rows[i]), validity.RowIsValid(i)) << rows[i];
        if (valid(rows[i])) {
          EXPECT_EQ(Scrambled(rows[i]), data[i]) << rows[i];
        }
      }
    }
  }
  EXPECT_EQ(prefetched, cache.GetStatistics().misses);
}

int64_t Mixed(uint64_t row) {
  auto bits = static_cast<uint64_t>(Scrambled(row));
  bits ^= bits >> 31;
  bits *= 0xD6E8FEB86659FD93ULL;
  return static_cast<int64_t>(bits ^ (bits >> 29));
}

void WriteMixedColumn(const tests::IndexDir& dir, irs::field_id id,
                      uint32_t rg_size, uint64_t rows) {
  irs::ColWriter writer{Db(), dir.Tracker("seg"), TestAlloc()};
  auto& plain =
    writer.OpenColumn(id, duckdb::LogicalType::BIGINT,
                      {.skip_validity = true, .row_group_size = rg_size});
  for (uint64_t pos = 0; pos != rows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector p{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* pd = duckdb::FlatVector::GetDataMutable<int64_t>(p);
    for (duckdb::idx_t k = 0; k != STANDARD_VECTOR_SIZE; ++k) {
      pd[k] = Mixed(pos + k);
    }
    duckdb::FlatVector::SetSize(p, STANDARD_VECTOR_SIZE);
    plain.Append(p, STANDARD_VECTOR_SIZE);
  }
  writer.Commit(0);
}

TEST(ObjectStoreIndexTest, a_column_scan_fetches_ahead_in_one_request) {
  constexpr uint32_t kRgSize = 16 * 1024;
  constexpr uint64_t kRows = 32 * kRgSize;
  constexpr irs::field_id kPlain = 1;
  auto& store = tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  WriteMixedColumn(dir, kPlain, kRgSize, kRows);
  const irs::ColReader reader = irs::tests::OpenColReader(dir, "seg");
  const auto* column = reader.Column(kPlain);
  ASSERT_NE(nullptr, column);
  const auto blocks = column->DataBlocks();
  ASSERT_EQ(kRows / kRgSize, blocks.size());
  for (const auto& block : blocks) {
    ASSERT_LE(kRgSize * sizeof(int64_t), block.byte_size);
    ASSERT_GT(kRgSize * sizeof(int64_t) + 4096, block.byte_size);
  }

  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  store.ResetRequests();
  irs::ReadContext ctx{reader, TestAlloc()};
  auto state = column->InitScan(ctx);
  for (uint64_t pos = 0; pos != kRows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    column->Scan(state, out, STANDARD_VECTOR_SIZE);
    out.Flatten(STANDARD_VECTOR_SIZE);
    const auto* data = duckdb::FlatVector::GetData<int64_t>(out);
    for (duckdb::idx_t k = 0; k != STANDARD_VECTOR_SIZE; ++k) {
      ASSERT_EQ(Mixed(pos + k), data[k]) << pos + k;
    }
  }
  EXPECT_EQ(3, store.Requests().get);
}

TEST(ObjectStoreIndexTest, a_centroid_layer_is_fetched_in_one_request) {
  constexpr uint32_t kDim = 128;
  constexpr size_t kNodes = 8;
  constexpr size_t kChildren = 256;
  auto& store = tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  uint64_t root_size = 0;
  {
    irs::CentroidsNode root{1, kDim, TestAlloc()};
    root.size = kNodes;
    root.centroids.resize(kNodes * kDim);
    for (size_t i = 0; i != kNodes; ++i) {
      std::fill_n(root.centroids.begin() + i * kDim, kDim,
                  static_cast<float>(i));
      root.child_offsets.push_back(i * kChildren);
    }
    root.child_offsets.push_back(kNodes * kChildren);
    irs::CentroidsNode leaves{0, kDim, TestAlloc()};
    leaves.size = kNodes * kChildren;
    leaves.centroids.resize(leaves.size * kDim);
    for (size_t i = 0; i != leaves.size; ++i) {
      std::fill_n(leaves.centroids.begin() + i * kDim, kDim,
                  static_cast<float>(i) / kChildren);
    }
    auto out = dir.Create("centroids");
    irs::IVFHeader{.metric = irs::VectorMetric::L2Sqr, .d = kDim}.Serialize(
      *out);
    out->Write<uint64_t>(root.level);
    root.Serialize(*out);
    root_size = out->Position();
    leaves.Serialize(*out);
    out->Close();
  }
  ASSERT_LT(root_size, kBlock);
  const auto file = dir.Open("centroids", duckdb::MappedFileAccess::NORMAL);
  ASSERT_LT(kNodes * kBlock, file->GetSize());
  const auto tree = [&] {
    duckdb::InputFileReader in{*file};
    return irs::CentroidsTree::Deserialize(in, root_size, TestAlloc());
  }();
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  store.ResetRequests();
  duckdb::InputFileReader in{*file};
  const std::vector<float> query(kDim, 0.F);
  irs::TrackedVector<uint32_t> ids{TestAlloc()};
  tree.Search(query, in, kNodes, ids, nullptr);
  EXPECT_EQ(kNodes, ids.size());
  EXPECT_EQ(2, store.Requests().get);
}

TEST(ObjectStoreIndexTest, gathers_read_column_blocks_across_cache_blocks) {
  constexpr uint32_t kRgSize = 64 * 1024;
  constexpr uint64_t kRows = 4 * kRgSize;
  constexpr irs::field_id kPlain = 1;
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  WriteMixedColumn(dir, kPlain, kRgSize, kRows);
  const irs::ColReader reader = irs::tests::OpenColReader(dir, "seg");
  const auto* column = reader.Column(kPlain);
  ASSERT_NE(nullptr, column);
  const auto blocks = column->DataBlocks();
  std::vector<uint64_t> rows;
  for (size_t block = 0; block != blocks.size(); ++block) {
    if (blocks[block].byte_size > reader.File()->BlockSize()) {
      rows.push_back(column->DataBlockFirstRow(block) + 5);
    }
  }
  ASSERT_LT(4, rows.size());

  auto& cache = duckdb::ExternalFileCache::Get(Db());
  const auto gather = [&] {
    irs::ReadContext ctx{reader, TestAlloc()};
    auto state = column->InitScan(ctx);
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    irs::column_internal::GatherRows(*column, state, Rows{rows}, out, 0);
    out.Flatten(rows.size());
    const auto* data = duckdb::FlatVector::GetData<int64_t>(out);
    for (size_t i = 0; i != rows.size(); ++i) {
      EXPECT_EQ(Mixed(rows[i]), data[i]) << rows[i];
    }
  };
  DropCachedBlocks(cache);
  gather();
  const auto fetches = cache.GetStatistics().fetches;
  gather();
  EXPECT_EQ(fetches, cache.GetStatistics().fetches);
}

TEST(ObjectStoreIndexTest, collected_variant_blocks_serve_gathers) {
  constexpr uint32_t kRgSize = 64 * 1024;
  constexpr irs::field_id kVariant = 4;
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    irs::ColWriter writer{Db(), dir.Tracker("seg"), TestAlloc()};
    auto& variants = writer.OpenColumn(kVariant, duckdb::LogicalType::VARIANT(),
                                       {.row_group_size = kRgSize});
    duckdb::Connection con{Db()};
    auto result = con.Query(
      absl::StrCat("SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE "
                   "{'a': i, 'b': 'v' || (i % 1000)}::VARIANT END FROM range(",
                   4 * kRgSize, ") t(i) ORDER BY i"));
    ASSERT_FALSE(result->HasError()) << result->GetError();
    while (auto chunk = result->Fetch()) {
      if (chunk->size() != 0) {
        variants.Append(chunk->data[0], chunk->size());
      }
    }
    writer.Commit(0);
  }
  const irs::ColReader reader = irs::tests::OpenColReader(dir, "seg");
  const auto* column = reader.Column(kVariant);
  ASSERT_NE(nullptr, column);

  const std::vector<uint64_t> rows{kRgSize + 10, kRgSize + 30000,
                                   2 * kRgSize + 5};
  std::vector<duckdb::FileRange> extents;
  column->CollectBlocks(rows.front(), rows.back(), extents);
  ASSERT_FALSE(extents.empty());
  auto& cache = duckdb::ExternalFileCache::Get(Db());
  DropCachedBlocks(cache);
  for (auto& extent : extents) {
    reader.File()->Prefetch({&extent, 1});
  }
  const auto prefetched = cache.GetStatistics().misses;

  irs::ReadContext ctx{reader, TestAlloc()};
  auto state = column->InitScan(ctx);
  duckdb::Vector out{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(*column, state, Rows{rows}, out, 0);
  for (size_t i = 0; i != rows.size(); ++i) {
    EXPECT_EQ(rows[i] % 7 == 0, out.GetValue(i).IsNull()) << rows[i];
  }
  EXPECT_EQ(prefetched, cache.GetStatistics().misses);
}

TEST(ObjectStoreIndexTest, reads_past_the_end_are_refused) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    auto out = dir.Create("short");
    out->Write<uint32_t>(42);
    out->Close();
  }
  const auto file = dir.Open("short", duckdb::MappedFileAccess::NORMAL);
  duckdb::InputFileReader in{*file};
  EXPECT_EQ(42, in.Read<int32_t>());
  EXPECT_THROW(in.Read<irs::byte_type>(), duckdb::DataCorruptionException);
  in.Seek(2);
  irs::byte_type buf[8];
  EXPECT_THROW(in.ReadData(buf, sizeof(buf)), duckdb::DataCorruptionException);
}

TEST(ObjectStoreIndexTest, a_listing_skips_nested_objects) {
  tests::ObjectStore();
  const auto root = Root();
  const tests::IndexDir dir{Db(), root};
  dir.Create("top")->Close();
  const tests::IndexDir nested{Db(), absl::StrCat(root, "/nested")};
  nested.Create("inner")->Close();

  EXPECT_EQ(std::vector<std::string>{"top"}, dir.Files());
}

TEST(ObjectStoreIndexTest, posting_heads_prefetch_every_clause) {
  constexpr size_t kDocs = 20000;
  constexpr irs::field_id kWords = 1;
  constexpr irs::field_id kText = 2;
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    tests::Document doc;
    auto words =
      std::make_shared<tests::StringField>("words", irs::IndexFeatures::Freq);
    words->id = kWords;
    auto text = std::make_shared<tests::TextField<std::string>>("text");
    text->id = kText;
    doc.insert(words);
    doc.insert(text);
    for (size_t i = 0; i != kDocs; ++i) {
      words->value(i % 2 == 0 ? "long" : (i % 100 == 1 ? "short" : "other"));
      text->value(i % 50 == 0 ? "quick fox" : "fox quick");
      ASSERT_TRUE(tests::Insert(*writer, doc));
    }
    ASSERT_TRUE(writer->RefreshCommit());
  }
  const auto reader =
    tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  const auto& segment = reader[0];

  const auto meta = [&](irs::field_id field, std::string_view term) {
    const auto* terms = segment.field(field);
    EXPECT_NE(nullptr, terms);
    return std::pair{terms->Handles(),
                     terms->Lookup(irs::ViewCast<irs::byte_type>(term))};
  };
  const auto [words, long_list] = meta(kWords, "long");
  const auto short_list = meta(kWords, "short").second;
  ASSERT_LT(irs::doc_limits::kBlockSize, long_list.docs_count);
  ASSERT_LT(1, short_list.docs_count);

  const auto clause = [](std::string_view term) {
    return irs::TermClause{
      .field = kWords,
      .term = irs::bstring{irs::ViewCast<irs::byte_type>(term)}};
  };
  irs::QueryMemory memory;
  irs::BooleanFilter only;
  only.Add(clause("long"), irs::Occur::Must);
  const auto only_query = only.PrepareSegment(segment, {.memory = memory});
  irs::BooleanFilter either;
  either.Add(clause("long"), irs::Occur::Should);
  either.Add(clause("short"), irs::Occur::Should);
  either.SetMinShouldMatch(1);
  const auto terms_query = either.PrepareSegment(segment, {.memory = memory});
  auto& cache = duckdb::ExternalFileCache::Get(Db());
  DropCachedBlocks(cache);
  irs::PrefetchPostingHeads(*only_query, 2);
  EXPECT_FALSE(words.doc->IsResident(long_list.doc_start, 1));
  irs::PrefetchPostingHeads(*terms_query, 2);
  EXPECT_TRUE(words.doc->IsResident(long_list.doc_start, 1));
  EXPECT_TRUE(
    words.doc->IsResident(long_list.doc_start + long_list.doc_delta, 1));
  EXPECT_TRUE(words.doc->IsResident(short_list.doc_start, 1));

  irs::ByPhrase phrase;
  *phrase.mutable_field_id() = kText;
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  const auto phrase_query = phrase.PrepareSegment(segment, {.memory = memory});
  const auto [text, quick] = meta(kText, "quick");
  ASSERT_NE(nullptr, text.pos);
  DropCachedBlocks(cache);
  EXPECT_FALSE(text.pos->IsResident(quick.pos_start, 1));
  irs::PrefetchPostingHeads(*phrase_query, 1);
  EXPECT_TRUE(text.doc->IsResident(quick.doc_start, 1));
  EXPECT_TRUE(text.pos->IsResident(quick.pos_start, 1));

  irs::ByPrefix prefix;
  *prefix.mutable_field_id() = kWords;
  prefix.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"sh"});
  const auto prefix_query = prefix.PrepareSegment(segment, {.memory = memory});
  DropCachedBlocks(cache);
  EXPECT_FALSE(words.doc->IsResident(short_list.doc_start, 1));
  irs::PrefetchPostingHeads(*prefix_query, 1);
  EXPECT_TRUE(words.doc->IsResident(short_list.doc_start, 1));

  irs::ByPhrase variadic;
  *variadic.mutable_field_id() = kText;
  variadic.mutable_options()->push_back<irs::ByPrefixOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"qui"});
  variadic.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  const auto variadic_query =
    variadic.PrepareSegment(segment, {.memory = memory});
  DropCachedBlocks(cache);
  EXPECT_FALSE(text.pos->IsResident(quick.pos_start, 1));
  irs::PrefetchPostingHeads(*variadic_query, 1);
  EXPECT_TRUE(text.doc->IsResident(quick.doc_start, 1));
  EXPECT_TRUE(text.pos->IsResident(quick.pos_start, 1));
}

TEST(ObjectStoreIndexTest, random_reads_fetch_only_their_blocks) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  const auto file = WriteUncached(dir);
  duckdb::InputFileReader in{*file};
  for (uint64_t block = 0; block < kBlocks; block += 2) {
    in.Seek(block * kBlock);
    std::ignore = in.Read<irs::byte_type>();
  }
  EXPECT_EQ(kBlocks / 2, tests::ObjectStore().Requests().get);

  irs::bstring buf(2 * kBlock + 2, 0);
  in.Seek(3 * kBlock - 1);
  in.ReadData(buf.data(), buf.size());
  EXPECT_EQ(kBlocks / 2 + 1, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, a_long_one_time_read_fetches_every_block_in_runs) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  constexpr uint64_t kSize = uint64_t{200} << 20;
  const std::vector<irs::byte_type> block(kBlock, 0x5A);
  {
    auto out = dir.Create("long");
    for (uint64_t written = 0; written < kSize; written += kBlock) {
      out->WriteData(block.data(), block.size());
    }
    out->Close();
  }
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  const auto file = dir.Open("long", duckdb::MappedFileAccess::NORMAL);
  tests::ObjectStore().ResetRequests();
  duckdb::InputFileReader in{*file};
  std::vector<irs::byte_type> read(kSize);
  in.ReadOnce(read.data(), kSize);
  EXPECT_EQ(kSize, static_cast<uint64_t>(absl::c_count(read, 0x5A)));
  EXPECT_LE(tests::ObjectStore().Requests().get, 2 * kSize / kPieceSize);
}

struct LeafSink {
  static constexpr bool kOrdered = true;

  void Run(uint64_t prev, uint32_t len) {
    for (uint32_t i = 1; i <= len; ++i) {
      docs.push_back(static_cast<irs::doc_id_t>(prev + i));
    }
  }

  void Bitset(uint64_t prev, const irs::UnalignedWord* bits, uint32_t words,
              uint64_t) {
    positions.push_back(in->Position());
    for (uint32_t i = 0; i != words * 64; ++i) {
      if ((bits[i / 64] >> (i % 64)) & 1) {
        docs.push_back(static_cast<irs::doc_id_t>(prev + 1 + i));
      }
    }
  }

  void Doc(size_t doc) { docs.push_back(static_cast<irs::doc_id_t>(doc)); }

  const duckdb::InputFileReader* in = nullptr;
  std::vector<irs::doc_id_t> docs;
  std::vector<uint64_t> positions;
};

TEST(ObjectStoreIndexTest,
     a_bitset_leaf_reaches_its_sink_in_the_block_it_lies_in) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  constexpr uint32_t kLen = irs::doc_limits::kBlockSize;
  std::vector<irs::doc_id_t> sparse(kLen);
  std::vector<irs::doc_id_t> dense;
  for (uint32_t i = 0; i != kLen; ++i) {
    sparse[i] = 1 + 1000 * i;
  }
  for (uint32_t i = 0; i != kLen; ++i) {
    dense.push_back((i == 0 ? sparse.back() : dense.back()) + 1 + i % 4);
  }
  std::vector<uint32_t> freqs(kLen);
  for (uint32_t i = 0; i != kLen; ++i) {
    freqs[i] = 0x40000000U | ((i * 2654435761U) >> 2);
  }
  uint32_t buf[irs::block_io::kEncWords];
  duckdb::MemoryStream leaves;
  irs::block_io::WriteTailDelta(kLen, leaves, sparse.data(),
                                irs::doc_limits::invalid(), buf);
  irs::block_io::WriteTail(kLen, leaves, freqs.data(), buf);
  const auto bitset_at = leaves.GetPosition();
  irs::block_io::WriteTailDelta(kLen, leaves, dense.data(), sparse.back(), buf);
  const auto freqs_at = leaves.GetPosition();
  irs::block_io::WriteTail(kLen, leaves, freqs.data(), buf);
  const auto end = leaves.GetPosition();
  irs::block_io::WriteSlack(leaves);
  ASSERT_TRUE(irs::block_codec::IsTokenBitset(leaves.GetData()[bitset_at]));
  ASSERT_GT(end - bitset_at, irs::block_io::kEncBytes);
  const uint64_t start = kBlock - irs::block_io::kEncBytes - bitset_at;
  {
    auto out = dir.Create("bitset.doc");
    const std::vector<irs::byte_type> filler(start, 0x5A);
    out->WriteData(filler.data(), filler.size());
    out->WriteData(leaves.GetData(), leaves.GetPosition());
    out->Close();
  }
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  const auto file = dir.Open("bitset.doc", duckdb::MappedFileAccess::NORMAL);
  ASSERT_EQ(nullptr, file->Data());
  irs::PostingMeta meta;
  meta.docs_count = 2 * kLen;
  meta.doc_start = start;
  duckdb::InputFileReader in{*file};
  in.Seek(start);
  in.ReadAheadUntil(start + end);
  irs::detail::EncBuf enc;
  irs::detail::HoleBuf holes;
  irs::DocsBuf docs;
  LeafSink sink{.in = &in};
  irs::detail::ReadPosting(meta, in, enc.data, holes.data, docs,
                           /*has_score_bounds=*/false, /*has_freq=*/true, sink);

  auto expected = sparse;
  expected.insert(expected.end(), dense.begin(), dense.end());
  EXPECT_EQ(expected, sink.docs);
  ASSERT_EQ(1, sink.positions.size());
  EXPECT_LT(sink.positions[0], kBlock);
  EXPECT_LT(start + freqs_at, kBlock);
}

constexpr uint64_t kValues = 100000;
constexpr uint64_t kChunkEvery = 25000;

const irs::bstring& Chunk() {
  static const irs::bstring chunk(2 * kBlock + 100, irs::byte_type{0x5A});
  return chunk;
}

void WriteValues(duckdb::WriteStream& out, uint64_t from, uint64_t to) {
  for (uint64_t i = from; i != to; ++i) {
    out.WriteVarint<uint64_t>(i * 2654435761);
    out.Write<uint32_t>(static_cast<uint32_t>(i));
    out.Write<irs::byte_type>(static_cast<irs::byte_type>(i));
    if (i % kChunkEvery == 0) {
      out.WriteData(Chunk().data(), Chunk().size());
    }
  }
}

void ReadValues(const tests::IndexDir& dir, uint32_t checksum) {
  const auto file = dir.Open("values", duckdb::MappedFileAccess::NORMAL);
  duckdb::InputFileReader in{*file};
  irs::bstring read(Chunk().size(), irs::byte_type{0});
  for (uint64_t i = 0; i != kValues; ++i) {
    ASSERT_EQ(i * 2654435761, irs::ReadV64(in));
    ASSERT_EQ(static_cast<int32_t>(i), in.Read<int32_t>());
    ASSERT_EQ(static_cast<irs::byte_type>(i), in.Read<irs::byte_type>());
    if (i % kChunkEvery == 0) {
      in.ReadData(read.data(), read.size());
      ASSERT_EQ(Chunk(), read);
    }
  }
  const auto body = in.Position();
  ASSERT_EQ(static_cast<int64_t>(kValues), in.Read<int64_t>());
  ASSERT_EQ(file->GetSize(), in.Position());
  duckdb::InputFileReader whole{*file};
  irs::bstring bytes(body, irs::byte_type{0});
  whole.ReadData(bytes.data(), bytes.size());
  absl::crc32c_t crc{0};
  crc = absl::ExtendCrc32c(
    crc, {reinterpret_cast<const char*>(bytes.data()), bytes.size()});
  EXPECT_EQ(static_cast<uint32_t>(crc), checksum);
}

TEST(ObjectStoreIndexTest, written_files_read_back_from_their_cache_blocks) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  uint32_t checksum = 0;
  {
    auto out = dir.Create("values");
    WriteValues(*out, 0, kValues);
    checksum = out->Checksum();
    out->Write<uint64_t>(kValues);
    out->Close();
  }
  tests::ObjectStore().ResetRequests();
  ReadValues(dir, checksum);
  EXPECT_EQ(0, tests::ObjectStore().Requests().get);

  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  ReadValues(dir, checksum);
  EXPECT_NE(0, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, a_merge_reads_its_segments_ahead) {
  constexpr size_t kSegments = 2;
  constexpr size_t kDocs = 150000;
  constexpr size_t kWords = 24;
  constexpr irs::field_id kText = 1;
  auto& store = tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  {
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    tests::Document doc;
    auto text = std::make_shared<tests::TextField<std::string>>("text");
    text->id = kText;
    doc.insert(text);
    std::string value;
    for (size_t segment = 0; segment != kSegments; ++segment) {
      for (size_t i = 0; i != kDocs; ++i) {
        value.clear();
        for (size_t word = 0; word != kWords; ++word) {
          absl::StrAppend(&value, "w", (i * 31 + word * 17) % 5003, " ");
        }
        text->value(value);
        ASSERT_TRUE(tests::Insert(*writer, doc));
      }
      ASSERT_TRUE(writer->RefreshCommit());
    }
  }
  const auto restarted = dir.Reopened();
  const auto reader =
    tests::OpenReader(restarted, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(kSegments, reader.size());
  uint64_t blocks = 0;
  for (const auto& segment : reader) {
    const auto* terms = segment.field(kText);
    ASSERT_NE(nullptr, terms);
    const auto handles = terms->Handles();
    for (const auto* file : {handles.doc, handles.pos, handles.pay}) {
      ASSERT_NE(nullptr, file);
      blocks += (file->GetSize() + kBlock - 1) / kBlock;
    }
  }
  ASSERT_LT(128, blocks);
  DropCachedBlocks(duckdb::ExternalFileCache::Get(Db()));
  store.ResetRequests();
  const tests::IndexDir merged{Db(), TestEnv::MemoryRoot()};
  irs::SegmentMeta meta;
  irs::LocalBudget budget{irs::gRootBudget};
  irs::MergeWriter merger{merged.Db(), merged.Fs(), merged.Root(),
                          irs::SegmentWriterOptions{},
                          irs::TrackedAllocator<>{budget}};
  merger.Reset(reader.begin(), reader.end());
  ASSERT_TRUE(irs::GetReady(merger.Flush(meta)));
  EXPECT_LT(store.Requests().get, blocks / 3);
}

#ifdef SDB_FAULT_INJECTION
TEST(ObjectStoreIndexTest, a_write_short_of_cache_blocks_uploads_every_byte) {
  tests::ObjectStore();
  const tests::IndexDir dir{Db(), Root()};
  absl::Cleanup clear = [] { duckdb::FaultPoints::Clear(); };
  uint32_t checksum = 0;
  {
    auto out = dir.Create("values");
    WriteValues(*out, 0, kValues / 2);
    ASSERT_TRUE(duckdb::FaultPoints::Arm("remote_write_cache_block"));
    WriteValues(*out, kValues / 2, kValues);
    checksum = out->Checksum();
    out->Write<uint64_t>(kValues);
    out->Close();
  }
  duckdb::FaultPoints::Clear();
  tests::ObjectStore().ResetRequests();
  ReadValues(dir, checksum);
  EXPECT_NE(0, tests::ObjectStore().Requests().get);
}
#endif

}  // namespace
