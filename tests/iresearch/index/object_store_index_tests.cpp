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
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/queries/posting_heads.hpp>
#include <iresearch/store/directory.hpp>
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
#include "store/simulated_object_store.hpp"
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

void WriteSegments(const irs::Directory& dir, size_t segments) {
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

irs::ObjectStoreRequests Open(const std::string& root, uint64_t live_docs) {
  auto& store = tests::ObjectStore();
  const irs::Directory restarted{Db(), root};
  duckdb::BlockCache::Get(Db()).Clear();
  store.ResetRequests();
  const irs::DirectoryReader reader{restarted,
                                    irs::tests::DefaultReaderOptions()};
  const auto requests = store.Requests();
  EXPECT_EQ(live_docs, reader.live_docs_count());
  return requests;
}

TEST(ObjectStoreIndexTest, prefetched_term_blocks_serve_the_lookups) {
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::Directory{Db(), root}, 3);
  const irs::Directory restarted{Db(), root};
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

TEST(ObjectStoreIndexTest, segments_open_in_parallel) {
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::Directory{Db(), root}, 4);
  store.SetLatency(std::chrono::milliseconds{20});
  absl::Cleanup no_latency = [&] {
    store.SetLatency(std::chrono::microseconds{0});
  };

  EXPECT_LT(1, Open(root, 4).max_in_flight);
}

TEST(ObjectStoreIndexTest, writer_opens_segments_in_parallel) {
  constexpr size_t kSegments = 16;
  constexpr std::chrono::milliseconds kLatency{50};
  auto& store = tests::ObjectStore();
  const auto root = Root();
  WriteSegments(irs::Directory{Db(), root}, kSegments);
  const irs::Directory restarted{Db(), root};
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
  WriteSegments(irs::Directory{Db(), root}, 4);
  const irs::Directory restarted{Db(), root};
  store.ResetRequests();

  const auto writer = irs::IndexWriter::Make(
    restarted, irs::kOmAppend, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);
  EXPECT_EQ(1, store.Requests().remove);
  EXPECT_FALSE(restarted.Has("segments_1"));
}

TEST(ObjectStoreIndexTest, replace_adopts_segments) {
  tests::ObjectStore();
  const irs::Directory dir{Db(), Root()};
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

std::unique_ptr<irs::InputFile> WriteUncached(const irs::Directory& dir) {
  const std::vector<irs::byte_type> block(kBlock, 0x5A);
  {
    auto out = dir.Create("blocks");
    for (uint64_t i = 0; i != kBlocks; ++i) {
      out->WriteData(block.data(), block.size());
    }
    out->Close();
  }
  duckdb::BlockCache::Get(Db()).Clear();
  auto file = dir.Open("blocks", duckdb::MappedFileAccess::NORMAL);
  tests::ObjectStore().ResetRequests();
  return file;
}

TEST(ObjectStoreIndexTest, sequential_reads_fetch_ahead) {
  tests::ObjectStore();
  const irs::Directory dir{Db(), Root()};
  const auto file = WriteUncached(dir);
  irs::IndexInput in{*file};
  while (!in.IsEOF()) {
    std::ignore = in.ReadByte();
  }
  EXPECT_EQ(4, tests::ObjectStore().Requests().get);
}

TEST(ObjectStoreIndexTest, large_reads_fetch_their_range_at_once) {
  tests::ObjectStore();
  const irs::Directory dir{Db(), Root()};
  const auto file = WriteUncached(dir);
  irs::IndexInput in{*file};
  irs::bstring buf(kBlocks * kBlock, 0);
  in.ReadData(buf.data(), buf.size());
  EXPECT_EQ(1, tests::ObjectStore().Requests().get);
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
  const irs::Directory dir{Db(), Root()};
  {
    irs::ColWriter writer{dir, "seg", Db()};
    auto& plain = writer.OpenColumn(kPlain, duckdb::LogicalType::BIGINT,
                                    /*skip_validity=*/false, kRgSize);
    auto& structs =
      writer.OpenColumn(kStruct, stype, /*skip_validity=*/false, kRgSize);
    auto& arrays =
      writer.OpenColumn(kArray, atype, /*skip_validity=*/false, kRgSize);
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
  std::vector<irs::ByteExtent> extents;
  plain->CollectBlocks(rows.front(), rows.back(), extents);
  std::vector<irs::ByteExtent> expected;
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
  const auto key = [](const irs::ByteExtent& e) {
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
  auto& cache = duckdb::BlockCache::Get(Db());
  cache.Clear();
  for (auto& extent : extents) {
    reader.PrefetchExtents({&extent, 1});
  }
  const auto prefetched = cache.GetStatistics().misses;

  irs::ReadContext ctx{reader};
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

TEST(ObjectStoreIndexTest, collected_variant_blocks_serve_gathers) {
  constexpr uint32_t kRgSize = 64 * 1024;
  constexpr irs::field_id kVariant = 4;
  tests::ObjectStore();
  const irs::Directory dir{Db(), Root()};
  {
    irs::ColWriter writer{dir, "seg", Db()};
    auto& variants = writer.OpenColumn(kVariant, duckdb::LogicalType::VARIANT(),
                                       /*skip_validity=*/false, kRgSize);
    duckdb::Connection con{Db()};
    auto result = con.Query(
      absl::StrCat("SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE "
                   "{'a': i, 'b': 'v' || (i % 1000)}::VARIANT END FROM range(",
                   4 * kRgSize, ") t(i)"));
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
  std::vector<irs::ByteExtent> extents;
  column->CollectBlocks(rows.front(), rows.back(), extents);
  ASSERT_FALSE(extents.empty());
  auto& cache = duckdb::BlockCache::Get(Db());
  cache.Clear();
  for (auto& extent : extents) {
    reader.PrefetchExtents({&extent, 1});
  }
  const auto prefetched = cache.GetStatistics().misses;

  irs::ReadContext ctx{reader};
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
  const irs::Directory dir{Db(), Root()};
  {
    auto out = dir.Create("short");
    out->WriteU32(42);
    out->Close();
  }
  const auto file = dir.Open("short", duckdb::MappedFileAccess::NORMAL);
  irs::IndexInput in{*file};
  EXPECT_EQ(42, in.ReadI32());
  EXPECT_THROW(in.ReadByte(), irs::SqlException);
  in.Seek(2);
  irs::byte_type buf[8];
  EXPECT_THROW(in.ReadData(buf, sizeof(buf)), irs::SqlException);
}

TEST(ObjectStoreIndexTest, a_listing_skips_nested_objects) {
  tests::ObjectStore();
  const auto root = Root();
  const irs::Directory dir{Db(), root};
  dir.Create("top")->Close();
  const irs::Directory nested{Db(), absl::StrCat(root, "/nested")};
  nested.Create("inner")->Close();

  std::vector<std::string> names;
  dir.Visit([&](std::string_view name) { names.emplace_back(name); });
  EXPECT_EQ(std::vector<std::string>{"top"}, names);
}

TEST(ObjectStoreIndexTest, posting_heads_prefetch_every_clause) {
  constexpr size_t kDocs = 20000;
  constexpr irs::field_id kWords = 1;
  constexpr irs::field_id kText = 2;
  tests::ObjectStore();
  const irs::Directory dir{Db(), Root()};
  {
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
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
  const irs::DirectoryReader reader{dir, irs::tests::DefaultReaderOptions()};
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
  irs::BooleanFilter only;
  only.Add(clause("long"), irs::Occur::Must);
  const auto only_query = only.PrepareSegment(segment, {});
  irs::BooleanFilter either;
  either.Add(clause("long"), irs::Occur::Should);
  either.Add(clause("short"), irs::Occur::Should);
  either.SetMinShouldMatch(1);
  const auto terms_query = either.PrepareSegment(segment, {});
  auto& cache = duckdb::BlockCache::Get(Db());
  cache.Clear();
  irs::PrefetchPostingHeads(*only_query, 2);
  EXPECT_FALSE(words.doc->Warm(long_list.doc_start, 1));
  irs::PrefetchPostingHeads(*terms_query, 2);
  EXPECT_TRUE(words.doc->Warm(long_list.doc_start, 1));
  EXPECT_TRUE(words.doc->Warm(long_list.doc_start + long_list.doc_delta, 1));
  EXPECT_TRUE(words.doc->Warm(short_list.doc_start, 1));

  irs::ByPhrase phrase;
  *phrase.mutable_field_id() = kText;
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"quick"});
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  const auto phrase_query = phrase.PrepareSegment(segment, {});
  const auto [text, quick] = meta(kText, "quick");
  ASSERT_NE(nullptr, text.pos);
  cache.Clear();
  EXPECT_FALSE(text.pos->Warm(quick.pos_start, 1));
  irs::PrefetchPostingHeads(*phrase_query, 1);
  EXPECT_TRUE(text.doc->Warm(quick.doc_start, 1));
  EXPECT_TRUE(text.pos->Warm(quick.pos_start, 1));

  irs::ByPrefix prefix;
  *prefix.mutable_field_id() = kWords;
  prefix.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"sh"});
  const auto prefix_query = prefix.PrepareSegment(segment, {});
  cache.Clear();
  EXPECT_FALSE(words.doc->Warm(short_list.doc_start, 1));
  irs::PrefetchPostingHeads(*prefix_query, 1);
  EXPECT_TRUE(words.doc->Warm(short_list.doc_start, 1));

  irs::ByPhrase variadic;
  *variadic.mutable_field_id() = kText;
  variadic.mutable_options()->push_back<irs::ByPrefixOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"qui"});
  variadic.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  const auto variadic_query = variadic.PrepareSegment(segment, {});
  cache.Clear();
  EXPECT_FALSE(text.pos->Warm(quick.pos_start, 1));
  irs::PrefetchPostingHeads(*variadic_query, 1);
  EXPECT_TRUE(text.doc->Warm(quick.doc_start, 1));
  EXPECT_TRUE(text.pos->Warm(quick.pos_start, 1));
}

TEST(ObjectStoreIndexTest, random_reads_fetch_only_their_blocks) {
  tests::ObjectStore();
  const irs::Directory dir{Db(), Root()};
  const auto file = WriteUncached(dir);
  irs::IndexInput in{*file};
  for (uint64_t block = 0; block < kBlocks; block += 2) {
    in.Seek(block * kBlock);
    std::ignore = in.ReadByte();
  }
  EXPECT_EQ(kBlocks / 2, tests::ObjectStore().Requests().get);

  irs::bstring buf(2 * kBlock + 2, 0);
  in.ReadData(3 * kBlock - 1, buf.data(), buf.size());
  EXPECT_EQ(kBlocks / 2 + 1, tests::ObjectStore().Requests().get);
}

}  // namespace
