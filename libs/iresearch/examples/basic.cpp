////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/database.hpp>
#include <iostream>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/columnstore/column_reader.hpp>
#include <iresearch/columnstore/column_writer.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/text_format.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>

#include "basics/duckdb_engine.h"

// This example demonstrates the core iresearch workflow:
//   1. Create a directory and index writer
//   2. Define fields and index documents (inverted index + cs stored values)
//   3. Parse Lucene-syntax queries and execute them (count + top-K with BM25)

// Per-segment columnstore needs a duckdb::DatabaseInstance for codec lookup
// and the buffer manager. main() brackets Initialize / Shutdown on the
// process-wide sdb::DuckDBEngine; this helper just hands out a reference.
duckdb::DatabaseInstance& Db() {
  return sdb::DuckDBEngine::Instance().instance();
}

// Stored-value column ids. Field-name -> field_id mapping is the caller's
// responsibility in the new cs (column lookups are id-based, not by name).
inline constexpr irs::field_id kTitleColumnId = 1;
inline constexpr irs::field_id kBodyColumnId = 2;

// A minimal text field that tokenizes its value for the inverted index.
// Fields must provide: Name(), GetIndexFeatures(), GetTokens().
// Stored values are written separately into the columnstore (see
// AppendStoredText below) -- the legacy `Write()` STORE callback is gone.
struct TextField {
  std::string_view name;
  std::string_view text;
  irs::analysis::Analyzer::ptr tokenizer{
    irs::analysis::SegmentationTokenizer::Make(
      irs::analysis::SegmentationTokenizer::Options{})};

  std::string_view Name() const noexcept { return name; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm;
  }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(text);
    return *tokenizer;
  }
};

// Append one BLOB row to a cs column. Wraps the per-row-at-a-time pattern
// the example uses (one Insert per doc, one stored value per field).
void AppendStoredText(irs::columnstore::ColumnWriter& cw, irs::doc_id_t doc,
                      std::string_view text) {
  duckdb::Vector v{duckdb::LogicalType::BLOB, /*capacity=*/1};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
  slots[0] = duckdb::StringVector::AddStringOrBlob(v, text.data(), text.size());
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  const uint64_t row = static_cast<uint64_t>(doc) - irs::doc_limits::min();
  cw.Append(row, v, /*count=*/1);
}

// Helper: index a single document with two text fields. Stored text bytes
// flow into per-segment cs BLOB columns at `kTitleColumnId` / `kBodyColumnId`
// (replaces the legacy Action::STORE pathway).
void IndexDocument(irs::IndexWriter::Transaction& ctx, TextField& title_field,
                   TextField& body_field, std::string_view title,
                   std::string_view body,
                   irs::columnstore::ColumnWriter*& title_cw,
                   irs::columnstore::ColumnWriter*& body_cw) {
  title_field.text = title;
  body_field.text = body;

  auto doc = ctx.Insert();
  std::array<TextField*, 2> fields{&title_field, &body_field};
  doc.Insert(fields.begin(), fields.end());

  auto* cs = doc.Columnstore();
  if (cs == nullptr) {
    return;  // cs disabled (no `options.db` plumbed through).
  }
  if (title_cw == nullptr) {
    title_cw = &cs->OpenColumn(kTitleColumnId, duckdb::LogicalType::BLOB);
  }
  if (body_cw == nullptr) {
    body_cw = &cs->OpenColumn(kBodyColumnId, duckdb::LogicalType::BLOB);
  }
  AppendStoredText(*title_cw, doc.DocId(), title);
  AppendStoredText(*body_cw, doc.DocId(), body);
}

// Parse a Lucene-syntax query string into a filter.
// The default_field is used for terms without an explicit field prefix.
// Supported syntax: terms, phrases ("..."), boolean (+required -excluded),
//                   AND/OR operators, prefix (term*), wildcards, fuzzy
//                   (term~N), ranges ([min TO max]).
irs::Filter::ptr ParseQuery(std::string_view query_str,
                            std::string_view default_field,
                            irs::analysis::Analyzer& tokenizer) {
  auto root = std::make_unique<irs::MixedBooleanFilter>();
  sdb::ParserContext context{*root, default_field, tokenizer};
  auto result = sdb::ParseQuery(context, query_str);
  if (!result.ok()) {
    std::cerr << "Query parse error: " << context.error_message << "\n";
    return {};
  }
  return root;
}

// Helper: count documents matching a filter across all segments.
size_t CountMatches(const irs::DirectoryReader& reader,
                    const irs::Filter& filter) {
  auto prepared = filter.prepare({.index = reader});
  size_t count = 0;
  for (auto& segment : reader) {
    auto docs = prepared->execute({.segment = segment});
    while (docs->next()) {
      ++count;
    }
  }
  return count;
}

// Index five sample documents about information retrieval topics.
void BuildIndex(irs::IndexWriter& writer) {
  TextField title_field{.name = "title"};
  TextField body_field{.name = "body"};

  {
    auto ctx = writer.GetBatch();
    // cs writers are opened lazily by the first IndexDocument that observes
    // a non-null `Document::Columnstore()`. The pointers refer into the
    // per-segment cs Writer and stay valid until the transaction commits.
    irs::columnstore::ColumnWriter* title_cw = nullptr;
    irs::columnstore::ColumnWriter* body_cw = nullptr;

    IndexDocument(ctx, title_field, body_field,
                  "Introduction to Information Retrieval",
                  "Information retrieval is the activity of obtaining "
                  "information system resources that are relevant to an "
                  "information need from a collection.",
                  title_cw, body_cw);

    IndexDocument(ctx, title_field, body_field, "Search Engine Architecture",
                  "A search engine architecture describes the core components "
                  "including the indexer, the query processor, and the "
                  "ranking system that scores documents by relevance.",
                  title_cw, body_cw);

    IndexDocument(ctx, title_field, body_field,
                  "Inverted Index Data Structures",
                  "An inverted index is a database index storing a mapping "
                  "from content, such as words or numbers, to its locations "
                  "in a set of documents.",
                  title_cw, body_cw);

    IndexDocument(ctx, title_field, body_field, "BM25 Scoring Function",
                  "BM25 is a ranking function used by search engines to "
                  "estimate the relevance of documents to a given search "
                  "query based on term frequency and document length.",
                  title_cw, body_cw);

    IndexDocument(ctx, title_field, body_field, "The History of Databases",
                  "Databases have evolved from flat file systems to "
                  "relational models and beyond, powering modern applications "
                  "from banking to social media.",
                  title_cw, body_cw);
  }  // Transaction commits here when ctx goes out of scope.

  // Commit flushes segments to disk and makes documents visible to readers.
  writer.RefreshCommit();
  std::cout << "Indexed 5 documents.\n\n";
}

// Print basic index statistics.
void PrintIndexStats(const irs::DirectoryReader& reader) {
  std::cout << "=== Index Stats ===\n";
  std::cout << "Total documents: " << reader.docs_count() << "\n";
  std::cout << "Live documents:  " << reader.live_docs_count() << "\n";
  std::cout << "Segments:        " << reader.size() << "\n\n";
}

// Search for a single term using Lucene syntax.
void QuerySingleTerm(const irs::DirectoryReader& reader,
                     irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Single Term Query ===\n";
  auto filter = ParseQuery("search", "body", tokenizer);
  auto count = CountMatches(reader, *filter);
  std::cout << "Query 'search' (default field=body): " << count
            << " matches\n\n";
}

// Retrieve top-K results ranked by BM25 score.
void QueryTopK(const irs::DirectoryReader& reader, const irs::Scorer& scorer,
               irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Top-K with BM25 Scoring ===\n";
  auto filter = ParseQuery("search", "body", tokenizer);

  constexpr size_t kTopK = 3;
  std::vector<irs::ScoreDoc> results(irs::BlockSize(kTopK));

  auto total = irs::ExecuteTopKWithCount(reader, *filter, scorer, kTopK,
                                         std::span{results});

  std::cout << "Top " << kTopK << " results for 'search' "
            << "(total matches: " << total << "):\n";
  for (size_t i = 0; i < std::min<size_t>(kTopK, total); ++i) {
    std::cout << "  #" << (i + 1) << "  doc=" << results[i].doc
              << "  score=" << results[i].score << "\n";
  }
  std::cout << "\n";
}

// Search with boolean AND: both terms must be present.
void QueryBooleanAnd(const irs::DirectoryReader& reader,
                     irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Boolean AND Query ===\n";
  auto filter = ParseQuery("+index +search", "body", tokenizer);
  auto count = CountMatches(reader, *filter);
  std::cout << "Query '+index +search': " << count << " matches\n\n";
}

// Search with boolean OR: either term may match.
void QueryBooleanOr(const irs::DirectoryReader& reader,
                    irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Boolean OR Query ===\n";
  auto filter = ParseQuery("database retrieval", "body", tokenizer);
  auto count = CountMatches(reader, *filter);
  std::cout << "Query 'database retrieval': " << count << " matches\n\n";
}

// Search for an exact phrase.
void QueryPhrase(const irs::DirectoryReader& reader,
                 irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Phrase Query ===\n";
  auto filter = ParseQuery(R"("search engine")", "body", tokenizer);
  auto count = CountMatches(reader, *filter);
  std::cout << "Query '\"search engine\"': " << count << " matches\n\n";
}

// Search using a prefix wildcard.
void QueryPrefix(const irs::DirectoryReader& reader,
                 irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Prefix Query ===\n";
  auto filter = ParseQuery("rank*", "body", tokenizer);
  auto count = CountMatches(reader, *filter);
  std::cout << "Query 'rank*': " << count << " matches\n\n";
}

// Search with exclusion: require one term, exclude another.
void QueryExclusion(const irs::DirectoryReader& reader,
                    irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Exclusion Query ===\n";
  auto filter = ParseQuery("+documents -database", "body", tokenizer);
  auto count = CountMatches(reader, *filter);
  std::cout << "Query '+documents -database': " << count << " matches\n\n";
}

// Read back stored field values from the per-segment columnstore. Title
// and body live as BLOB columns at `kTitleColumnId` / `kBodyColumnId`;
// each doc-id maps to row `doc_id - doc_limits::min()` in the column.
void ReadStoredFields(const irs::DirectoryReader& reader) {
  std::cout << "=== Stored Fields ===\n";
  for (size_t seg_idx = 0; seg_idx < reader.size(); ++seg_idx) {
    auto& segment = reader[seg_idx];

    const auto* cs_reader = segment.CsReader();
    if (cs_reader == nullptr) {
      continue;  // cs disabled.
    }
    const auto* title_col = cs_reader->Column(kTitleColumnId);
    const auto* body_col = cs_reader->Column(kBodyColumnId);
    if (title_col == nullptr || body_col == nullptr) {
      continue;
    }

    irs::columnstore::ColumnReader::PointReader title_cursor{*cs_reader,
                                                             *title_col};
    irs::columnstore::ColumnReader::PointReader body_cursor{*cs_reader,
                                                            *body_col};
    duckdb::Vector title_vec{duckdb::LogicalType::BLOB, 1};
    duckdb::Vector body_vec{duckdb::LogicalType::BLOB, 1};

    const uint64_t row_count = title_col->RowCount();
    for (uint64_t row = 0; row < row_count; ++row) {
      title_cursor.FetchRow(row, title_vec, 0);
      body_cursor.FetchRow(row, body_vec, 0);
      const auto& title_slot =
        duckdb::FlatVector::GetData<duckdb::string_t>(title_vec)[0];
      const auto& body_slot =
        duckdb::FlatVector::GetData<duckdb::string_t>(body_vec)[0];
      const auto doc = static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
      std::cout << "  doc=" << doc << "\n"
                << "    title: \""
                << std::string_view{title_slot.GetData(),
                                    static_cast<size_t>(title_slot.GetSize())}
                << "\"\n"
                << "    body:  \""
                << std::string_view{body_slot.GetData(),
                                    static_cast<size_t>(body_slot.GetSize())}
                << "\"\n";
    }
  }
  std::cout << "\n";
}

// Remove documents matching a query and print updated stats.
void RemoveDocuments(irs::IndexWriter& writer,
                     irs::analysis::Analyzer& tokenizer) {
  std::cout << "=== Remove Documents ===\n";
  auto filter = ParseQuery("databases", "title", tokenizer);
  writer.GetBatch().Remove(*filter);
  writer.RefreshCommit();

  auto updated_reader = writer.GetSnapshot();
  std::cout << "After removal:\n";
  std::cout << "  Total documents: " << updated_reader.docs_count() << "\n";
  std::cout << "  Live documents:  " << updated_reader.live_docs_count()
            << "\n\n";
}

// Compact segments after removal to reclaim space and merge segments.
// Deleted documents are only physically removed during compaction.
void CompactIndex(irs::IndexWriter& writer, irs::Directory& dir) {
  std::cout << "=== Compact Index ===\n";

  auto before = writer.GetSnapshot();
  std::cout << "Before compaction: " << before.size() << " segment(s)\n";

  // Tier-based compaction merges segments of similar size.
  irs::index_utils::CompactionTier tier_opts;
  tier_opts.min_segments = 1;
  tier_opts.max_segments = 10;
  auto policy = irs::index_utils::MakePolicy(tier_opts);

  writer.Compact(policy);
  writer.RefreshCommit();

  // Remove files no longer referenced by any reader.
  irs::directory_utils::RemoveAllUnreferenced(dir);

  auto after = writer.GetSnapshot();
  std::cout << "After compaction:  " << after.size() << " segment(s)\n";
  std::cout << "  Total documents: " << after.docs_count() << "\n";
  std::cout << "  Live documents:  " << after.live_docs_count() << "\n";
}

int main() {
  // Bracket the process-wide duckdb::DuckDB lifetime; Db() reads it back.
  auto& engine = sdb::DuckDBEngine::Instance();
  engine.Initialize();

  // Initialize subsystems (required once per process).
  irs::formats::Init();

  auto format = irs::formats::Get("1_5simd");
  auto scorer = irs::BM25::Make(irs::BM25::Options{});
  auto tokenizer = irs::analysis::SegmentationTokenizer::Make(
    irs::analysis::SegmentationTokenizer::Options{});

  irs::MemoryDirectory dir;
  // cs needs a DatabaseInstance plumbed through both the writer (for
  // OpenColumn) and the reader_options (for CsReader on the snapshot).
  // Norm-featured fields additionally require a norm_column_options
  // callback that allocates a (column id, row-group size) pair per field.
  irs::IndexWriterOptions options;
  options.db = &Db();
  options.reader_options.db = &Db();
  options.column_options = [](irs::field_id) -> irs::ColumnOptions {
    return {.row_group_size = DEFAULT_ROW_GROUP_SIZE};
  };
  options.norm_column_options =
    [next = std::make_shared<std::atomic<irs::field_id>>(0)](
      std::string_view) -> irs::NormColumnOptions {
    return {
      .id = next->fetch_add(1, std::memory_order_relaxed),
      .row_group_size = DEFAULT_ROW_GROUP_SIZE,
    };
  };
  auto writer = irs::IndexWriter::Make(dir, format, irs::kOmCreate, options);

  BuildIndex(*writer);

  auto reader = writer->GetSnapshot();

  PrintIndexStats(reader);
  QuerySingleTerm(reader, *tokenizer);
  QueryTopK(reader, *scorer, *tokenizer);
  QueryBooleanAnd(reader, *tokenizer);
  QueryBooleanOr(reader, *tokenizer);
  QueryPhrase(reader, *tokenizer);
  QueryPrefix(reader, *tokenizer);
  QueryExclusion(reader, *tokenizer);
  ReadStoredFields(reader);
  RemoveDocuments(*writer, *tokenizer);
  CompactIndex(*writer, dir);

  engine.Shutdown();
  return 0;
}
