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

#include <iresearch/parser/parser.h>

#include <iostream>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/compression.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/text_format.hpp>

// This example demonstrates the core iresearch workflow:
//   1. Create a directory and index writer
//   2. Define fields and index documents
//   3. Parse Lucene-syntax queries and execute them (count + top-K with BM25)

// A minimal text field that tokenizes its value and optionally stores it.
// Fields must provide: Name(), GetIndexFeatures(), GetTokens(), Write().
struct TextField {
  std::string_view name;
  std::string_view text;
  irs::analysis::Analyzer::ptr tokenizer{irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(), R"({})")};

  std::string_view Name() const noexcept { return name; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm;
  }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(text);
    return *tokenizer;
  }

  // Write is called when Action::STORE is used. Return true to store the value.
  bool Write(irs::DataOutput& out) const {
    irs::WriteStr(out, text);
    return true;
  }
};

// Configure the index writer to handle Norm features (needed for BM25).
irs::IndexWriterOptions MakeWriterOptions() {
  irs::IndexWriterOptions opts;
  opts.features = [](irs::IndexFeatures id) {
    const irs::ColumnInfo info{
      irs::Type<irs::compression::None>::get(), {}, false};
    if (irs::IndexFeatures::Norm == id) {
      return std::make_pair(info, &irs::Norm::MakeWriter);
    }
    return std::make_pair(info, irs::FeatureWriterFactory{});
  };
  return opts;
}

// Helper: index a single document with two text fields.
void IndexDocument(irs::IndexWriter::Transaction& ctx, TextField& title_field,
                   TextField& body_field, std::string_view title,
                   std::string_view body) {
  title_field.text = title;
  body_field.text = body;

  auto doc = ctx.Insert();
  std::array<TextField*, 2> fields{&title_field, &body_field};
  doc.Insert<irs::Action::INDEX | irs::Action::STORE>(fields.begin(),
                                                      fields.end());
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
  auto& opt = root->GetOptional();
  auto& req = root->GetRequired();
  if (opt.size() == 1 && req.empty()) {
    return opt.PopBack();
  }
  if (req.size() == 1 && opt.empty()) {
    return req.PopBack();
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

    IndexDocument(ctx, title_field, body_field,
                  "Introduction to Information Retrieval",
                  "Information retrieval is the activity of obtaining "
                  "information system resources that are relevant to an "
                  "information need from a collection.");

    IndexDocument(ctx, title_field, body_field, "Search Engine Architecture",
                  "A search engine architecture describes the core components "
                  "including the indexer, the query processor, and the "
                  "ranking system that scores documents by relevance.");

    IndexDocument(ctx, title_field, body_field,
                  "Inverted Index Data Structures",
                  "An inverted index is a database index storing a mapping "
                  "from content, such as words or numbers, to its locations "
                  "in a set of documents.");

    IndexDocument(ctx, title_field, body_field, "BM25 Scoring Function",
                  "BM25 is a ranking function used by search engines to "
                  "estimate the relevance of documents to a given search "
                  "query based on term frequency and document length.");

    IndexDocument(ctx, title_field, body_field, "The History of Databases",
                  "Databases have evolved from flat file systems to "
                  "relational models and beyond, powering modern applications "
                  "from banking to social media.");
  }  // Transaction commits here when ctx goes out of scope.

  // Commit flushes segments to disk and makes documents visible to readers.
  writer.Commit();
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
    std::cout << "  #" << (i + 1) << "  doc=" << results[i].second
              << "  score=" << results[i].first << "\n";
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

// Read back stored field values from the columnar storage.
void ReadStoredFields(const irs::DirectoryReader& reader) {
  std::cout << "=== Stored Fields ===\n";
  for (size_t seg_idx = 0; seg_idx < reader.size(); ++seg_idx) {
    auto& segment = reader[seg_idx];

    const auto* title_col = segment.column("title");
    const auto* body_col = segment.column("body");
    if (!title_col || !body_col) {
      continue;
    }

    auto title_it = title_col->iterator(irs::ColumnHint::Normal);
    auto* title_payload = irs::get<irs::PayAttr>(*title_it);

    auto body_it = body_col->iterator(irs::ColumnHint::Normal);
    auto* body_payload = irs::get<irs::PayAttr>(*body_it);

    while (title_it->next()) {
      irs::BytesViewInput in;
      in.reset(title_payload->value);
      auto title = irs::ReadString<std::string>(in);

      body_it->seek(title_it->value());
      in.reset(body_payload->value);
      auto body = irs::ReadString<std::string>(in);

      std::cout << "  doc=" << title_it->value() << "\n"
                << "    title: \"" << title << "\"\n"
                << "    body:  \"" << body << "\"\n";
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
  writer.Commit();

  auto updated_reader = writer.GetSnapshot();
  std::cout << "After removal:\n";
  std::cout << "  Total documents: " << updated_reader.docs_count() << "\n";
  std::cout << "  Live documents:  " << updated_reader.live_docs_count()
            << "\n\n";
}

// Consolidate segments after removal to reclaim space and merge segments.
// Deleted documents are only physically removed during consolidation.
void ConsolidateIndex(irs::IndexWriter& writer, irs::Directory& dir) {
  std::cout << "=== Consolidate Index ===\n";

  auto before = writer.GetSnapshot();
  std::cout << "Before consolidation: " << before.size() << " segment(s)\n";

  // Tier-based consolidation merges segments of similar size.
  irs::index_utils::ConsolidateTier tier_opts;
  tier_opts.min_segments = 1;
  tier_opts.max_segments = 10;
  auto policy = irs::index_utils::MakePolicy(tier_opts);

  writer.Consolidate(policy);
  writer.Commit();

  // Remove files no longer referenced by any reader.
  irs::directory_utils::RemoveAllUnreferenced(dir);

  auto after = writer.GetSnapshot();
  std::cout << "After consolidation:  " << after.size() << " segment(s)\n";
  std::cout << "  Total documents: " << after.docs_count() << "\n";
  std::cout << "  Live documents:  " << after.live_docs_count() << "\n";
}

int main() {
  // Initialize subsystems (required once per process).
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  auto format = irs::formats::Get("1_5simd");
  auto scorer =
    irs::scorers::Get("bm25", irs::Type<irs::text_format::Json>::get(), "{}");
  auto tokenizer = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(), "{}");

  irs::MemoryDirectory dir;
  auto writer =
    irs::IndexWriter::Make(dir, format, irs::kOmCreate, MakeWriterOptions());

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
  ConsolidateIndex(*writer, dir);

  return 0;
}
