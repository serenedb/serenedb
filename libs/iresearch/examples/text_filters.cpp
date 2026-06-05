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

#include <duckdb/main/database.hpp>
#include <iostream>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>

#include "basics/duckdb_engine.h"

// This example shows direct construction of the advanced text filters:
//   - ByPhrase            (positional, "quick brown fox")
//   - ByNGramSimilarity   (fuzzy multi-token match with threshold)
//   - ByRegexp            (regex over field terms)
//   - ByWildcard          (SQL LIKE-style: _ single char, % any sequence)
//   - ByEditDistance      (Levenshtein, fuzzy term match)
//
// Each filter is shown in isolation against the same corpus so you can
// compare what they match. For end-to-end query parsing (Lucene syntax),
// see basic.cpp which routes "fox~1", "f%x", "+foo +bar" through a parser
// to the same underlying filter classes.

namespace {

// Per-segment columnstore needs a duckdb::DatabaseInstance for codec lookup
// and the buffer manager. main() brackets Initialize / Shutdown on the
// process-wide sdb::DuckDBEngine; this helper just hands out a reference.
duckdb::DatabaseInstance& Db() {
  return sdb::DuckDBEngine::Instance().instance();
}

// Stored-value field id for the body column.
inline constexpr irs::field_id kBodyFieldId = 1;

// Tokenizes a text value into the inverted index. Lowercase whitespace
// tokenizer; same shape as basic.cpp's TextField.
struct TextField {
  irs::field_id id{kBodyFieldId};
  std::string_view text;
  irs::analysis::Analyzer::ptr tokenizer{
    irs::analysis::SegmentationTokenizer::Make(
      irs::analysis::SegmentationTokenizer::Options{})};

  irs::field_id Id() const noexcept { return id; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm;
  }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(text);
    return *tokenizer;
  }
};

// Six documents chosen to make each filter's effect visible. Tokens are
// lowercase-segmented; the indexed terms for doc 0 are
// {"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog"} etc.
constexpr std::pair<std::string_view, std::string_view> kCorpus[] = {
  {"d0", "the quick brown fox jumps over the lazy dog"},
  {"d1", "the quick brown cat naps under the moon"},
  {"d2", "a swift brown fox runs through the forest"},
  {"d3", "search engine optimization is hard"},
  {"d4", "the foxy lady walked down the street"},
  {"d5", "fix the broken latch"},
};

irs::IndexWriterOptions MakeWriterOptions() {
  irs::IndexWriterOptions options;
  options.db = &Db();
  options.reader_options.db = &Db();
  options.column_options = [](irs::field_id) -> irs::ColumnOptions {
    return {.row_group_size = DEFAULT_ROW_GROUP_SIZE};
  };
  options.norm_column_options =
    [next = std::make_shared<std::atomic<irs::field_id>>(0)](
      irs::field_id) -> irs::NormColumnOptions {
    return {.id = next->fetch_add(1, std::memory_order_relaxed),
            .row_group_size = DEFAULT_ROW_GROUP_SIZE};
  };
  return options;
}

// Build the index from kCorpus, return the snapshot. Doc ids are assigned
// in order, so doc_id N corresponds to kCorpus[N-min_doc_id].
irs::DirectoryReader BuildIndex(irs::Directory& dir,
                                std::vector<std::string>& names_out) {
  auto format = irs::formats::Get("1_5simd");
  auto writer =
    irs::IndexWriter::Make(dir, format, irs::kOmCreate, MakeWriterOptions());

  TextField body;

  {
    auto trx = writer->GetBatch();
    for (auto [name, text] : kCorpus) {
      body.text = text;
      auto doc = trx.Insert();
      doc.Insert(body);
      names_out.emplace_back(name);
    }
  }
  writer->RefreshCommit();
  return writer->GetSnapshot();
}

// Run a prepared filter over all segments, collect doc names
// (kCorpus[i].first).
std::vector<std::string> RunFilter(const irs::DirectoryReader& reader,
                                   const irs::Filter& filter,
                                   const std::vector<std::string>& names) {
  std::vector<std::string> hits;
  auto prepared = filter.prepare({.index = reader});
  for (auto& segment : reader) {
    auto it = prepared->execute({.segment = segment});
    while (it->next()) {
      const auto idx = it->value() - irs::doc_limits::min();
      if (idx < names.size()) {
        hits.push_back(names[idx]);
      }
    }
  }
  return hits;
}

void PrintHits(std::string_view label, const std::vector<std::string>& hits) {
  std::cout << label << ": " << hits.size() << " hit(s)";
  if (!hits.empty()) {
    std::cout << " -- {";
    for (size_t i = 0; i < hits.size(); ++i) {
      if (i) {
        std::cout << ", ";
      }
      std::cout << hits[i];
    }
    std::cout << "}";
  }
  std::cout << "\n";
}

// Convenience: std::string_view -> bytes_view, used by every filter's
// term option (the index stores raw bytes).
irs::bytes_view Bytes(std::string_view s) noexcept {
  return irs::ViewCast<irs::byte_type>(s);
}

}  // namespace

int main() {
  // Bracket the process-wide duckdb::DuckDB lifetime; Db() reads it back.
  auto& engine = sdb::DuckDBEngine::Instance();
  engine.Initialize();

  irs::formats::Init();

  // Nested scope so reader/dir destruct before DuckDBEngine::Shutdown tears
  // down the duckdb::DuckDB they were dispatching through.
  {
    std::vector<std::string> names;
    irs::MemoryDirectory dir;
    auto reader = BuildIndex(dir, names);
    std::cout << "Indexed " << reader.docs_count() << " docs across "
              << reader.size() << " segment(s).\n\n";

    // 1) Phrase: "quick brown fox" -- positional, requires the three terms
    //    to appear in order with no gap.
    {
      std::cout << "=== ByPhrase \"quick brown fox\" ===\n";
      irs::ByPhrase q;
      *q.mutable_field_id() = kBodyFieldId;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        Bytes("quick");
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        Bytes("brown");
      q.mutable_options()->push_back<irs::ByTermOptions>().term = Bytes("fox");
      PrintHits("expect d0 only", RunFilter(reader, q, names));
    }

    // 2) NGramSimilarity: any doc that contains >= threshold * |ngrams| of
    //    the supplied tokens. With threshold=0.5 and two ngrams, a doc with
    //    just one of them still matches; with 1.0 both must appear. (Using
    //    word-level ngrams here for clarity; the same filter works against
    //    char-ngram-tokenized fields for fuzzy substring matching.)
    {
      std::cout << "\n=== ByNGramSimilarity {brown, fox} threshold=1.0 ===\n";
      irs::ByNGramSimilarity q;
      *q.mutable_field_id() = kBodyFieldId;
      q.mutable_options()->ngrams.emplace_back(Bytes("brown"));
      q.mutable_options()->ngrams.emplace_back(Bytes("fox"));
      q.mutable_options()->threshold = 1.0F;
      PrintHits("expect d0, d2", RunFilter(reader, q, names));
    }
    {
      std::cout << "\n=== ByNGramSimilarity {brown, fox} threshold=0.5 ===\n";
      irs::ByNGramSimilarity q;
      *q.mutable_field_id() = kBodyFieldId;
      q.mutable_options()->ngrams.emplace_back(Bytes("brown"));
      q.mutable_options()->ngrams.emplace_back(Bytes("fox"));
      q.mutable_options()->threshold = 0.5F;
      PrintHits("expect d0, d1, d2 (>= 1 of 2)", RunFilter(reader, q, names));
    }

    // 3) Regex: `f[ao]x` matches token "fox" and "fax" (no "fix"). Pattern
    //    syntax is Perl/RE2 by default; switch via options().syntax.
    {
      std::cout << "\n=== ByRegexp /f[ao]x/ ===\n";
      irs::ByRegexp q;
      *q.mutable_field_id() = kBodyFieldId;
      q.mutable_options()->pattern = irs::bstring{Bytes("f[ao]x")};
      PrintHits("expect d0, d2 (matches 'fox')", RunFilter(reader, q, names));
    }

    // 4) Wildcard / LIKE: '_' = exactly one char, '%' = any sequence. So
    //    'f_x' matches three-letter tokens like "fox" and "fix" but not
    //    longer tokens like "foxy".
    {
      std::cout << "\n=== ByWildcard \"f_x\" ===\n";
      irs::ByWildcard q;
      *q.mutable_field_id() = kBodyFieldId;
      q.mutable_options()->term = irs::bstring{Bytes("f_x")};
      PrintHits("expect d0, d2, d5 (fox, fox, fix)",
                RunFilter(reader, q, names));
    }

    // 5) Levenshtein (fuzzy term): edits up to max_distance. "fox" with
    //    max_distance=1 matches "fox" exactly and any token within one
    //    edit: "foxy" (one insertion), "fix" (one substitution).
    {
      std::cout << "\n=== ByEditDistance \"fox\" max_distance=1 ===\n";
      irs::ByEditDistance q;
      *q.mutable_field_id() = kBodyFieldId;
      q.mutable_options()->term = irs::bstring{Bytes("fox")};
      q.mutable_options()->max_distance = 1;
      q.mutable_options()->with_transpositions = true;
      PrintHits("expect d0, d2, d4, d5 (fox, fox, foxy, fix)",
                RunFilter(reader, q, names));
    }
  }

  engine.Shutdown();
  return 0;
}
