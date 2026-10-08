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

#include "iresearch/ffi/iresearch_ffi.h"

#include <duckdb/common/types/string_type.hpp>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-missing-comma-variadic-parameter"
#include <roaring/roaring64map.hh>
#pragma clang diagnostic pop
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/parser/parser.hpp"
#include "iresearch/search/detail/doc_collector.hpp"
#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/filter_optimizer.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/wildcard_filter.hpp"
#include "iresearch/search/scorers/bm25.hpp"
#include "iresearch/store/fs_directory.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/duckdb_engine.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

constexpr irs::field_id kField = 1;

thread_local std::string g_error;
std::atomic<bool> g_shutdown{false};

void SetError(std::string_view what) { g_error.assign(what); }

template<typename F>
auto Guard(F&& f, decltype(f()) on_error) -> decltype(f()) {
  try {
    g_error.clear();
    return f();
  } catch (const std::exception& e) {
    SetError(e.what());
  } catch (...) {
    SetError("unknown error");
  }
  return on_error;
}

irs::analysis::Tokenizer::ptr MakeTokenizer() {
  return irs::analysis::TextTokenizer::Make(
    irs::analysis::TextTokenizer::Options{});
}

}  // namespace

struct irs_ffi_index {
  std::unique_ptr<irs::Directory> dir;
  irs::IndexWriter::ptr writer;
  irs::analysis::Tokenizer::ptr tokenizer;
  std::vector<int64_t> rows;
  std::optional<irs::IndexWriter::Transaction> batch;
  bool failed = false;

  irs::IndexWriter::Transaction& Batch() {
    if (failed) {
      throw std::runtime_error("index transaction failed; close the index");
    }
    if (!batch) {
      batch.emplace(writer->GetBatch());
    }
    return *batch;
  }

  void CloseBatch() {
    if (failed) {
      throw std::runtime_error("index transaction failed; close the index");
    }
    if (batch) {
      const bool committed = batch->Commit();
      batch.reset();
      if (!committed) {
        failed = true;
        throw std::runtime_error("index transaction failed; close the index");
      }
    }
  }
};

struct irs_ffi_reader {
  std::unique_ptr<irs::Directory> dir;
  irs::DirectoryReader reader;
  irs::analysis::Tokenizer::ptr tokenizer;
  std::unique_ptr<irs::BM25> scorer;
  std::vector<int64_t> rows;
  std::vector<size_t> segment_offsets;

  void MapSegments() {
    size_t offset = 0;
    for (const auto& segment : reader) {
      segment_offsets.push_back(offset);
      offset += segment.docs_count();
    }
    if (offset != rows.size()) {
      throw std::invalid_argument(
        "row map does not match the index document count");
    }
  }

  int64_t Row(irs::doc_id_t doc, uint32_t segment) const {
    const auto i = segment_offsets.at(segment) +
                   static_cast<size_t>(doc - irs::doc_limits::min());
    return rows.at(i);
  }
};

namespace {

constexpr uint32_t kArchiveMagic = 0x53524915;
constexpr size_t kCopyChunk = 1u << 16;
constexpr std::string_view kRowMapName = "irs_ffi_rows";

void PutU32(std::string& out, uint32_t v) {
  for (int i = 0; i != 4; ++i) {
    out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}

void PutU64(std::string& out, uint64_t v) {
  for (int i = 0; i != 8; ++i) {
    out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}

uint32_t GetU32(const unsigned char* p) {
  return static_cast<uint32_t>(p[0]) | static_cast<uint32_t>(p[1]) << 8 |
         static_cast<uint32_t>(p[2]) << 16 | static_cast<uint32_t>(p[3]) << 24;
}

uint64_t GetU64(const unsigned char* p) {
  uint64_t v = 0;
  for (int i = 7; i >= 0; --i) {
    v = v << 8 | p[i];
  }
  return v;
}

}  // namespace
namespace {

irs::bytes_view AsBytes(std::string_view s) noexcept {
  return irs::ViewCast<irs::byte_type>(s);
}

irs::Filter::ptr AnalyzeQuery(std::string_view text, irs_ffi_search_type type,
                              irs::analysis::Tokenizer& tokenizer) {
  irs::ValueAnalyzer analyzer;
  irs::ValueTokens<irs::TokenLayout::TermsPos> tokens{tokenizer.Traits()};
  if (!analyzer.Analyze(
        tokenizer,
        duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())},
        tokens) ||
      tokens.terms().empty()) {
    throw std::invalid_argument("query produced no tokens");
  }
  if (type == IRS_FFI_PHRASE && tokens.terms().size() > 1) {
    auto phrase = std::make_unique<irs::ByPhrase>();
    *phrase->mutable_field_id() = kField;
    for (size_t i = 0; i < tokens.terms().size(); ++i) {
      const auto gap = i == 0 ? 0 : tokens.pos()[i] - tokens.pos()[i - 1] - 1;
      phrase->mutable_options()->push_back<irs::ByTermOptions>(gap).term =
        irs::bstring{irs::AsBytesView(tokens.terms()[i])};
    }
    return phrase;
  }
  auto boolean = std::make_unique<irs::BooleanFilter>();
  if (type != IRS_FFI_MATCH_ALL) {
    boolean->SetMinShouldMatch(1);
  }
  for (const auto& token : tokens.terms()) {
    boolean->Add(
      irs::TermClause{.field = kField,
                      .term = irs::bstring{irs::AsBytesView(token)}},
      type == IRS_FFI_MATCH_ALL ? irs::Occur::Must : irs::Occur::Should);
  }
  return boolean;
}

std::string NormalizeLiteral(std::string_view text) {
  std::string result{text};
  if (std::ranges::all_of(text, [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
               (c >= '0' && c <= '9');
      })) {
    for (char& c : result) {
      if (c >= 'A' && c <= 'Z') {
        c += 'a' - 'A';
      }
    }
  }
  return result;
}

std::string WildcardPattern(std::string_view text, bool normalize) {
  std::string result;
  size_t begin = 0;
  const auto append_literal = [&](std::string_view literal) {
    const auto value =
      normalize ? NormalizeLiteral(literal) : std::string{literal};
    for (char c : value) {
      if (c == '%' || c == '_' || c == '\\') {
        result.push_back('\\');
      }
      result.push_back(c);
    }
  };
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '*' || text[i] == '?') {
      append_literal(text.substr(begin, i - begin));
      result.push_back(text[i] == '*' ? '%' : '_');
      begin = i + 1;
    }
  }
  append_literal(text.substr(begin));
  return result;
}

}  // namespace

const char* irs_ffi_error(void) { return g_error.c_str(); }

int irs_ffi_init(void) {
  return Guard(
    [&] {
      if (g_shutdown.load()) {
        SetError("engine has been shut down");
        return -1;
      }
      static std::once_flag once;
      std::call_once(once, [] {
        irs::DuckDBEngine::Instance().Initialize();
        irs::InitOptimizeRules();
        std::atexit(&irs_ffi_shutdown);
      });
      return 0;
    },
    -1);
}

void irs_ffi_shutdown(void) {
  static std::once_flag once;
  std::call_once(once, [] {
    g_shutdown.store(true);
    auto& engine = irs::DuckDBEngine::Instance();
    engine.CloseDatabases();
    engine.Shutdown();
  });
}

irs_ffi_index* irs_ffi_index_create(const char* path, size_t path_len) {
  return Guard(
    [&]() -> irs_ffi_index* {
      if (!path || path_len == 0 || irs_ffi_init() != 0) {
        SetError("invalid path or unavailable engine");
        return nullptr;
      }
      std::filesystem::path dir_path{std::string{path, path_len}};
      std::filesystem::create_directories(dir_path);

      auto index = std::make_unique<irs_ffi_index>();
      index->dir = std::make_unique<irs::FSDirectory>(std::move(dir_path));

      auto& db = irs::DuckDBEngine::Instance().instance();

      irs::IndexWriterOptions options;
      options.db = &db;
      options.reader_options.db = &db;
      options.norm_column_id = [](irs::field_id) { return irs::field_id{2}; };

      index->writer =
        irs::IndexWriter::Make(*index->dir, irs::kOmCreate, std::move(options));
      index->tokenizer = MakeTokenizer();
      return index.release();
    },
    nullptr);
}

int irs_ffi_index_add_row(irs_ffi_index* index, const char* text,
                          size_t text_len, int64_t row) {
  return Guard(
    [&] {
      if (index == nullptr || (!text && text_len != 0) ||
          text_len > std::numeric_limits<uint32_t>::max() || row < 0) {
        SetError("invalid index, text, or row id");
        return -1;
      }
      const duckdb::string_t value{text ? text : "",
                                   static_cast<uint32_t>(text_len)};
      auto& tokenizer = *index->tokenizer;

      auto& batch = index->Batch();
      auto doc = batch.Insert();
      const irs::doc_id_t id = doc.DocId();
      const bool ok = doc.WithTokens(
        kField,
        irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
          irs::IndexFeatures::Norm,
        nullptr, [&](irs::FieldInverter& field, irs::TokenSink& sink) {
          field.Configure(tokenizer.Traits());
          tokenizer.Fill(value, id, sink, {field.Layout()});
        });
      if (!ok) {
        SetError("document was not indexed");
        return -1;
      }
      index->rows.push_back(row);
      return 0;
    },
    -1);
}

int irs_ffi_index_add(irs_ffi_index* index, const char* text, size_t text_len) {
  if (index == nullptr) {
    SetError("index is null");
    return -1;
  }
  return irs_ffi_index_add_row(index, text, text_len,
                               static_cast<int64_t>(index->rows.size()));
}

int irs_ffi_index_commit(irs_ffi_index* index) {
  return Guard(
    [&] {
      if (index == nullptr) {
        SetError("index is null");
        return -1;
      }
      index->CloseBatch();
      index->writer->RefreshCommit();
      auto out = index->dir->create(kRowMapName);
      if (!out) {
        SetError("could not write the row map");
        return -1;
      }
      std::string rows;
      PutU64(rows, index->rows.size());
      for (const auto row : index->rows) {
        PutU64(rows, static_cast<uint64_t>(row));
      }
      out->WriteData(reinterpret_cast<const irs::byte_type*>(rows.data()),
                     rows.size());
      out->Flush();
      return 0;
    },
    -1);
}

irs_ffi_index* irs_ffi_index_create_memory(void) {
  return Guard(
    [&]() -> irs_ffi_index* {
      if (irs_ffi_init() != 0) {
        return nullptr;
      }
      auto& db = irs::DuckDBEngine::Instance().instance();

      auto index = std::make_unique<irs_ffi_index>();
      index->dir = std::make_unique<irs::MemoryDirectory>();

      irs::IndexWriterOptions options;
      options.db = &db;
      options.reader_options.db = &db;
      options.norm_column_id = [](irs::field_id) { return irs::field_id{2}; };
      options.lock_repository = false;

      index->writer =
        irs::IndexWriter::Make(*index->dir, irs::kOmCreate, std::move(options));
      index->tokenizer = MakeTokenizer();
      return index.release();
    },
    nullptr);
}

int irs_ffi_index_write(irs_ffi_index* index, irs_ffi_write_fn write,
                        void* ctx) {
  return Guard(
    [&] {
      if (index == nullptr || write == nullptr) {
        SetError("index or write callback is null");
        return -1;
      }
      if (index->failed) {
        SetError("index transaction failed; close the index");
        return -1;
      }
      if (index->batch) {
        SetError("commit the index before writing its archive");
        return -1;
      }

      std::vector<std::string> names;
      if (!index->dir->visit([&](std::string_view name) {
            if (name != kRowMapName) {
              names.emplace_back(name);
            }
            return true;
          })) {
        SetError("could not list the index files");
        return -1;
      }
      std::sort(names.begin(), names.end());

      std::string header;
      PutU32(header, kArchiveMagic);
      PutU32(header, static_cast<uint32_t>(names.size()));
      PutU64(header, static_cast<uint64_t>(index->rows.size()));
      for (const auto row : index->rows) {
        PutU64(header, static_cast<uint64_t>(row));
      }
      if (write(ctx, header.data(), header.size()) != 0) {
        SetError("caller rejected the archive header");
        return -1;
      }

      std::vector<irs::byte_type> buffer(kCopyChunk);
      for (const auto& name : names) {
        uint64_t length = 0;
        if (!index->dir->length(length, name)) {
          SetError("could not size " + name);
          return -1;
        }
        auto in = index->dir->open(name, irs::IOAdvice::ReadonceSequential);
        if (!in) {
          SetError("could not open " + name);
          return -1;
        }

        std::string entry;
        PutU32(entry, static_cast<uint32_t>(name.size()));
        entry.append(name);
        PutU64(entry, length);
        if (write(ctx, entry.data(), entry.size()) != 0) {
          SetError("caller rejected an archive entry");
          return -1;
        }

        for (uint64_t left = length; left != 0;) {
          const auto chunk =
            static_cast<size_t>(std::min<uint64_t>(left, buffer.size()));
          in->ReadData(buffer.data(), chunk);
          if (write(ctx, buffer.data(), chunk) != 0) {
            SetError("caller rejected the body of " + name);
            return -1;
          }
          left -= chunk;
        }
      }
      return 0;
    },
    -1);
}

irs_ffi_reader* irs_ffi_reader_open_bytes(const void* data, size_t len) {
  return Guard(
    [&]() -> irs_ffi_reader* {
      if (irs_ffi_init() != 0) {
        return nullptr;
      }
      const auto* p = static_cast<const unsigned char*>(data);
      if (p == nullptr || len < 8 || GetU32(p) != kArchiveMagic) {
        SetError("not an iresearch archive");
        return nullptr;
      }
      const uint32_t count = GetU32(p + 4);
      size_t offset = 8;

      if (offset + 8 > len) {
        SetError("archive truncated in the row map");
        return nullptr;
      }
      const auto rows_count = static_cast<size_t>(GetU64(p + offset));
      offset += 8;
      if (rows_count > (len - offset) / 8) {
        SetError("archive truncated in the row map");
        return nullptr;
      }
      std::vector<int64_t> rows(rows_count);
      for (size_t i = 0; i != rows_count; ++i) {
        const auto row = GetU64(p + offset);
        if (row > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          SetError("archive row id is out of range");
          return nullptr;
        }
        rows[i] = static_cast<int64_t>(row);
        offset += 8;
      }

      auto dir = std::make_unique<irs::MemoryDirectory>();
      for (uint32_t i = 0; i != count; ++i) {
        if (offset + 4 > len) {
          SetError("archive truncated in an entry header");
          return nullptr;
        }
        const uint32_t name_len = GetU32(p + offset);
        offset += 4;
        if (name_len == 0 || name_len > len - offset ||
            len - offset - name_len < 8) {
          SetError("archive truncated in an entry name");
          return nullptr;
        }
        const std::string name{reinterpret_cast<const char*>(p + offset),
                               name_len};
        offset += name_len;
        const uint64_t length = GetU64(p + offset);
        offset += 8;
        if (length > len - offset) {
          SetError("archive truncated in the body of " + name);
          return nullptr;
        }

        if (name.find('/') != std::string::npos ||
            name.find('\\') != std::string::npos || name == "." ||
            name == ".." || name.find('\0') != std::string::npos) {
          SetError("invalid archive entry name");
          return nullptr;
        }
        bool exists = false;
        if (!dir->exists(exists, name) || exists) {
          SetError("duplicate archive entry");
          return nullptr;
        }
        auto out = dir->create(name);
        if (!out) {
          SetError("could not create " + name);
          return nullptr;
        }
        out->WriteData(p + offset, length);
        out->Flush();
        offset += length;
      }

      auto reader = std::make_unique<irs_ffi_reader>();
      irs::IndexReaderOptions reader_options;
      reader_options.db = &irs::DuckDBEngine::Instance().instance();
      reader->dir = std::move(dir);
      reader->rows = std::move(rows);
      if (offset != len) {
        SetError("archive has trailing data");
        return nullptr;
      }
      reader->reader = irs::DirectoryReader{*reader->dir, reader_options};
      if (reader->reader.docs_count() != reader->rows.size()) {
        SetError("archive row map does not match the index");
        return nullptr;
      }
      reader->MapSegments();
      reader->tokenizer = MakeTokenizer();
      reader->scorer = irs::BM25::Make(irs::BM25::Options{});
      return reader.release();
    },
    nullptr);
}

void irs_ffi_index_close(irs_ffi_index* index) { delete index; }

irs_ffi_reader* irs_ffi_reader_open(const char* path, size_t path_len) {
  return Guard(
    [&]() -> irs_ffi_reader* {
      if (!path || path_len == 0 || irs_ffi_init() != 0) {
        SetError("invalid path or unavailable engine");
        return nullptr;
      }
      auto reader = std::make_unique<irs_ffi_reader>();
      reader->dir = std::make_unique<irs::FSDirectory>(
        std::filesystem::path{std::string{path, path_len}});
      irs::IndexReaderOptions reader_options;
      reader_options.db = &irs::DuckDBEngine::Instance().instance();
      reader->reader = irs::DirectoryReader{*reader->dir, reader_options};
      auto rows =
        reader->dir->open(kRowMapName, irs::IOAdvice::ReadonceSequential);
      if (!rows || rows->Length() < 8 || (rows->Length() - 8) % 8 != 0) {
        SetError("missing or invalid row map");
        return nullptr;
      }
      unsigned char encoded[8];
      rows->ReadData(encoded, sizeof(encoded));
      const auto count = GetU64(encoded);
      if (count != reader->reader.docs_count() ||
          count != (rows->Length() - 8) / 8) {
        SetError("row map does not match the index");
        return nullptr;
      }
      reader->rows.reserve(count);
      for (uint64_t i = 0; i < count; ++i) {
        rows->ReadData(encoded, sizeof(encoded));
        const auto row = GetU64(encoded);
        if (row > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          SetError("row id is out of range");
          return nullptr;
        }
        reader->rows.push_back(static_cast<int64_t>(row));
      }
      reader->MapSegments();
      reader->tokenizer = MakeTokenizer();
      reader->scorer = irs::BM25::Make(irs::BM25::Options{});
      return reader.release();
    },
    nullptr);
}

int64_t irs_ffi_reader_docs(const irs_ffi_reader* reader) {
  if (reader == nullptr) {
    return -1;
  }
  return static_cast<int64_t>(reader->reader.docs_count());
}

int64_t irs_ffi_search(irs_ffi_reader* reader, const char* query,
                       size_t query_len, irs_ffi_hit* hits, size_t hits_len) {
  return Guard(
    [&]() -> int64_t {
      if (reader == nullptr || (!query && query_len != 0) ||
          query_len > std::numeric_limits<uint32_t>::max() ||
          (hits == nullptr && hits_len != 0)) {
        SetError("reader is null or hits buffer is invalid");
        return -1;
      }

      auto root = std::make_unique<irs::BooleanFilter>();
      irs::ParserContext context{*root, kField, *reader->tokenizer};
      if (!irs::ParseQuery(context, std::string_view{query, query_len})) {
        SetError(context.error_message);
        return -1;
      }
      if (!root->Valid()) {
        SetError("query parsed into an unusable filter");
        return -1;
      }

      irs::Filter::ptr filter = std::move(root);
      irs::Optimize(filter, {.scored = true});

      std::vector<irs::ScoreDoc> found(std::max<size_t>(hits_len, 1));
      const auto total =
        irs::ExecuteTopK(reader->reader, *filter, *reader->scorer, found.size(),
                         /*score_prune=*/false, std::span{found});

      const size_t count = std::min<size_t>(hits_len, total);
      for (size_t i = 0; i != count; ++i) {
        hits[i] = {.row = reader->Row(found[i].doc, found[i].segment_idx),
                   .score = found[i].score};
      }
      return static_cast<int64_t>(total);
    },
    -1);
}

namespace {

using Roaring64 = std::unique_ptr<roaring::Roaring64Map>;

}

static int64_t SearchCore(irs_ffi_reader* reader, irs_ffi_search_type type,
                          const char* query, size_t query_len, size_t limit,
                          int with_score, float min_score,
                          const void* prefilter, size_t prefilter_len,
                          irs_ffi_hit* hits, size_t hits_len) {
  return Guard(
    [&]() -> int64_t {
      if (reader == nullptr || (!query && query_len != 0) ||
          query_len > std::numeric_limits<uint32_t>::max() ||
          (!hits && hits_len != 0) || (!prefilter && prefilter_len != 0) ||
          (prefilter && prefilter_len == 0)) {
        SetError("invalid reader, query, hits, or pre-filter buffer");
        return -1;
      }
      const std::string_view text{query ? query : "", query_len};

      Roaring64 allowed;
      if (prefilter != nullptr && prefilter_len != 0) {
        try {
          allowed = std::make_unique<roaring::Roaring64Map>(
            roaring::Roaring64Map::readSafe(static_cast<const char*>(prefilter),
                                            prefilter_len));
          if (allowed->getSizeInBytes() != prefilter_len) {
            SetError("pre-filter has trailing or duplicate data");
            return -1;
          }
        } catch (const std::exception& e) {
          SetError(std::string{"pre-filter is not a roaring bitmap: "} +
                   e.what());
          return -1;
        }
      }
      const auto passes = [&](int64_t row) {
        return !allowed || allowed->contains(static_cast<uint64_t>(row));
      };

      irs::Filter::ptr filter;
      switch (type) {
        case IRS_FFI_MATCH_ANY:
        case IRS_FFI_MATCH_ALL:
        case IRS_FFI_PHRASE:
          filter = AnalyzeQuery(text, type, *reader->tokenizer);
          break;
        case IRS_FFI_PREFIX:
        case IRS_FFI_WILDCARD: {
          if (text.empty()) {
            SetError("query is empty");
            return -1;
          }
          auto variants = std::make_unique<irs::BooleanFilter>();
          variants->SetMinShouldMatch(1);
          variants->SetMergeType(irs::ScoreMergeType::Max);
          const auto add = [&](std::string_view value) {
            if (type == IRS_FFI_PREFIX) {
              auto prefix = std::make_unique<irs::ByPrefix>();
              *prefix->mutable_field_id() = kField;
              prefix->mutable_options()->term = irs::bstring{AsBytes(value)};
              variants->Add(std::move(prefix), irs::Occur::Should);
            } else {
              auto wildcard = std::make_unique<irs::ByWildcard>();
              *wildcard->mutable_field_id() = kField;
              *wildcard->mutable_options() =
                irs::ByWildcardOptions{irs::bstring{AsBytes(value)}};
              variants->Add(std::move(wildcard), irs::Occur::Should);
            }
          };
          const auto raw = type == IRS_FFI_PREFIX
                             ? std::string{text}
                             : WildcardPattern(text, false);
          const auto normalized = type == IRS_FFI_PREFIX
                                    ? NormalizeLiteral(text)
                                    : WildcardPattern(text, true);
          add(raw);
          if (normalized != raw) {
            add(normalized);
          }
          filter = std::move(variants);
          break;
        }
        default:
          SetError("unknown search type");
          return -1;
      }
      const bool scored = with_score != 0 || !std::isnan(min_score);
      irs::Optimize(filter, {.scored = scored});

      if (!scored) {
        uint64_t total = 0;
        size_t written = 0;
        std::vector<irs::doc_id_t> batch(1024);
        uint32_t segment_idx = 0;
        for (auto& segment : reader->reader) {
          const auto current_segment = segment_idx++;
          auto query = filter->PrepareSegment(segment, {});
          if (!query) {
            continue;
          }
          auto plan = query->PlanDocs({});
          if (!plan) {
            continue;
          }
          const uint64_t end = irs::doc_limits::min() + segment.docs_count();
          for (uint64_t min = irs::doc_limits::min(); min < end;
               min += batch.size()) {
            const auto max = std::min<uint64_t>(end, min + batch.size());
            const auto count =
              plan->Run(static_cast<irs::doc_id_t>(min),
                        static_cast<irs::doc_id_t>(max), batch.data());
            for (uint32_t i = 0; i != count; ++i) {
              const auto row = reader->Row(batch[i], current_segment);
              if (!passes(row)) {
                continue;
              }
              ++total;
              if (written == hits_len || (limit != 0 && written == limit)) {
                continue;
              }
              hits[written++] = {.row = row, .score = 0.f};
            }
          }
        }
        return static_cast<int64_t>(total);
      }

      const size_t k = std::max<size_t>(reader->reader.docs_count(), 1);
      std::vector<irs::ScoreDoc> found(k);

      const auto total =
        irs::ExecuteTopK(reader->reader, *filter, *reader->scorer, k,
                         /*score_prune=*/false, std::span{found});

      std::sort(found.begin(), found.begin() + std::min<size_t>(k, total),
                [&](const auto& left, const auto& right) {
                  return left.score != right.score
                           ? left.score > right.score
                           : reader->Row(left.doc, left.segment_idx) <
                               reader->Row(right.doc, right.segment_idx);
                });
      size_t written = 0;
      uint64_t kept = 0;
      const size_t available = std::min<size_t>(k, total);
      for (size_t i = 0; i != available; ++i) {
        if (min_score == min_score && found[i].score <= min_score) {
          continue;
        }
        const auto row = reader->Row(found[i].doc, found[i].segment_idx);
        if (!passes(row)) {
          continue;
        }
        ++kept;
        if (written == hits_len || (limit != 0 && written == limit)) {
          continue;
        }
        hits[written++] = {.row = row,
                           .score = with_score ? found[i].score : 0.f};
      }
      return static_cast<int64_t>(allowed || !std::isnan(min_score) ? kept
                                                                    : total);
    },
    -1);
}

int64_t irs_ffi_search_typed(irs_ffi_reader* reader, irs_ffi_search_type type,
                             const char* query, size_t query_len, size_t limit,
                             int with_score, float min_score, irs_ffi_hit* hits,
                             size_t hits_len) {
  return SearchCore(reader, type, query, query_len, limit, with_score,
                    min_score, nullptr, 0, hits, hits_len);
}

int64_t irs_ffi_search_filtered(irs_ffi_reader* reader,
                                irs_ffi_search_type type, const char* query,
                                size_t query_len, size_t limit, int with_score,
                                float min_score, const void* prefilter,
                                size_t prefilter_len, irs_ffi_hit* hits,
                                size_t hits_len) {
  return SearchCore(reader, type, query, query_len, limit, with_score,
                    min_score, prefilter, prefilter_len, hits, hits_len);
}

void irs_ffi_reader_close(irs_ffi_reader* reader) { delete reader; }
