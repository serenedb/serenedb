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

#pragma once

#include <array>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>

#include "executor.h"
#include "line_source.h"

namespace bench {

struct BatchDoc {
  std::string_view id;
  std::string_view text;
};

class Batch {
 public:
  void Add(std::string_view id, std::string_view text);
  void Seal();

  size_t Size() const noexcept { return _spans.size(); }
  std::span<const BatchDoc> Docs() const noexcept { return _docs; }

 private:
  struct Span {
    size_t offset;
    size_t id;
    size_t text;
  };

  std::string _arena;
  std::vector<Span> _spans;
  std::vector<BatchDoc> _docs;
};

struct IBatchHandler {
  virtual ~IBatchHandler() = default;
  virtual void operator()(const Batch& batch,
                          irs::IndexWriter::Transaction& ctx) = 0;
};

using BatchHandlerFactory = std::unique_ptr<IBatchHandler> (*)();

struct IndexBuilderOptions {
  size_t batch_size = 100000;
  size_t indexer_threads = 1;
  size_t refresh_interval_ms = 0;
  size_t compaction_interval_ms = 5000;
  size_t compaction_threads = 0;
  bool compact_all = true;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
};

class IndexBuilder {
 public:
  IndexBuilder(std::string_view path, const IndexBuilderOptions& opts,
               const BenchConfig& config);

  void IndexFrom(LineSource& source, BatchHandlerFactory factory);

  irs::MMapDirectory& GetDirectory() { return _dir; }
  irs::IndexWriter& GetWriter() { return *_writer; }
  auto GetReader() { return _writer->GetSnapshot(); }

 private:
  void CompactAll();

  IndexBuilderOptions _opts;
  irs::Scorer::ptr _scorer;
  irs::Scorer* _scorer_ptr{_scorer.get()};
  irs::MMapDirectory _dir;
  irs::IndexWriter::ptr _writer;
};

inline constexpr auto kTextIndexFeatures =
  irs::IndexFeatures::Freq | irs::IndexFeatures::Pos | irs::IndexFeatures::Norm;

inline constexpr irs::field_id kIdFieldId = 1;
inline constexpr irs::field_id kTextFieldId = 2;

struct TextField {
  irs::field_id id{irs::field_limits::invalid()};
  std::string_view text;
  irs::analysis::Tokenizer::ptr tokenizer{irs::analysis::TextTokenizer::Make(
    irs::analysis::TextTokenizer::Options{})};

  irs::field_id Id() const noexcept { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return *tokenizer; }

  std::string_view Value() const noexcept { return text; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return kTextIndexFeatures;
  }

  bool Write(irs::DataOutput& out) const;
};

struct Document {
  std::array<TextField, 2> fields{
    TextField{.id = kIdFieldId},
    TextField{.id = kTextFieldId},
  };

  void Fill(const BatchDoc& doc) noexcept {
    fields[0].text = doc.id;
    fields[1].text = doc.text;
  }
};

}  // namespace bench
