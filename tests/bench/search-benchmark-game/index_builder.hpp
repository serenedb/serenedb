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

#include <simdjson.h>

#include <array>
#include <iosfwd>

#include "executor.hpp"

namespace bench {

struct IBatchHandler {
  virtual ~IBatchHandler() = default;
  virtual void operator()(std::vector<std::string>& buf,
                          irs::IndexWriter::Transaction& ctx) = 0;
};

using BatchHandlerFactory = std::unique_ptr<IBatchHandler> (*)();

struct IndexBuilderOptions {
  size_t batch_size = 100000;
  size_t indexer_threads = 1;
  size_t commit_interval_ms = 0;
  size_t consolidation_interval_ms = 5000;
  size_t consolidation_threads = 0;
  bool consolidate_all = true;
};

class IndexBuilder {
 public:
  IndexBuilder(std::string_view path, const IndexBuilderOptions& opts,
               const BenchConfig& config);

  void IndexFromStream(std::istream& input, BatchHandlerFactory factory);

  irs::MMapDirectory& GetDirectory() { return _dir; }
  auto GetReader() { return _writer->GetSnapshot(); }

 private:
  void ConsolidateAll();

  IndexBuilderOptions _opts;
  irs::Scorer::ptr _scorer;
  irs::Scorer* _scorer_ptr{_scorer.get()};
  irs::MMapDirectory _dir;
  irs::Format::ptr _format;
  irs::IndexWriter::ptr _writer;
};

inline constexpr auto kTextIndexFeatures =
  irs::IndexFeatures::Freq | irs::IndexFeatures::Pos | irs::IndexFeatures::Norm;

struct TextField {
  std::string_view name;
  std::string_view text;
  irs::analysis::Analyzer::ptr tokenizer{irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(), R"({})")};

  std::string_view Name() const noexcept { return name; }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(text);
    return *tokenizer;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return kTextIndexFeatures;
  }

  bool Write(irs::DataOutput& out) const;
};

struct Document {
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document json_doc;
  std::array<TextField, 2> fields{
    TextField{.name = "id"},
    TextField{.name = "text"},
  };

  void Fill(std::string_view line);
};

}  // namespace bench
