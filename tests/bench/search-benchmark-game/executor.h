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

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <span>
#include <string>
#include <vector>

namespace bench {

struct BenchConfig {
  std::string_view format_name = "1_5simd";
  std::string_view scorer = "bm25";
  std::string_view scorer_options = R"({})";
  std::string_view tokenizer = "segmentation";
  std::string_view tokenizer_options = R"({})";
  size_t segment_mem_max = 1 << 28;
};

// What a query line asks for besides the documents themselves: a checksum
// over them, every one of them printed, or both -- and by default neither,
// which leaves only how many there were.
struct Report {
  bool hash = false;
  bool print = false;
};

struct EmitResult {
  size_t count = 0;
  size_t hash = 0;
};

class Executor {
 public:
  explicit Executor(std::string_view path, const BenchConfig& config = {});

  size_t ExecuteTopK(size_t k, std::string_view query);
  size_t ExecuteTopKWithCount(size_t k, std::string_view query);
  size_t ExecuteCount(std::string_view query);
  size_t HashResults() const;
  void PrintResults() const;
  EmitResult ExecuteEmitDocs(std::string_view query, Report report = {});
  EmitResult ExecuteEmitScoredDocs(std::string_view query, Report report = {});

  const irs::DirectoryReader& GetReader() const { return _reader; }
  auto GetResults(this auto& self) {
    return std::span{self._results.data(), self._result_count};
  }

  irs::Filter::ptr ParseFilter(std::string_view str);

 private:
  void ResetResults(size_t k) noexcept {
    _results.resize(k);
    std::memset(static_cast<void*>(_results.data()), 0,
                k * sizeof(_results[0]));
  }

  static constexpr size_t kEmitWindow = STANDARD_VECTOR_SIZE;

  std::vector<irs::ScoreDoc> _results;
  std::array<irs::doc_id_t, kEmitWindow> _emit_docs;
  std::array<irs::score_t, kEmitWindow> _emit_scores;
  size_t _result_count{0};
  irs::Scorer::ptr _scorer;
  irs::Scorer* _scorer_ptr{_scorer.get()};
  irs::analysis::Analyzer::ptr _tokenizer;
  irs::Format::ptr _format;
  irs::MMapDirectory _dir;
  irs::DirectoryReader _reader;
};

}  // namespace bench
