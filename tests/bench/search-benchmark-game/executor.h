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
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/text_format.hpp>
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

class Executor {
 public:
  explicit Executor(std::string_view path, const BenchConfig& config = {});

  size_t ExecuteTopK(size_t k, std::string_view query);
  size_t ExecuteTopKWithCount(size_t k, std::string_view query);
  size_t ExecuteCount(std::string_view query);

  const irs::DirectoryReader& GetReader() const { return _reader; }
  auto GetResults(this auto& self) {
    return std::span{self._results.data(), self._result_count};
  }

  irs::Filter::ptr ParseFilter(std::string_view str);

 private:
  void ResetResults(size_t k) noexcept {
    const auto size = irs::BlockSize(k);
    _results.resize(size);
    std::memset(static_cast<void*>(_results.data()), 0,
                size * sizeof(_results[0]));
  }
  std::vector<irs::ScoreDoc> _results;
  size_t _result_count{0};
  irs::Scorer::ptr _scorer;
  irs::Scorer* _scorer_ptr{_scorer.get()};
  irs::analysis::Analyzer::ptr _tokenizer;
  irs::Format::ptr _format;
  irs::MMapDirectory _dir;
  irs::DirectoryReader _reader;
};

}  // namespace bench
