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

#include <absl/synchronization/mutex.h>

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <tuple>
#include <vector>

#include "catalog/object.h"
#include "catalog/search_analyzer_impl.h"
#include "vpack/builder.h"
#include "vpack/slice.h"

namespace sdb::catalog {

class AnalyzersPool {
 public:
  AnalyzersPool(std::string data) : _data{std::move(data)} {
    auto analyzer = CreateAnalyzer();
    _pool.push_back(std::move(analyzer));
  }

  irs::analysis::Analyzer::ptr GetAnalyzer();

  void PushAnalyzer(irs::analysis::Analyzer::ptr analyzer) noexcept;

  vpack::Slice GetAnalyzerOptions() const {
    return vpack::Slice{reinterpret_cast<const uint8_t*>(_data.data())};
  }

 protected:
  irs::analysis::Analyzer::ptr CreateAnalyzer() const;

 private:
  absl::Mutex _m;
  std::vector<irs::analysis::Analyzer::ptr> _pool;
  std::string _data;
};

class Tokenizer : public SchemaObject {
 public:
  irs::analysis::Analyzer::ptr GetTokenizer() const {
    return _pool->GetAnalyzer();
  }
  void PushTokenizer(irs::analysis::Analyzer::ptr analyzer) noexcept {
    return _pool->PushAnalyzer(std::move(analyzer));
  }

  void WriteInternal(vpack::Builder& b) const final;

  Tokenizer(ObjectId id, std::string_view name, search::Features features,
            std::string data);

 private:
  std::unique_ptr<AnalyzersPool> _pool;
  search::Features _features;
};

}  // namespace sdb::catalog
