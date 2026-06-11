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
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <memory>
#include <tuple>
#include <vector>

#include "catalog/object.h"
#include "catalog/search_analyzer_impl.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

class Tokenizer : public Object {
 public:
  struct Deleter {
    Tokenizer* tokenizer{nullptr};

    void operator()(irs::analysis::Analyzer* analyzer) {
      // TODO(mbkkt) Revert this when global identity will be available
      if (tokenizer) {
        tokenizer->PushTokenizer(irs::analysis::Analyzer::ptr{analyzer});
      } else {
        delete analyzer;
      }
    }
  };

  using TokenizerWrapper = std::unique_ptr<irs::analysis::Analyzer, Deleter>;

  static std::shared_ptr<Tokenizer> Deserialize(duckdb::Deserializer& src,
                                                ReadContext ctx);

  ResultOr<TokenizerWrapper> GetTokenizer();

  void PushTokenizer(irs::analysis::Analyzer::ptr analyzer) noexcept;

  const auto& Config() const noexcept { return _config; }

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  Tokenizer(ObjectId schema_id, ObjectId id, std::string_view name,
            search::Features features, uint32_t norm_row_group_size,
            irs::analysis::TokenizerConfig config);

  const search::Features& GetFeatures() const noexcept { return _features; }

  uint32_t GetNormRowGroupSize() const noexcept { return _norm_row_group_size; }

 private:
  irs::analysis::Analyzer::ptr CreateAnalyzer() const;

  mutable absl::Mutex _m;
  std::vector<irs::analysis::Analyzer::ptr> _pool;
  irs::analysis::TokenizerConfig _config;
  search::Features _features;
  uint32_t _norm_row_group_size;
};

}  // namespace sdb::catalog
