////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Alex Geenen
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/analysis/tokenizer.hpp"

namespace fasttext {

class FastText;

}  // namespace fasttext
namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

class ClassificationTokenizer final
  : public TypedTokenizer<ClassificationTokenizer>,
    private util::Noncopyable {
 public:
  using model_ptr = duckdb::shared_ptr<const fasttext::FastText>;

  struct Options {
    using Owner = ClassificationTokenizer;
    std::string model_location;
    double threshold = 0.0;
    int32_t top_k = 1;
  };

  static constexpr std::string_view type_name() noexcept {
    return "classification";
  }

  static ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  explicit ClassificationTokenizer(const Options& options,
                                   model_ptr model) noexcept;

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = true,
      .offsets = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  model_ptr _model;
  std::vector<std::pair<float, std::string>> _predictions;
  double _threshold;
  int32_t _top_k;
};

extern template class TypedTokenizer<ClassificationTokenizer>;

}  // namespace irs::analysis
