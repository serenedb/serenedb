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
#include "iresearch/utils/attribute_helper.hpp"

namespace fasttext {

class FastText;

}  // namespace fasttext
namespace irs::analysis {

class ClassificationTokenizer final
  : public TypedTokenizer<ClassificationTokenizer>,
    private util::Noncopyable {
 public:
  using model_ptr = std::shared_ptr<const fasttext::FastText>;
  using model_provider_f = model_ptr (*)(std::string_view);

  static model_provider_f set_model_provider(
    model_provider_f provider) noexcept;

  struct Options {
    using Owner = ClassificationTokenizer;
    std::string model_location;
    double threshold = 0.0;
    int32_t top_k = 1;
  };

  static constexpr std::string_view type_name() noexcept {
    return "classification";
  }

  static ptr Make(Options opts);

  explicit ClassificationTokenizer(const Options& options,
                                   model_ptr mode) noexcept;

  TokenTraits Traits() const noexcept final { return {.dense_pos = false}; }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

 private:
  bool Bind(std::string_view value);

  model_ptr _model;
  std::vector<std::pair<float, std::string>> _predictions;
  std::vector<std::pair<float, std::string>>::iterator _predictions_it;
  double _threshold;
  int32_t _top_k;
  uint32_t _input_size = 0;
};

extern template class TypedTokenizer<ClassificationTokenizer>;

}  // namespace irs::analysis
