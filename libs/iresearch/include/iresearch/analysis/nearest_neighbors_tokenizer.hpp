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

class ImmutableFastText;
class Dictionary;

}  // namespace fasttext
namespace irs::analysis {

class NearestNeighborsTokenizer final
  : public TypedTokenizer<NearestNeighborsTokenizer>,
    private util::Noncopyable {
 public:
  using model_ptr = std::shared_ptr<const fasttext::ImmutableFastText>;
  using model_provider_f = model_ptr (*)(std::string_view);

  static model_provider_f set_model_provider(
    model_provider_f provider) noexcept;

  struct Options {
    using Owner = NearestNeighborsTokenizer;
    std::string model_location;
    int32_t top_k{1};
  };
  static Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "nearest_neighbors";
  }

  explicit NearestNeighborsTokenizer(const Options& options,
                                     model_ptr model_provider) noexcept;

  TokenTraits Traits() const noexcept final { return {.dense_pos = false}; }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

 private:
  bool Bind(std::string_view value);

  model_ptr _model;
  std::shared_ptr<const fasttext::Dictionary> _model_dict;
  std::vector<std::pair<float, std::string>> _neighbors;
  std::vector<std::pair<float, std::string>>::iterator _neighbors_it;
  std::vector<int32_t> _line_token_ids;
  std::vector<int32_t> _line_token_label_ids;
  int32_t _n_tokens = 0;
  int32_t _current_token_ind = 0;
  int32_t _top_k;
  uint32_t _input_size = 0;
};

extern template class TypedTokenizer<NearestNeighborsTokenizer>;

}  // namespace irs::analysis
