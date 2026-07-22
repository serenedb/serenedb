////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vector>

#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class ngram_token_stream
/// @brief produces ngram from a specified input in a range of
///         [min_gram;max_gram]. Can optionally preserve the original input.
////////////////////////////////////////////////////////////////////////////////
class NGramTokenizerBase : private util::Noncopyable {
 public:
  enum class InputType : uint8_t {
    Binary,  // input is treaten as generic bytes
    UTF8,    // input is treaten as ut8-encoded symbols
  };

  struct Options {
    using Owner = NGramTokenizerBase;
    size_t min_gram{0};
    size_t max_gram{0};
    bool preserve_original{true};  // emit input data as a token
    InputType stream_bytes_type{InputType::Binary};
    bstring start_marker;  // marker of ngrams at the beginning of stream
    bstring end_marker;    // marker of ngrams at the end of strem
  };

  static constexpr std::string_view type_name() noexcept { return "ngram"; }
  static Tokenizer::ptr Make(Options opts);

  explicit NGramTokenizerBase(Options&& options);

  size_t min_gram() const noexcept { return _options.min_gram; }
  size_t max_gram() const noexcept { return _options.max_gram; }
  bool preserve_original() const noexcept { return _options.preserve_original; }

 protected:
  bool Bind(std::string_view value) noexcept;

  Options _options;
  bytes_view _data;  // data to process
  const byte_type* _data_end{};

  enum class EmitOriginal {
    None,
    WithoutMarkers,
    WithStartMarker,
    WithEndMarker
  };

  template<TokenLayout Layout, bool Identity>
  void EmitGrams(TokenEmitter& sink, const uint32_t* bounds, uint32_t nsym);
  void BuildBoundaries();

  std::vector<uint32_t> _fill_bounds;
};

template<NGramTokenizerBase::InputType StreamType>
class NGramTokenizer : public TypedTokenizer<NGramTokenizer<StreamType>>,
                       public NGramTokenizerBase {
 public:
  static Tokenizer::ptr make(NGramTokenizerBase::Options&& options);

  explicit NGramTokenizer(NGramTokenizerBase::Options&& options);

  TokenTraits Traits() const noexcept final {
    return {.dense_pos = false};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

 private:
};

}  // namespace analysis

// use ngram_tokenizer_base type for ancestors
template<analysis::NGramTokenizerBase::InputType StreamType>
struct Type<analysis::NGramTokenizer<StreamType>>
  : Type<analysis::NGramTokenizerBase> {};

namespace analysis {

extern template class TypedTokenizer<
  NGramTokenizer<NGramTokenizerBase::InputType::Binary>>;
extern template class TypedTokenizer<
  NGramTokenizer<NGramTokenizerBase::InputType::UTF8>>;

}  // namespace analysis
}  // namespace irs
