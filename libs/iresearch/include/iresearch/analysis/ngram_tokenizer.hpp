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

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class ngram_token_stream
/// @brief produces ngram from a specified input in a range of
///         [min_gram;max_gram]. Can optionally preserve the original input.
////////////////////////////////////////////////////////////////////////////////
class NGramTokenizerBase : public TypedAnalyzer<NGramTokenizerBase>,
                           private util::Noncopyable {
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
  static ptr Make(Options opts);

  explicit NGramTokenizerBase(Options&& options);

  bool reset(std::string_view value) noexcept final;
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  size_t min_gram() const noexcept { return _options.min_gram; }
  size_t max_gram() const noexcept { return _options.max_gram; }
  bool preserve_original() const noexcept { return _options.preserve_original; }

 protected:
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  void emit_original() noexcept;

  Options _options;
  bytes_view _data;  // data to process
  attributes _attrs;
  const byte_type* _begin{};
  const byte_type* _data_end{};
  const byte_type* _ngram_end{};
  size_t _length{};

  enum class EmitOriginal {
    None,
    WithoutMarkers,
    WithStartMarker,
    WithEndMarker
  };

  EmitOriginal _emit_original{EmitOriginal::None};

  // buffer for emitting ngram with start/stop marker
  // we need continious memory for value so can not use
  // pointers to input memory block
  bstring _marked_term_buffer;

  // increment value for next token
  uint32_t _next_inc_val{0};

  // Aux flags to speed up marker properties access;
  bool _start_marker_empty;
  bool _end_marker_empty;
};

template<NGramTokenizerBase::InputType StreamType>
class NGramTokenizer : public NGramTokenizerBase {
 public:
  static ptr make(NGramTokenizerBase::Options&& options);

  explicit NGramTokenizer(NGramTokenizerBase::Options&& options);

  bool next() noexcept final;

 private:
  inline bool NextSymbol(const byte_type*& it) const noexcept;
};

}  // namespace analysis

// use ngram_tokenizer_base type for ancestors
template<analysis::NGramTokenizerBase::InputType StreamType>
struct Type<analysis::NGramTokenizer<StreamType>>
  : Type<analysis::NGramTokenizerBase> {};

}  // namespace irs
