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

#include <magic_enum/magic_enum.hpp>
#include <tuple>
#include <vector>

#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

class NGramTokenizer final : public TypedTokenizer<NGramTokenizer>,
                             private util::Noncopyable {
 public:
  enum class InputType : uint8_t {
    Binary,
    UTF8,
  };

  enum class NGramMode : uint8_t {
    All,
    Prefix,
    Suffix,
    PrefixAndSuffix,
  };

  enum class Kernel : uint8_t {
    AllFixed,
    AllVariable,
    Prefix,
    Suffix,
    PrefixAndSuffix,
  };

  struct Options {
    using Owner = NGramTokenizer;
    size_t min_gram{0};
    size_t max_gram{0};
    bool preserve_original{true};
    InputType stream_bytes_type{InputType::Binary};
    bstring start_marker;
    bstring end_marker;
    NGramMode ngram_mode{NGramMode::All};
  };

  static constexpr std::string_view type_name() noexcept { return "ngram"; }
  static Tokenizer::ptr Make(Options opts);

  explicit NGramTokenizer(Options&& options);

  size_t min_gram() const noexcept { return _options.min_gram; }
  size_t max_gram() const noexcept { return _options.max_gram; }
  bool preserve_original() const noexcept { return _options.preserve_original; }

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = !DenseFill(),
      .offsets = true,
    };
  }

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = _options.stream_bytes_type == InputType::UTF8};
  }

  std::tuple<bool, Kernel, bool> PrepareBatch(BlockTraits traits) const;

  size_t MemoryUsage() const noexcept final {
    return _fill_bounds.capacity() * sizeof(uint32_t);
  }

  template<TokenLayout Layout, bool Plain, Kernel K, bool KnownAscii>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  bool PlainFill() const noexcept {
    return !_options.preserve_original && _options.start_marker.empty() &&
           _options.end_marker.empty();
  }

  bool DenseFill() const noexcept {
    return PlainFill() && _options.ngram_mode == NGramMode::All &&
           _options.min_gram == _options.max_gram;
  }

  Options _options;
  std::vector<uint32_t> _fill_bounds;
  Kernel _kernel;
};

extern template class TypedTokenizer<NGramTokenizer>;

}  // namespace irs::analysis
namespace magic_enum {

// The one reflection of NGramMode, beside the enum: every TU that reflects it
// must see the same names, or the linker mixes the per-TU tables (ODR). The
// names are the user surface -- the `mode` option of the ngram dictionary.
template<>
constexpr customize::customize_t
customize::enum_name<irs::analysis::NGramTokenizer::NGramMode>(
  irs::analysis::NGramTokenizer::NGramMode value) noexcept {
  using NGramMode = irs::analysis::NGramTokenizer::NGramMode;
  switch (value) {
    case NGramMode::All:
      return "all";
    case NGramMode::Prefix:
      return "only_prefix";
    case NGramMode::Suffix:
      return "only_suffix";
    case NGramMode::PrefixAndSuffix:
      return "only_prefix_and_suffix";
  }
  return invalid_tag;
}

}  // namespace magic_enum
