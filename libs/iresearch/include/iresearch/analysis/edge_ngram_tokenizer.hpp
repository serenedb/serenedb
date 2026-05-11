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

#include <cstddef>
#include <string_view>

#include "analyzer.hpp"
#include "token_attributes.hpp"

namespace irs {
namespace analysis {

/// @brief UTF-8 edge (prefix) n-grams for one input string.
/// @note Configuration:
///       - min (number, required): minimum UTF-8 code point count
///       - max (number, optional): maximum code point count; if omitted,
///         prefixes grow to the end of the input
///       - preserveOriginal (bool, optional, default false)
class EdgeNGramTokenizer final : public TypedAnalyzer<EdgeNGramTokenizer> {
 public:
  struct Options {
    size_t min_gram = 1;
    size_t max_gram = 1;
    bool preserve_original = false;
    /// When false, `max_gram` is ignored (unbounded prefix growth).
    bool max_gram_set = true;
  };

  static constexpr std::string_view type_name() noexcept {
    return "edge_ngram";
  }

  static void init();  // trigger registration in static builds
  static Analyzer::ptr make(Options&& options);

  ~EdgeNGramTokenizer() override;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  bool next() final;
  bool reset(std::string_view data) final;

  explicit EdgeNGramTokenizer(Options&& options) noexcept;

 private:
  // UTF-8 scan: end of current prefix in bytes; code point count for min/max.
  const byte_type* _prefix_end{};
  uint32_t _code_points{};

  using Attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  Attributes _attrs;
  const Options _options;

  bool _term_eof = true;
  bytes_view _data;
};

}  // namespace analysis
}  // namespace irs
