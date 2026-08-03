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

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <string>
#include <string_view>

#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/first_len_filter.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

class WordnetSynonymsTokenizer final
  : public TypedTokenizer<WordnetSynonymsTokenizer>,
    private util::Noncopyable {
 public:
  using SynonymsGroups = std::vector<duckdb::string_t>;
  using SynonymsMap = absl::flat_hash_map<std::string, SynonymsGroups>;

  // `mapping`'s value string_views reference `text`; the keys own their own
  // storage.
  // Members are listed in lifetime order: text must outlive mapping.
  struct State {
    std::string text;
    Prefiltered<SynonymsMap> mapping;
  };

  struct Options {
    using Owner = WordnetSynonymsTokenizer;
    // Inline synonyms file content (Wordnet `s(...)` lines).
    std::string synonyms_text;
  };
  static Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "wordnet_synonyms";
  }

  static SynonymsMap Parse(std::string_view input);
  static std::shared_ptr<const State> MakeState(std::string text);

  explicit WordnetSynonymsTokenizer(
    std::shared_ptr<const State> state) noexcept;
  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<TokenLayout Layout>
  bool DoFill(const duckdb::string_t& value, TokenSink& sink);

  const SynonymsGroups* Lookup(const duckdb::string_t& value) const noexcept {
    return _state->mapping.Find(
      std::string_view{value.GetData(), value.GetSize()});
  }

 private:
  std::shared_ptr<const State> _state;
};

extern template class TypedTokenizer<WordnetSynonymsTokenizer>;

}  // namespace irs::analysis
