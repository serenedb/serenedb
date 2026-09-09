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

#include <duckdb/storage/shared_object_cache.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/expand_tokens.hpp"
#include "iresearch/analysis/text/dict/string_table.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

class WordnetSynonymsTokenizer final
  : public TypedTokenizer<WordnetSynonymsTokenizer>,
    public TypedTokenExpander<WordnetSynonymsTokenizer>,
    private util::Noncopyable {
 public:
  using SynonymsGroups = std::vector<std::string_view>;
  using SynonymsMap = dict::StringMap<std::string_view, SynonymsGroups>;

  // `mapping`'s keys and value string_views reference `text`, whose escaped
  // lemmas `Parse` unescapes in place.
  // Members are listed in lifetime order: text must outlive mapping.
  struct State : duckdb::ObjectCacheEntry {
    static constexpr std::string_view ObjectType() {
      return "wordnet_synonyms_state";
    }

    std::string GetObjectType() final { return std::string{ObjectType()}; }

    duckdb::optional_idx GetEstimatedCacheMemory() const final {
      size_t size = text.size() + mapping.MemoryBytes();
      mapping.ForEachMapped([&](const SynonymsGroups& groups) {
        size += groups.capacity() * sizeof(std::string_view);
      });
      return size;
    }

    std::string text;
    SynonymsMap mapping;
  };

  struct Options {
    using Owner = WordnetSynonymsTokenizer;
    // Inline synonyms file content (Wordnet `s(...)` lines).
    std::string synonyms_text;
  };
  static Tokenizer::ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static constexpr std::string_view type_name() noexcept {
    return "wordnet_synonyms";
  }

  static SynonymsMap Parse(std::string& input);
  static duckdb::unique_ptr<State> MakeState(std::string text);

  explicit WordnetSynonymsTokenizer(
    duckdb::shared_ptr<const State> state) noexcept;
  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = true};
  }

  template<TokenLayout Layout, typename Sink>
  IRS_FORCE_INLINE bool DoFill(const duckdb::string_t& value, Sink& sink);

 private:
  duckdb::shared_ptr<const State> _state;
};

extern template class TypedTokenizer<WordnetSynonymsTokenizer>;
extern template class TypedTokenExpander<WordnetSynonymsTokenizer>;

}  // namespace irs::analysis
