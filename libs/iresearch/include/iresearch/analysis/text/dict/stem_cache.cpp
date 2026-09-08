////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/analysis/text/dict/stem_cache.hpp"

#include <libstemmer.h>

namespace irs::analysis::dict {

std::optional<std::string_view> StemUncached(sb_stemmer* stemmer,
                                             std::string_view word) {
  static_assert(sizeof(sb_symbol) == sizeof(char));
  const auto* value =
    sb_stemmer_stem(stemmer, reinterpret_cast<const sb_symbol*>(word.data()),
                    static_cast<int>(word.size()));
  if (!value) {
    return std::nullopt;
  }
  return std::string_view{reinterpret_cast<const char*>(value),
                          static_cast<size_t>(sb_stemmer_length(stemmer))};
}

const std::string& StemCache::Insert(const duckdb::string_t& word,
                                     std::string_view stem) {
  if (_stems.Size() == kMaxEntries) {
    _stems.EraseHalf();
  }
  auto& entry = _stems[std::string{word.GetData(), word.GetSize()}];
  entry = stem;
  return entry;
}

std::optional<std::string_view> StemCache::Stem(sb_stemmer* stemmer,
                                                const duckdb::string_t& word) {
  if (const auto* stem = Find(word)) {
    return std::string_view{*stem};
  }
  const auto stemmed = StemUncached(stemmer, {word.GetData(), word.GetSize()});
  if (!stemmed) {
    return std::nullopt;
  }
  return std::string_view{Insert(word, *stemmed)};
}

}  // namespace irs::analysis::dict
