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

#include "iresearch/analysis/stem_cache.hpp"

#include <libstemmer.h>

#include <cstring>

#include "iresearch/analysis/term_view.hpp"

namespace irs::analysis {

std::optional<std::string_view> StemCache::Stem(sb_stemmer* stemmer,
                                                std::string_view word) {
  const bool inline_key = word.size() <= duckdb::string_t::INLINE_LENGTH;
  const bool cacheable = word.size() <= kMaxCachedKey;
  __uint128_t ikey;
  if (inline_key) {
    const auto handle =
      MakeTermView(word.data(), static_cast<uint32_t>(word.size()));
    std::memcpy(&ikey, &handle, sizeof ikey);
    if (const auto it = _cache_inline.find(ikey); it != _cache_inline.end()) {
      return it->second;
    }
  } else if (cacheable) {
    if (const auto it = _cache.find(word); it != _cache.end()) {
      return it->second;
    }
  }
  static_assert(sizeof(sb_symbol) == sizeof(char));
  const auto* value =
    sb_stemmer_stem(stemmer, reinterpret_cast<const sb_symbol*>(word.data()),
                    static_cast<int>(word.size()));
  if (value == nullptr) {
    return std::nullopt;
  }
  const std::string_view stemmed{
    reinterpret_cast<const char*>(value),
    static_cast<size_t>(sb_stemmer_length(stemmer))};
  if (inline_key) {
    if (_cache_inline.size() >= kMaxCacheEntries) [[unlikely]] {
      _cache_inline.clear();
    }
    return _cache_inline.emplace(ikey, stemmed).first->second;
  }
  if (cacheable) {
    if (_cache.size() >= kMaxCacheEntries) [[unlikely]] {
      _cache.clear();
    }
    return _cache.emplace(word, stemmed).first->second;
  }
  return stemmed;
}

}  // namespace irs::analysis
