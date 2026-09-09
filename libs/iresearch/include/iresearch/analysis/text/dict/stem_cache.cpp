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

#include <cstring>

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

const char* StemCache::Store(duckdb::ArenaAllocator& arena,
                             std::string_view stem) {
  auto* data = arena.Allocate(stem.size());
  std::memcpy(data, stem.data(), stem.size());
  return reinterpret_cast<const char*>(data);
}

void StemCache::Compact() {
  duckdb::ArenaAllocator survivors{_arena.GetAllocator()};
  _stems.ForEachMapped([&](duckdb::string_t& stem) {
    if (!stem.IsInlined()) {
      const auto size = static_cast<uint32_t>(stem.GetSize());
      stem = duckdb::string_t{Store(survivors, {stem.GetData(), size}), size};
    }
  });
  _arena.Destroy();
  survivors.Move(_arena);
}

const duckdb::string_t& StemCache::Insert(const duckdb::string_t& word,
                                          std::string_view stem) {
  if (_stems.Size() == kMaxEntries) {
    _stems.EraseHalf();
    Compact();
  }
  const auto size = static_cast<uint32_t>(stem.size());
  auto& entry = _stems[std::string{word.GetData(), word.GetSize()}];
  entry = size <= duckdb::string_t::INLINE_LENGTH
            ? duckdb::string_t{stem.data(), size}
            : duckdb::string_t{Store(_arena, stem), size};
  return entry;
}

std::optional<std::string_view> StemCache::Stem(sb_stemmer* stemmer,
                                                const duckdb::string_t& word) {
  if (const auto* stem = Find(word)) {
    return std::string_view{stem->GetData(), stem->GetSize()};
  }
  const auto stemmed = StemUncached(stemmer, {word.GetData(), word.GetSize()});
  if (!stemmed) {
    return std::nullopt;
  }
  const auto& entry = Insert(word, *stemmed);
  return std::string_view{entry.GetData(), entry.GetSize()};
}

}  // namespace irs::analysis::dict
