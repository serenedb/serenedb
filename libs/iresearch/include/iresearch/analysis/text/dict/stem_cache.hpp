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

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "iresearch/analysis/text/dict/string_table.hpp"

struct sb_stemmer;

namespace irs::analysis::dict {

// Returns nullopt when the stemmer declines; the returned view is valid until
// the next Stem or Insert call.
class StemCache {
 public:
  IRS_FORCE_INLINE const std::string* Find(
    const duckdb::string_t& word) const noexcept {
    return _stems.Find(word);
  }

  const std::string& Insert(const duckdb::string_t& word,
                            std::string_view stem);

  std::optional<std::string_view> Stem(sb_stemmer* stemmer,
                                       const duckdb::string_t& word);

  size_t MemoryBytes() const noexcept { return _stems.MemoryBytes(); }

 private:
  static constexpr size_t kMaxEntries = size_t{1} << 16;

  StringMap<std::string, std::string> _stems;
};

// The stemmer's result view, valid until the next stem call; nullopt when
// the stemmer declines.
std::optional<std::string_view> StemUncached(sb_stemmer* stemmer,
                                             std::string_view word);

}  // namespace irs::analysis::dict
