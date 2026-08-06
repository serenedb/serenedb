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

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string>
#include <string_view>

struct sb_stemmer;

namespace irs::analysis {

// Two-tier per-instance stem cache. Inline-size words (<= 12B, the vast
// majority) are keyed by the 16 raw bytes of their canonical inline handle:
// integer hash + 16-byte integer equality, no per-probe memcmp against slot
// strings (72% of the pipeline stem arm before this split); longer cacheable
// words key a string map. Returns nullopt when the stemmer declines; the
// returned view is valid until the next Stem call.
class StemCache {
 public:
  static constexpr size_t kMaxCachedKey = 64;
  static constexpr size_t kMaxCacheEntries = 65536;

  std::optional<std::string_view> Stem(sb_stemmer* stemmer,
                                       std::string_view word);

 private:
  absl::flat_hash_map<__uint128_t, std::string> _cache_inline;
  absl::flat_hash_map<std::string, std::string> _cache;
};

}  // namespace irs::analysis
