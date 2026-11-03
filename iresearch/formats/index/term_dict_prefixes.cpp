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

#include "iresearch/formats/index/term_dict_prefixes.hpp"

#include <limits>
#include <utility>

#include "iresearch/utils/bytes_utils.hpp"

namespace irs::term_dict {

std::vector<uint32_t> DecodePrefixes(bytes_view encoded, field_id id) {
  std::vector<uint32_t> prefixes;
  prefixes.reserve(
    absl::c_count_if(encoded, [](byte_type b) { return b < 0x80; }));
  uint64_t value = 0;
  const auto* const end = encoded.data() + encoded.size();
  for (const auto* p = encoded.data(); p != end;) {
    const auto delta = ReadVarint<uint32_t>(p, end);
    if (!delta) {
      Corrupted("prefixes", id);
    }
    value += *delta;
    if ((*delta == 0 && !prefixes.empty()) ||
        value > std::numeric_limits<uint32_t>::max()) {
      Corrupted("prefixes", id);
    }
    prefixes.push_back(static_cast<uint32_t>(value));
  }
  return prefixes;
}

PrefixIndex::PrefixIndex(std::vector<uint32_t> prefixes)
  : _prefixes{std::move(prefixes)} {
  for (size_t i = 0; i != _prefixes.size(); ++i) {
    const uint32_t first = _prefixes[i] >> 24;
    const uint32_t pair = _prefixes[i] >> 16;
    if (i == 0 || pair != _prefixes[i - 1] >> 16) {
      _pair_starts.push_back(static_cast<uint32_t>(i));
      _pairs[pair / 64] |= uint64_t{1} << (pair % 64);
      ++_pair_rows[first + 1];
    }
    _firsts[first / 64] |= uint64_t{1} << (first % 64);
    ++_buckets[first + 1];
  }
  _pair_starts.push_back(static_cast<uint32_t>(_prefixes.size()));
  for (size_t i = 1; i != _buckets.size(); ++i) {
    _buckets[i] += _buckets[i - 1];
    _pair_rows[i] += _pair_rows[i - 1];
  }
}

}  // namespace irs::term_dict
