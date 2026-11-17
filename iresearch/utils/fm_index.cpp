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

#include "iresearch/utils/fm_index.hpp"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <array>
#include <functional>
#include <limits>
#include <sdsl/construct.hpp>
#include <sdsl/csa_wt.hpp>
#include <sdsl/rrr_vector.hpp>
#include <sdsl/suffix_array_algorithm.hpp>
#include <sdsl/wavelet_trees.hpp>
#include <utility>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

struct FmIndex::Backend {
  explicit Backend(sdsl::int_vector<>&& text) {
    sdsl::cache_config cache{false, "@"};
    absl::Cleanup cleanup = [&] {
      sdsl::util::delete_all_files(cache.file_map);
    };
    sdsl::append_zero_symbol(text);
    SDB_ENSURE(sdsl::store_to_cache(text, sdsl::conf::KEY_TEXT_INT, cache),
               "failed to cache FM-index text");

    std::array<uint16_t, 258> symbols{};
    for (const auto c : text) {
      symbols[c] = 1;
    }
    symbols[0] = 0;
    uint16_t alphabet = 0;
    for (size_t i = 1; i != symbols.size(); ++i) {
      if (symbols[i]) {
        symbols[i] = ++alphabet;
      }
    }
    if (alphabet <= std::numeric_limits<uint8_t>::max()) {
      sdsl::int_vector<8> bytes(text.size());
      for (size_t i = 0; i != text.size(); ++i) {
        bytes[i] = symbols[text[i]];
      }
      sdsl::util::clear(text);
      sdsl::int_vector<> suffixes(bytes.size(), 0,
                                  sdsl::bits::hi(bytes.size()) + 1);
      sdsl::algorithm::calculate_sa(
        reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(),
        suffixes);
      SDB_ENSURE(sdsl::store_to_cache(suffixes, sdsl::conf::KEY_SA, cache),
                 "failed to cache FM-index suffix array");
    }
    sdsl::util::clear(text);
    sdsl::construct(index, "", cache, 0);
  }

  size_t SerializedSize() const { return sdsl::size_in_bytes(index); }

  template<typename Bits, uint32_t Sampling>
  using Csa = sdsl::csa_wt<sdsl::wt_huff_int<Bits>, Sampling, UINT32_MAX,
                           sdsl::text_order_sa_sampling<>>;
#if defined(SDB_FM_BACKEND_rrr8)
  using Index = Csa<sdsl::rrr_vector<63>, 8>;
#elif defined(SDB_FM_BACKEND_rrr32)
  using Index = Csa<sdsl::rrr_vector<63>, 32>;
#elif defined(SDB_FM_BACKEND_plain8)
  using Index = Csa<sdsl::bit_vector, 8>;
#elif defined(SDB_FM_BACKEND_plain32)
  using Index = Csa<sdsl::bit_vector, 32>;
#endif
  Index index;
};

FmIndex::FmIndex(std::span<const std::string> terms,
                 std::span<const size_t> group_starts) {
  SDB_ASSERT(terms.empty() == group_starts.empty());
  SDB_ASSERT(group_starts.empty() || group_starts.front() == 0);
  SDB_ASSERT(group_starts.empty() || group_starts.back() < terms.size());
  SDB_ASSERT(
    absl::c_adjacent_find(group_starts, std::greater_equal<size_t>{}) ==
    group_starts.end());
  size_t length = terms.size();
  for (const auto& term : terms) {
    length += term.size();
  }
  sdsl::int_vector<> text(length, 0, 9);
  sdsl::sd_vector_builder starts(length + 1, group_starts.size() + 1);
  size_t at = 0;
  size_t group = 0;
  for (size_t i = 0; i != terms.size(); ++i) {
    const auto& term = terms[i];
    if (group != group_starts.size() && group_starts[group] == i) {
      starts.set(at);
      ++group;
    }
    for (const unsigned char c : term) {
      text[at++] = uint64_t{c} + 2;
    }
    text[at++] = 1;
  }
  starts.set(at);
  _starts = sdsl::sd_vector<>{starts};
  sdsl::util::init_support(_rank, &_starts);
  _backend = std::make_unique<Backend>(std::move(text));
  _bytes = _backend->SerializedSize() + sdsl::size_in_bytes(_starts) +
           sdsl::size_in_bytes(_rank);
  _live_bytes.fetch_add(_bytes, std::memory_order_relaxed);
}

FmIndex::~FmIndex() {
  _live_bytes.fetch_sub(_bytes, std::memory_order_relaxed);
}

std::optional<std::vector<uint64_t>> FmIndex::Find(
  std::string_view pattern, size_t max_occurrences) const {
  SDB_ASSERT(!pattern.empty());
  std::vector<uint64_t> matches;
  const auto& csa = _backend->index;
  uint64_t lower = 0;
  uint64_t upper = csa.size() - 1;
  uint64_t count = 0;
  for (size_t i = pattern.size(); i != 0; --i) {
    const uint64_t c = static_cast<unsigned char>(pattern[i - 1]) + uint64_t{2};
    count = sdsl::backward_search(csa, lower, upper, c, lower, upper);
    if (count == 0) {
      return matches;
    }
  }
  if (count > max_occurrences) {
    return std::nullopt;
  }
  matches.reserve(count);
  for (uint64_t i = lower; i <= upper; ++i) {
    matches.push_back(_rank(csa[i] + 1) - 1);
  }
  absl::c_sort(matches);
  const auto tail = std::ranges::unique(matches);
  matches.erase(tail.begin(), tail.end());
  return matches;
}

size_t FmIndex::SizeInBytes() const { return _bytes; }

}  // namespace irs
