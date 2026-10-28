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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <sdsl/csa_wt.hpp>
#include <sdsl/rrr_vector.hpp>
#include <sdsl/sd_vector.hpp>
#include <sdsl/wavelet_trees.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "third_party/duckdb/third_party/fsst/fsst.h"

namespace irs {

class FmIndex {
 public:
  explicit FmIndex(std::span<const std::string> terms);
  ~FmIndex();
  FmIndex(const FmIndex&) = delete;
  FmIndex& operator=(const FmIndex&) = delete;

  std::optional<std::vector<uint64_t>> Find(
    std::string_view pattern,
    size_t max_occurrences = std::numeric_limits<size_t>::max()) const;
  std::string Term(uint64_t id) const;
  size_t SizeInBytes() const;
  size_t Size() const noexcept { return _size; }
  static uint64_t LiveBytes() noexcept {
    return _live_bytes.load(std::memory_order_relaxed);
  }

 private:
  using Csa = sdsl::csa_wt<sdsl::wt_huff_int<sdsl::rrr_vector<63>>, 32,
                           UINT32_MAX, sdsl::text_order_sa_sampling<>>;

  Csa _csa;
  sdsl::sd_vector<> _starts;
  sdsl::sd_vector<>::rank_1_type _rank;
  sdsl::sd_vector<>::select_1_type _select;
  sdsl::int_vector<> _key_offsets;
  std::vector<unsigned char> _keys;
  std::unique_ptr<duckdb_fsst_decoder_t> _decoder;
  size_t _size;
  size_t _bytes{0};
  inline static std::atomic<uint64_t> _live_bytes{0};
};

}  // namespace irs
