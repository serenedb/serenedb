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

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

template<typename V>
class FforBlocks {
 public:
  template<typename Less>
  void Plan(std::span<const V> v, Less less) {
    const size_t blocks =
      (v.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.resize(blocks);
    _bits.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(v.size(), begin + numeric::kBlockValues);
      V lo = v[begin];
      V hi = lo;
      for (size_t i = begin + 1; i < end; ++i) {
        if (less(v[i], lo)) {
          lo = v[i];
        }
        if (less(hi, v[i])) {
          hi = v[i];
        }
      }
      _base[b] = lo;
      _bits[b] = static_cast<uint8_t>(std::bit_width(static_cast<V>(hi - lo)));
    }
  }

  uint64_t Bytes(size_t first, size_t count) const noexcept {
    uint64_t bytes = count * kFforBlockMetaBytes;
    for (size_t b = first; b < first + count; ++b) {
      bytes += numeric::PackedBytes(_bits[b]);
    }
    return bytes;
  }

  uint64_t Bytes() const noexcept { return Bytes(0, _bits.size()); }

  void PlanDeltas(std::span<const V> deltas, std::span<const V> values) {
    const size_t blocks =
      (deltas.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.assign(blocks, 0);
    _bits.resize(blocks);
    _first.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(deltas.size(), begin + numeric::kBlockValues);
      V hi = 0;
      for (size_t i = begin + 1; i < end; ++i) {
        hi = std::max(hi, deltas[i]);
      }
      _first[b] = values[begin];
      _bits[b] = static_cast<uint8_t>(std::bit_width(hi));
    }
  }

  void Append(std::span<const V> v, size_t first, size_t count,
              std::string& out) {
    _scratch.resize(numeric::kBlockValues);
    const size_t at = out.size();
    out.resize_and_overwrite(at + Bytes(first, count), [&](char* buf,
                                                           size_t size) {
      auto* meta = reinterpret_cast<uint8_t*>(buf + at);
      auto* packed = meta + count * kFforBlockMetaBytes;
      for (size_t b = 0; b < count; ++b) {
        const size_t g = first + b;
        const uint64_t stored_base = _first.empty() ? _base[g] : _first[g];
        std::memset(meta, 0, kFforBlockMetaBytes);
        duckdb::Store(stored_base, meta);
        meta[sizeof(stored_base)] = _bits[g];
        const size_t begin = g * numeric::kBlockValues;
        const size_t n = std::min(numeric::kBlockValues, v.size() - begin);
        const V* in = v.data() + begin;
        if (n < numeric::kBlockValues) {
          std::memcpy(_scratch.data(), in, n * sizeof(V));
          std::fill_n(_scratch.data() + n, numeric::kBlockValues - n, _base[g]);
          in = _scratch.data();
        }
        numeric::Pack<V>(_bits[g], in,
                         reinterpret_cast<numeric::LaneWord<V>*>(packed),
                         _base[g]);
        meta += kFforBlockMetaBytes;
        packed += numeric::PackedBytes(_bits[g]);
      }
      return size;
    });
  }

 private:
  std::vector<V> _base;
  std::vector<V> _first;
  std::vector<uint8_t> _bits;
  ScratchVector<V> _scratch;
};

}  // namespace irs::codecs
