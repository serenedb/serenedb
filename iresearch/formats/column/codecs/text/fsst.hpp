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

#include <fsst.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"

namespace irs::codecs::text::fsst {

inline constexpr size_t kMaxExpansion = 8;

struct Free {
  void operator()(duckdb_fsst_encoder_t* encoder) const noexcept {
    duckdb_fsst_destroy(encoder);
  }
};

struct Encoded {
  size_t strings = 0;
  size_t bytes = 0;
};

class Encoder {
 public:
  Encoded Encode(std::span<const duckdb::string_t> strings,
                 std::span<const uint32_t> skip, char* out, size_t capacity,
                 std::span<uint32_t> lengths);

  std::string_view SymbolTable() const noexcept {
    return {reinterpret_cast<const char*>(_table), _table_size};
  }

 private:
  std::unique_ptr<duckdb_fsst_encoder_t, Free> _encoder;
  unsigned char _table[sizeof(duckdb_fsst_decoder_t)];
  size_t _table_size = 0;
  std::vector<size_t> _in_lengths;
  std::vector<unsigned char*> _in_ptrs;
  std::vector<size_t> _out_lengths;
  std::vector<unsigned char*> _out_ptrs;
};

class Decoder {
 public:
  bool Import(std::string_view table) noexcept;

  size_t Decode(const char* in, size_t in_length, char* out,
                size_t capacity) const noexcept {
    return duckdb_fsst_decompress(
      const_cast<duckdb_fsst_decoder_t*>(&_decoder), in_length,
      reinterpret_cast<const unsigned char*>(in), capacity,
      reinterpret_cast<unsigned char*>(out));
  }

 private:
  duckdb_fsst_decoder_t _decoder{};
};

class CompressionState {
 public:
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;

  CompressionState(const duckdb::LogicalType& type, DedupScratch& dedup,
                   EstimatorState& estimators, uint32_t target)
    : _rows{type, dedup, estimators.codes},
      _history{estimators.history[static_cast<size_t>(kCodec)]},
      _target{target} {}

  void InitCompression(const Config& config, uint64_t first_row,
                       size_t distinct, Segment& out) {
    _rows.Begin(config, first_row, distinct);
    _out = &out;
    out.bytes.Truncate(0);
  }

  IRS_FORCE_INLINE void Compress(const duckdb::string_t* value) {
    _rows.AddValue(value, [](const duckdb::string_t&, uint32_t) {});
  }

  IRS_FORCE_INLINE void Compress(
    uint32_t code, std::span<const duckdb::string_t* const> values) {
    _rows.AddCode(code, values, [](const duckdb::string_t&, uint32_t) {});
  }

  IRS_FORCE_INLINE bool Full(uint32_t target) {
    return _rows.EstimateDue(target) &&
           _rows.Estimate() +
               EstimateEntries(
                 _rows.View(),
                 _history[static_cast<size_t>(_rows.SegmentShape())]) >=
             target;
  }

  void FinalizeCompress();

 private:
  SegmentRows _rows;
  SortScratch _sort;
  RatioHistory* _history;
  uint32_t _target;
  Encoder _codec;
  Segment* _out = nullptr;
  std::vector<FrameMeta> _frames;
  std::vector<uint32_t> _lcp_stream;
};

}  // namespace irs::codecs::text::fsst
