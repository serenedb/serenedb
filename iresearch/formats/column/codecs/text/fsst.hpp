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
#include <fsst12.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text::fsst {

inline constexpr size_t kMaxExpansion = 8;

struct Fsst8 {
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;

  using Encoding = duckdb_fsst_encoder_t;
  using Table = duckdb_fsst_decoder_t;
  using Exported = std::array<unsigned char, sizeof(Table)>;

  static Encoding* Create(size_t n, size_t* lengths,
                          const unsigned char** strings) {
    return duckdb_fsst_create(n, lengths, const_cast<unsigned char**>(strings),
                              0);
  }

  static void Destroy(Encoding* encoder) noexcept {
    duckdb_fsst_destroy(encoder);
  }

  static size_t Export(Encoding* encoder, Exported& table) {
    return duckdb_fsst_export(encoder, table.data());
  }

  static size_t Compress(Encoding* encoder, size_t n, size_t* lengths,
                         const unsigned char** strings, size_t capacity,
                         unsigned char* out, size_t* out_lengths,
                         unsigned char** out_strings) {
    return duckdb_fsst_compress(encoder, n, lengths,
                                const_cast<unsigned char**>(strings), capacity,
                                out, out_lengths, out_strings);
  }

  static size_t Import(Table& table, const unsigned char* in,
                       size_t size) noexcept {
    return duckdb_fsst_import(&table, in, size);
  }

  static size_t Decompress(const Table& table, const unsigned char* in,
                           size_t size, unsigned char* out,
                           size_t capacity) noexcept {
    return duckdb_fsst_decompress(const_cast<Table*>(&table), size, in,
                                  capacity, out);
  }
};

struct Fsst12 {
  static constexpr ByteCodec kCodec = ByteCodec::Fsst12;

  using Encoding = duckdb_fsst12_encoder_t;
  using Table = duckdb_fsst12_decoder_t;
  using Exported = std::vector<unsigned char>;

  static Encoding* Create(size_t n, size_t* lengths,
                          const unsigned char** strings) {
    return duckdb_fsst12_create(n, lengths, strings, 0);
  }

  static void Destroy(Encoding* encoder) noexcept {
    duckdb_fsst12_destroy(encoder);
  }

  static size_t Export(Encoding* encoder, Exported& table) {
    table.resize(sizeof(Table));
    return duckdb_fsst12_export(encoder, table.data());
  }

  static size_t Compress(Encoding* encoder, size_t n, size_t* lengths,
                         const unsigned char** strings, size_t capacity,
                         unsigned char* out, size_t* out_lengths,
                         unsigned char** out_strings) {
    return duckdb_fsst12_compress(encoder, n, lengths, strings, capacity, out,
                                  out_lengths, out_strings);
  }

  static size_t Import(Table& table, const unsigned char* in,
                       size_t size) noexcept {
    return duckdb_fsst12_import(&table, in, size);
  }

  static size_t Decompress(const Table& table, const unsigned char* in,
                           size_t size, unsigned char* out,
                           size_t capacity) noexcept {
    return duckdb_fsst12_decompress(&table, size, in, capacity, out);
  }
};

struct Encoded {
  size_t strings = 0;
  size_t bytes = 0;
};

template<typename Impl>
class Encoder {
 public:
  Encoded Encode(std::span<const duckdb::string_t> strings,
                 std::span<const uint32_t> skip, char* out, size_t capacity,
                 std::span<uint32_t> lengths) {
    SDB_ASSERT(skip.size() == strings.size());
    SDB_ASSERT(lengths.size() >= strings.size());
    const size_t n = strings.size();
    if (n == 0) {
      return {};
    }
    _in_lengths.resize(n);
    _in_ptrs.resize(n);
    for (size_t i = 0; i < n; ++i) {
      _in_lengths[i] = strings[i].GetSize() - skip[i];
      _in_ptrs[i] =
        reinterpret_cast<const unsigned char*>(strings[i].GetData() + skip[i]);
    }
    if (!_encoder) {
      _encoder.reset(Impl::Create(n, _in_lengths.data(), _in_ptrs.data()));
      SDB_ENSURE(_encoder, "fsst: cannot build a symbol table");
      _table_size = Impl::Export(_encoder.get(), _table);
    }
    _out_lengths.resize(n);
    _out_ptrs.resize(n);
    const size_t done =
      Impl::Compress(_encoder.get(), n, _in_lengths.data(), _in_ptrs.data(),
                     capacity, reinterpret_cast<unsigned char*>(out),
                     _out_lengths.data(), _out_ptrs.data());
    size_t used = 0;
    for (size_t i = 0; i < done; ++i) {
      lengths[i] = static_cast<uint32_t>(_out_lengths[i]);
      used += _out_lengths[i];
    }
    return {.strings = done, .bytes = used};
  }

  std::string_view SymbolTable() const noexcept {
    return {reinterpret_cast<const char*>(_table.data()), _table_size};
  }

 private:
  struct Free {
    void operator()(typename Impl::Encoding* encoder) const noexcept {
      Impl::Destroy(encoder);
    }
  };

  std::unique_ptr<typename Impl::Encoding, Free> _encoder;
  typename Impl::Exported _table;
  size_t _table_size = 0;
  std::vector<size_t> _in_lengths;
  std::vector<const unsigned char*> _in_ptrs;
  std::vector<size_t> _out_lengths;
  std::vector<unsigned char*> _out_ptrs;
};

template<typename Impl>
class Decoder {
 public:
  bool Import(std::string_view table) noexcept {
    return Impl::Import(_decoder,
                        reinterpret_cast<const unsigned char*>(table.data()),
                        table.size()) == table.size();
  }

  size_t Decode(const char* in, size_t in_length, char* out,
                size_t capacity) const noexcept {
    return Impl::Decompress(
      _decoder, reinterpret_cast<const unsigned char*>(in), in_length,
      reinterpret_cast<unsigned char*>(out), capacity);
  }

 private:
  typename Impl::Table _decoder{};
};

template<typename Impl>
class CompressionState : public SortedCompressionState {
 public:
  static constexpr ByteCodec kCodec = Impl::kCodec;

  CompressionState(const duckdb::LogicalType& type, DedupScratch& dedup,
                   EstimatorState& estimators, uint32_t target)
    : SortedCompressionState{kCodec, type, dedup, estimators, target} {}

  void FinalizeCompress();

 private:
  static constexpr uint64_t kEncodeExpansion = 2;
  static constexpr uint64_t kEncodeSlack = 8;

  Encoder<Impl> _codec;
  std::vector<uint32_t> _lcp_stream;
};

template<typename Impl>
void CompressionState<Impl>::FinalizeCompress() {
  if (_rows.SegmentShape() == Shape::Dictionary) {
    _rows.Sort(_sort);
  }
  const auto lengths = _rows.Lengths();
  const auto rows = _rows.View();
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  auto& data = _out->bytes;
  _lcp_stream.assign(first_entry, 0);
  _frames.clear();
  uint32_t max_lcp = 0;
  uint64_t frame_raw = 0;
  uint64_t frame_first = 0;
  uint64_t suffixes = 0;
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    const auto size = entry.GetSize();
    if (frame_raw != 0 && frame_raw + size > kFsstFrameRawBytes) {
      _frames.emplace_back(static_cast<uint32_t>(frame_first),
                           static_cast<uint32_t>(frame_raw), 0, 0);
      frame_raw = 0;
      frame_first = i;
    }
    const uint32_t lcp = (i - frame_first) % kChainRestart == 0
                           ? 0
                           : CommonPrefix(entries[i - 1], entry);
    _lcp_stream.emplace_back(lcp);
    max_lcp = std::max(max_lcp, lcp);
    frame_raw += size;
    suffixes += size - lcp;
  }
  if (!entries.empty()) {
    _frames.emplace_back(static_cast<uint32_t>(frame_first),
                         static_cast<uint32_t>(frame_raw), 0, 0);
  }
  const auto skip = std::span{_lcp_stream}.subspan(first_entry);
  size_t done = 0;
  const auto encode = [&](uint64_t capacity) {
    auto* out = data.Extend(capacity);
    const auto encoded =
      _codec.Encode(entries.subspan(done), skip.subspan(done), out, capacity,
                    lengths.subspan(done));
    data.Shrink(capacity - encoded.bytes);
    for (auto i = done; i < done + encoded.strings; ++i) {
      suffixes -= entries[i].GetSize() - skip[i];
    }
    done += encoded.strings;
  };
  encode(suffixes + kEncodeSlack);
  if (done != entries.size()) {
    encode(kEncodeExpansion * suffixes + kEncodeSlack);
    SDB_ENSURE(done == entries.size(), "fsst: compression ran out of space");
  }
  size_t off = 0;
  uint32_t max_enc = 0;
  for (size_t f = 0; f < _frames.size(); ++f) {
    auto& frame = _frames[f];
    const size_t end =
      f + 1 < _frames.size() ? _frames[f + 1].first_entry : entries.size();
    frame.comp_off = static_cast<uint32_t>(off);
    for (size_t e = frame.first_entry; e < end; ++e) {
      off += lengths[e];
      max_enc = std::max(max_enc, lengths[e]);
    }
    frame.comp_len = static_cast<uint32_t>(off - frame.comp_off);
    frame.first_entry += first_entry;
  }
  _lcp_stream.resize(
    duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount(rows)),
    0);
  const auto symtab = _codec.SymbolTable();
  RecordRatio(_history[static_cast<size_t>(_rows.SegmentShape())], rows.raw,
              data.Size() + symtab.size(), _target);
  _rows.Write(
    {
      .codec = kCodec,
      .frames = _frames,
      .max_length = max_enc,
      .lcps = _lcp_stream,
      .max_lcp = max_lcp,
      .symtab = symtab,
    },
    *_out);
}

}  // namespace irs::codecs::text::fsst
