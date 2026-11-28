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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text::fsst {

inline constexpr size_t kMaxExpansion = 8;
inline constexpr size_t kPrefixSlack = 31;
inline constexpr uint64_t kEncodeExpansion = 2;
inline constexpr uint64_t kEncodeSlack = 8;

struct Fsst8 {
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;
  static constexpr bool kSharedTable = false;

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

  using Imported = Table;

  static bool Import(Imported& imported, std::string_view table) noexcept {
    return duckdb_fsst_import(
             &imported, reinterpret_cast<const unsigned char*>(table.data()),
             table.size()) == table.size();
  }

  static bool Import(Imported& imported, std::string_view table,
                     const ColumnSegment&, bool) noexcept {
    return Import(imported, table);
  }

  static size_t Decompress(const Imported& imported, const unsigned char* in,
                           size_t size, unsigned char* out,
                           size_t capacity) noexcept {
    return duckdb_fsst_decompress(const_cast<Table*>(&imported), size, in,
                                  capacity, out);
  }

  static size_t DecompressPrefix(const Imported& imported,
                                 const unsigned char* in, size_t size,
                                 unsigned char* out, size_t bytes) noexcept {
    return duckdb_fsst_decompress_prefix(const_cast<Table*>(&imported), size,
                                         in, bytes, out);
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

template<typename Impl, ByteCodec kLeaf = Impl::kCodec>
struct SharedEncoder final : State {
  static constexpr StateId kId = FsstEncoderState(kLeaf);

  explicit SharedEncoder(ColumnDataCheckpointData&) {}

  Encoder<Impl> encoder;
};

template<typename Impl>
class Decoder {
 public:
  bool Import(std::string_view table) noexcept {
    return Impl::Import(_imported, table);
  }

  bool Import(std::string_view table, const ColumnSegment& segment,
              bool shared) {
    return Impl::Import(_imported, table, segment, shared);
  }

  size_t Decode(const char* in, size_t in_length, char* out,
                size_t capacity) const noexcept {
    return Impl::Decompress(
      _imported, reinterpret_cast<const unsigned char*>(in), in_length,
      reinterpret_cast<unsigned char*>(out), capacity);
  }

  size_t DecodePrefix(const char* in, size_t in_length, char* out,
                      size_t bytes) const noexcept {
    return Impl::DecompressPrefix(
      _imported, reinterpret_cast<const unsigned char*>(in), in_length,
      reinterpret_cast<unsigned char*>(out), bytes);
  }

 private:
  typename Impl::Imported _imported{};
};

}  // namespace irs::codecs::text::fsst
