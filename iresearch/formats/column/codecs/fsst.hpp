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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace irs::codecs {

inline constexpr size_t kFsstMaxExpansion = 8;

enum class FsstCodes : uint8_t {
  Byte,
  Twelve,
};

class FsstEncoder {
 public:
  FsstEncoder() noexcept : FsstEncoder{FsstCodes::Byte} {}
  explicit FsstEncoder(FsstCodes codes) noexcept : _codes{codes} {}

  void Encode(std::span<const std::string_view> strings, std::string& out,
              std::vector<uint32_t>& lengths);

  std::string_view SymbolTable() const noexcept {
    return {reinterpret_cast<const char*>(_table.data()), _table_size};
  }

  void Reset() noexcept;

 private:
  struct Free {
    FsstCodes codes;
    void operator()(void** encoder) const noexcept;
  };

  FsstCodes _codes;
  std::unique_ptr<void*, Free> _encoder{nullptr, Free{_codes}};
  std::vector<unsigned char> _table;
  size_t _table_size = 0;
  std::vector<size_t> _in_lengths;
  std::vector<unsigned char*> _in_ptrs;
  std::vector<size_t> _out_lengths;
  std::vector<unsigned char*> _out_ptrs;
};

class FsstDecoder {
 public:
  bool Import(std::string_view table) noexcept {
    return duckdb_fsst_import(
             &_decoder, reinterpret_cast<const unsigned char*>(table.data()),
             table.size()) == table.size();
  }

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

class Fsst12Decoder {
 public:
  bool Import(std::string_view table) noexcept {
    return duckdb_fsst12_import(
             &_decoder, reinterpret_cast<const unsigned char*>(table.data()),
             table.size()) == table.size();
  }

  size_t Decode(const char* in, size_t in_length, char* out,
                size_t capacity) const noexcept {
    return duckdb_fsst12_decompress(
      &_decoder, in_length, reinterpret_cast<const unsigned char*>(in),
      capacity, reinterpret_cast<unsigned char*>(out));
  }

 private:
  duckdb_fsst12_decoder_t _decoder{};
};

}  // namespace irs::codecs
