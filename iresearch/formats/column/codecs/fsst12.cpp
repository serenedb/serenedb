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

#include "iresearch/formats/column/codecs/fsst12.hpp"

#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/fsst12_leaf.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

const duckdb::CompressionFunction& Fsst12Function() {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::Fsst12Leaf>>(
      duckdb::CompressionType::COMPRESSION_COL_FSST12);
  return kFunction;
}

const duckdb::CompressionFunction& DictFsst12Function() {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::Fsst12Leaf>>(
      duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12);
  return kFunction;
}

void Fsst12Encoder::Reset() noexcept {
  _encoder.reset();
  _table_size = 0;
}

Fsst12Encoded Fsst12Encoder::Encode(std::span<const duckdb::string_t> strings,
                                    std::span<const uint32_t> skip, char* out,
                                    size_t capacity,
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
    _encoder.reset(
      duckdb_fsst12_create(n, _in_lengths.data(), _in_ptrs.data(), 0));
    SDB_ENSURE(_encoder, "fsst12: cannot build a symbol table");
    _table.resize(sizeof(duckdb_fsst12_decoder_t));
    _table_size = duckdb_fsst12_export(_encoder.get(), _table.data());
  }
  _out_lengths.resize(n);
  _out_ptrs.resize(n);
  const size_t done = duckdb_fsst12_compress(
    _encoder.get(), n, _in_lengths.data(), _in_ptrs.data(), capacity,
    reinterpret_cast<unsigned char*>(out), _out_lengths.data(),
    _out_ptrs.data());
  size_t used = 0;
  for (size_t i = 0; i < done; ++i) {
    lengths[i] = static_cast<uint32_t>(_out_lengths[i]);
    used += _out_lengths[i];
  }
  return {.strings = done, .bytes = used};
}

bool Fsst12Decoder::Import(std::string_view table) noexcept {
  return duckdb_fsst12_import(
           &_decoder, reinterpret_cast<const unsigned char*>(table.data()),
           table.size()) == table.size();
}

}  // namespace irs::codecs
