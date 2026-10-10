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

#include "iresearch/formats/column/codecs/fsst_codec.hpp"

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

FsstEncoder::~FsstEncoder() { Reset(); }

void FsstEncoder::Reset() noexcept {
  if (_encoder) {
    duckdb_fsst_destroy(_encoder);
    _encoder = nullptr;
  }
  _table_size = 0;
}

void FsstEncoder::Encode(std::span<const std::string_view> strings,
                         std::string& out, std::vector<uint32_t>& lengths) {
  const size_t n = strings.size();
  if (n == 0) {
    out.clear();
    lengths.clear();
    return;
  }
  _in_lengths.resize(n);
  _in_ptrs.resize(n);
  size_t total = 0;
  for (size_t i = 0; i < n; ++i) {
    _in_lengths[i] = strings[i].size();
    _in_ptrs[i] =
      reinterpret_cast<unsigned char*>(const_cast<char*>(strings[i].data()));
    total += strings[i].size();
  }
  if (!_encoder) {
    _encoder = duckdb_fsst_create(n, _in_lengths.data(), _in_ptrs.data(), 0);
    SDB_ENSURE(_encoder, "fsst: cannot build a symbol table");
    _table_size = duckdb_fsst_export(_encoder, _table);
  }

  _out_lengths.resize(n);
  _out_ptrs.resize(n);
  size_t done = 0;
  out.resize_and_overwrite(2 * total + 8, [&](char* buf, size_t size) {
    done =
      duckdb_fsst_compress(_encoder, n, _in_lengths.data(), _in_ptrs.data(),
                           size, reinterpret_cast<unsigned char*>(buf),
                           _out_lengths.data(), _out_ptrs.data());
    return size;
  });
  SDB_ENSURE(done == n, "fsst: compression ran out of space");
  size_t used = 0;
  lengths.resize(n);
  for (size_t i = 0; i < n; ++i) {
    lengths[i] = static_cast<uint32_t>(_out_lengths[i]);
    used += _out_lengths[i];
  }
  out.resize(used);
}

bool FsstDecoder::Import(std::string_view table) noexcept {
  return duckdb_fsst_import(
           &_decoder, reinterpret_cast<const unsigned char*>(table.data()),
           table.size()) == table.size();
}

}  // namespace irs::codecs
