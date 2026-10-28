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

#include <algorithm>
#include <numeric>
#include <sdsl/construct.hpp>
#include <sdsl/suffix_array_algorithm.hpp>
#include <utility>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

FmIndex::FmIndex(std::span<const std::string> terms) : _size{terms.size()} {
  size_t length = terms.size();
  for (const auto& term : terms) {
    length += term.size();
  }
  sdsl::int_vector<> text(length, 0, 9);
  sdsl::sd_vector_builder starts(length + 1, terms.size() + 1);
  size_t at = 0;
  for (const auto& term : terms) {
    starts.set(at);
    for (const unsigned char c : term) {
      text[at++] = uint64_t{c} + 2;
    }
    text[at++] = 1;
  }
  starts.set(at);
  _starts = sdsl::sd_vector<>{starts};
  sdsl::util::init_support(_rank, &_starts);
  sdsl::util::init_support(_select, &_starts);
  sdsl::construct_im(_csa, std::move(text));
  if (length != terms.size()) {
    std::vector<size_t> lengths;
    std::vector<unsigned char*> inputs;
    lengths.reserve(terms.size());
    inputs.reserve(terms.size());
    for (const auto& term : terms) {
      lengths.push_back(term.size());
      inputs.push_back(
        reinterpret_cast<unsigned char*>(const_cast<char*>(term.data())));
    }
    std::unique_ptr<duckdb_fsst_encoder_t, decltype(&duckdb_fsst_destroy)>
      encoder{
        duckdb_fsst_create(terms.size(), lengths.data(), inputs.data(), 0),
        &duckdb_fsst_destroy};
    _decoder = std::make_unique<duckdb_fsst_decoder_t>(
      duckdb_fsst_decoder(encoder.get()));
    std::vector<unsigned char> buffer(7 + 2 * (length - terms.size()));
    std::vector<size_t> output_lengths(terms.size());
    std::vector<unsigned char*> outputs(terms.size());
    SDB_ENSURE(duckdb_fsst_compress(encoder.get(), terms.size(), lengths.data(),
                                    inputs.data(), buffer.size(), buffer.data(),
                                    output_lengths.data(),
                                    outputs.data()) == terms.size(),
               "failed to compress FM-index keys");
    const auto compressed_size = static_cast<size_t>(
      outputs.back() + output_lengths.back() - buffer.data());
    _keys = std::vector<unsigned char>{buffer.begin(),
                                       buffer.begin() + compressed_size};
    _key_offsets = sdsl::int_vector<>(terms.size() + 1, 0,
                                      sdsl::bits::hi(compressed_size) + 1);
    for (size_t i = 0; i != terms.size(); ++i) {
      _key_offsets[i] = outputs[i] - buffer.data();
    }
    _key_offsets[terms.size()] = compressed_size;
  }
  _bytes = sdsl::size_in_bytes(_csa) + sdsl::size_in_bytes(_starts) +
           sdsl::size_in_bytes(_rank) + sdsl::size_in_bytes(_select) +
           sizeof(_size) + sdsl::size_in_bytes(_key_offsets) + _keys.size() +
           (_decoder ? sizeof(duckdb_fsst_decoder_t) : 0);
  _live_bytes.fetch_add(_bytes, std::memory_order_relaxed);
}

FmIndex::~FmIndex() {
  _live_bytes.fetch_sub(_bytes, std::memory_order_relaxed);
}

std::optional<std::vector<uint64_t>> FmIndex::Find(
  std::string_view pattern, size_t max_occurrences) const {
  std::vector<uint64_t> matches;
  if (pattern.empty()) {
    matches.resize(_size);
    std::iota(matches.begin(), matches.end(), uint64_t{0});
    return matches;
  }
  uint64_t lower = 0;
  uint64_t upper = _csa.size() - 1;
  uint64_t count = 0;
  for (size_t i = pattern.size(); i != 0; --i) {
    const uint64_t c = static_cast<unsigned char>(pattern[i - 1]) + uint64_t{2};
    count = sdsl::backward_search(_csa, lower, upper, c, lower, upper);
    if (count == 0) {
      return matches;
    }
  }
  if (count > max_occurrences) {
    return std::nullopt;
  }
  matches.reserve(count);
  for (uint64_t i = lower; i <= upper; ++i) {
    matches.push_back(_rank(_csa[i] + 1) - 1);
  }
  absl::c_sort(matches);
  const auto tail = std::ranges::unique(matches);
  matches.erase(tail.begin(), tail.end());
  return matches;
}

std::string FmIndex::Term(uint64_t id) const {
  const uint64_t begin = _select(id + 1);
  const uint64_t end = _select(id + 2) - 1;
  std::string term(end - begin, '\0');
  if (term.empty()) {
    return term;
  }
  const size_t offset = _key_offsets[id];
  const size_t length = _key_offsets[id + 1] - offset;
  SDB_ENSURE(duckdb_fsst_decompress(
               _decoder.get(), length, _keys.data() + offset, term.size(),
               reinterpret_cast<unsigned char*>(term.data())) == term.size(),
             "failed to decompress FM-index key");
  return term;
}

size_t FmIndex::SizeInBytes() const { return _bytes; }

}  // namespace irs
