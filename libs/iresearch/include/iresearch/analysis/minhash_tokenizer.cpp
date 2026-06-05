////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "minhash_tokenizer.hpp"

#include <absl/strings/escaping.h>

#include "basics/exceptions.h"
#include "basics/wyhash.h"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"

namespace irs::analysis {
namespace {

constexpr OffsAttr kEmptyOffset;

}  // namespace

Analyzer::ptr MinHashTokenizer::Make(Options opts) {
  if (opts.num_hashes == 0) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "minhash: num_hashes must be positive");
  }
  Analyzer::ptr sub;
  if (opts.analyzer) {
    sub = CreateAnalyzer(std::move(*opts.analyzer));
  }
  // If `analyzer` is absent the ctor falls back to StringTokenizer.
  return std::make_unique<MinHashTokenizer>(std::move(sub), opts.num_hashes);
}

MinHashTokenizer::MinHashTokenizer(analysis::Analyzer::ptr analyzer,
                                   uint32_t num_hashes)
  : _analyzer{std::move(analyzer)},
    _num_hashes{num_hashes},
    _minhash{_num_hashes} {
  if (!_analyzer) {
    // Fallback to default implementation
    _analyzer = std::make_unique<StringTokenizer>();
  }

  _term = irs::get<TermAttr>(*_analyzer);

  if (!_term) [[unlikely]] {
    _analyzer = std::make_unique<EmptyAnalyzer>();
  }

  _offset = irs::get<OffsAttr>(*_analyzer);

  std::get<TermAttr>(_attrs).value = {
    reinterpret_cast<const byte_type*>(_buf.data()), _buf.size()};
}

bool MinHashTokenizer::next() {
  if (_begin == _end) {
    return false;
  }

  const auto value = absl::little_endian::FromHost(*_begin);

  [[maybe_unused]] const size_t length = absl::Base64EscapeInternal(
    reinterpret_cast<const uint8_t*>(&value), sizeof value, _buf.data(),
    _buf.size(), absl::kBase64Chars, false);
  SDB_ASSERT(length == _buf.size());

  std::get<IncAttr>(_attrs).value = std::exchange(_next_inc.value, 0);
  ++_begin;

  return true;
}

bool MinHashTokenizer::reset(std::string_view data) {
  _begin = _end = {};
  if (_analyzer->reset(data)) {
    ComputeSignature();
    return true;
  }
  return false;
}

void MinHashTokenizer::ComputeSignature() {
  _minhash.Clear();
  _next_inc.value = 1;

  if (_analyzer->next()) {
    SDB_ASSERT(_term);

    const auto* offs = _offset ? _offset : &kEmptyOffset;
    auto& [start, end] = std::get<OffsAttr>(_attrs);
    start = offs->start;
    end = offs->end;

    do {
      const std::string_view value = ViewCast<char>(_term->value);
      const auto hash_value =
        sdb::basics::WyHash(value.data(), value.size(), 0xdeadbeef);

      _minhash.Insert(hash_value);
      end = offs->end;
    } while (_analyzer->next());

    _begin = std::begin(_minhash);
    _end = std::end(_minhash);
  }
}

}  // namespace irs::analysis
