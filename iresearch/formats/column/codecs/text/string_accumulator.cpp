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

#include "iresearch/formats/column/codecs/text/string_accumulator.hpp"

#include <algorithm>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>

namespace irs::codecs {

using duckdb::string_t;

void StringAccumulator::Reserve(uint64_t rows, uint64_t distinct) {
  codes.reserve(rows);
  const auto expected = distinct == 0 ? rows : std::min(rows, distinct);
  values.reserve(expected);
  _distinct.reserve(expected);
}

void StringAccumulator::Add(const duckdb::Vector& input) {
  const auto base = codes.size();
  codes.resize(base + input.size());
  auto* out = codes.data() + base;
  uint64_t bytes = 0;
  for (const auto entry : input.Values<string_t>()) {
    auto& code = out[entry.GetIndex()];
    if (!entry.IsValid()) {
      code = 0;
      ++null_count;
      continue;
    }
    const auto& value = entry.GetValueUnsafe();
    bytes += value.GetSize();
    if (_last && value == *_last) {
      code = _last_code;
      continue;
    }
    code = *_distinct.lazy_emplace(value, [&](const auto& construct) noexcept {
      construct(static_cast<uint32_t>(values.size() + 1));
    });
    if (code > values.size()) {
      values.push_back(&value);
    }
    _last = &value;
    _last_code = code;
  }
  raw += bytes;
}

}  // namespace irs::codecs
