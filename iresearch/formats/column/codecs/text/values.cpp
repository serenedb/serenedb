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

#include "iresearch/formats/column/codecs/text/values.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>

#include "iresearch/formats/column/codecs/text/string_layout.hpp"

namespace irs::codecs::text {
namespace {

using duckdb::string_t;

}  // namespace

Values::Values(ColumnDataCheckpointData& data) {
  auto& last_distinct = data.tuning.strings.last_distinct;
  const uint64_t rows = CountRows(data.chunks);
  codes.reserve(rows);
  const auto expected =
    last_distinct == 0 ? rows : std::min(rows, last_distinct);
  distinct.reserve(expected);
  _codes.reserve(expected);
  for (const auto& chunk : data.chunks) {
    Add(chunk.data);
  }
  last_distinct = distinct.size();
  data.raw_input = std::max<uint64_t>(raw, 1);
}

Values::ShapeRows& Values::TrialRows(Shape shape) {
  auto& rows = _rows[std::to_underlying(shape)];
  if (rows.done) {
    return rows;
  }
  rows.done = true;
  auto& p = rows.profile;
  p.shape = shape;
  p.rows = trial.rows;
  p.values = distinct;
  const auto add = [&](uint32_t code) {
    const auto size = static_cast<uint32_t>(distinct[code - 1]->GetSize());
    p.raw += size;
    p.max_len = std::max(p.max_len, size);
  };
  if (shape == Shape::Plain) {
    p.codes = std::span{codes}.subspan(0, trial.rows);
    for (const auto code : p.codes) {
      if (code != 0) {
        add(code);
      }
    }
    return rows;
  }
  _dedup.Begin(distinct.size());
  rows.locals.resize(trial.rows);
  for (uint64_t row = 0; row < trial.rows; ++row) {
    const auto code = codes[row];
    uint32_t local = 0;
    if (code != 0) {
      const auto mapped =
        _dedup.Local(code, static_cast<uint32_t>(rows.codes.size()) + 1);
      local = mapped.local;
      if (mapped.fresh) {
        rows.codes.push_back(code);
        add(code);
      }
    }
    rows.locals[row] = local;
  }
  p.codes = rows.codes;
  return rows;
}

uint64_t Values::CodesSize(ShapeRows& rows) {
  if (rows.profile.shape != Shape::Dictionary) {
    return 0;
  }
  if (!rows.coded) {
    rows.coded = true;
    double ratio = 0;
    CompressCodes(rows.locals, ratio, [&](CodecType, std::string_view bytes) {
      rows.codes_bytes = bytes.size();
    });
  }
  return rows.codes_bytes;
}

std::string_view Values::Bytes(ShapeRows& rows) {
  if (!rows.assembled) {
    rows.assembled = true;
    const auto& p = rows.profile;
    rows.bytes.reserve(p.raw);
    for (size_t i = 0; i < p.codes.size(); ++i) {
      const auto entry = EntryOf(p, i);
      rows.bytes.append(entry.GetData(), entry.GetSize());
    }
  }
  return rows.bytes;
}

const blob::FrameSpans& Values::Split(ShapeRows& rows, blob::FrameShape frame) {
  const auto it = absl::c_find_if(rows.spans, [&](const Spans& s) {
    return s.frame.frame == frame.frame && s.frame.first == frame.first;
  });
  if (it != rows.spans.end()) {
    return it->spans;
  }
  auto& split = rows.spans.emplace_back(frame).spans;
  split.Reset(frame);
  for (size_t i = 0; i < rows.profile.codes.size(); ++i) {
    split.Add(i, EntryOf(rows.profile, i).GetSize());
  }
  split.Close();
  return split;
}

void Values::Add(const duckdb::Vector& input) {
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
    code = *_codes.lazy_emplace(value, [&](const auto& construct) noexcept {
      construct(static_cast<uint32_t>(distinct.size() + 1));
    });
    if (code > distinct.size()) {
      distinct.push_back(&value);
    }
    _last = &value;
    _last_code = code;
  }
  raw += bytes;
}

}  // namespace irs::codecs::text
