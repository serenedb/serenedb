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

#include "iresearch/formats/column/codecs/string_window.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>

#include "iresearch/formats/column/codecs/string_compression_function.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

constexpr uint64_t kTrialRawBytes = 512 * 1024;

}  // namespace

uint64_t StringWindow::Analyze(const Config& config) {
  if (const auto* seg = Encoded(config)) {
    return seg->Size();
  }
  const auto price =
    [&]<typename Codec>(std::optional<BlobPricer<Codec>>& slot) -> uint64_t {
    auto& rows = Rows(config.choice.shape);
    if (!slot) {
      slot.emplace();
    }
    const auto whole =
      config.layout == FrameLayout::Local ? Bytes(rows) : std::string_view{};
    return slot->Price(rows.profile, whole,
                       Split(rows, FrameShapeOf<Codec>(config.layout)),
                       CodesSize(rows), config.level, config.layout, _begin);
  };
  SDB_ASSERT(config.choice.leaf == ByteCodec::Lz4);
  if (EffectiveLevel<Lz4>(config.level) > 1) {
    return price(_pricers.lz4_hc);
  }
  if (RawBound(config.choice.shape) <= kTrialRawBytes) {
    return TrialCompress(config).Size();
  }
  return price(_pricers.lz4);
}

uint64_t StringWindow::RawBound(Shape shape) const noexcept {
  if (shape == Shape::Dedup && _first.config.choice.shape == Shape::Dedup) {
    return std::min(_first.input, _first.entries * _first.max_len);
  }
  return _first.input;
}

uint64_t StringWindow::Entries() {
  for (const auto* seg : _encoded) {
    if (seg->config.choice.shape == Shape::Dedup) {
      return seg->entries;
    }
  }
  return Rows(Shape::Dedup).profile.codes.size();
}

Segment& StringWindow::TrialCompress(const Config& config) {
  if (auto* seg = Encoded(config)) {
    return *seg;
  }
  auto& seg =
    *_owned.emplace_back(GetStringCompressionFunction(config).TrialCompress(
      _compressor, config, _acc, _begin, _end));
  _encoded.emplace_back(&seg);
  return seg;
}

Segment* StringWindow::Encoded(const Config& config) const noexcept {
  const auto it = absl::c_find_if(
    _encoded, [&](const Segment* seg) { return seg->config == config; });
  return it == _encoded.end() ? nullptr : *it;
}

PooledSegment StringWindow::Take(const Segment& seg) {
  const auto it = absl::c_find_if(
    _owned, [&](const PooledSegment& owned) { return owned.get() == &seg; });
  SDB_ASSERT(it != _owned.end());
  return std::move(*it);
}

StringWindow::Measured& StringWindow::Rows(Shape shape) {
  auto& rows = _measured[static_cast<size_t>(shape)];
  if (rows.done) {
    return rows;
  }
  rows.done = true;
  auto& p = rows.profile;
  p.shape = shape;
  p.rows = _end - _begin;
  p.values = _acc.values;
  const auto add = [&](uint32_t code) {
    const auto size = static_cast<uint32_t>(_acc.values[code - 1]->GetSize());
    p.raw += size;
    p.max_len = std::max(p.max_len, size);
  };
  if (shape == Shape::Plain) {
    p.codes = std::span{_acc.codes}.subspan(_begin, _end - _begin);
    for (const auto code : p.codes) {
      if (code != 0) {
        add(code);
      }
    }
  } else {
    _dedup.Begin(_acc.values.size());
    rows.locals.resize(_end - _begin);
    uint32_t last = 0;
    for (auto row = _begin; row < _end; ++row) {
      const auto code = _acc.codes[row];
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
      rows.locals[row - _begin] = local;
      if (row == _begin || local != last) {
        ++p.runs;
      }
      last = local;
    }
    p.codes = rows.codes;
  }
  return rows;
}

uint64_t StringWindow::CodesSize(Measured& rows) {
  if (rows.profile.shape != Shape::Dedup) {
    return 0;
  }
  if (!rows.coded) {
    rows.coded = true;
    auto tuning = _codes;
    rows.codes_bytes =
      EncodeCodes(rows.locals, rows.codes.size(), rows.profile.runs, tuning)
        .size();
  }
  return rows.codes_bytes;
}

std::string_view StringWindow::Bytes(Measured& rows) {
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

const FrameSpans& StringWindow::Split(Measured& rows, FrameShape frame) {
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

}  // namespace irs::codecs::text
