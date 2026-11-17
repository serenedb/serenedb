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

#include "iresearch/formats/column/codecs/text/string_window.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <span>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

constexpr uint64_t kTrialRawBytes = 512 * 1024;

}  // namespace

uint64_t StringWindow::Analyze(const Config& config) {
  if (const auto index = Find(config); index != _encoded.size()) {
    return At(index).Size();
  }
  const auto estimate =
    [&]<typename Codec>(
      std::optional<blob::SizeEstimator<Codec>>& slot) -> uint64_t {
    auto& rows = Rows(config.shape);
    if (!slot) {
      slot.emplace();
    }
    const auto whole = config.layout == FrameDictionary::Local
                         ? Bytes(rows)
                         : std::string_view{};
    return slot->EstimateSize(
      rows.profile, whole,
      Split(rows, blob::FrameShapeOf<Codec>(config.layout)), CodesSize(rows),
      config.level, config.layout, _begin);
  };
  SDB_ASSERT(config.leaf == ByteCodec::Lz4);
  if (EffectiveLevel<Lz4>(config.level) > 1) {
    return estimate(_size_estimators.lz4_hc);
  }
  if (RawBound(config.shape) <= kTrialRawBytes) {
    return At(TrialCompress(config)).Size();
  }
  return estimate(_size_estimators.lz4);
}

uint64_t StringWindow::RawBound(Shape shape) const noexcept {
  if (shape == Shape::Dictionary && _first.config.shape == Shape::Dictionary) {
    return std::min(_first.input, _first.entries * _first.max_len);
  }
  return _first.input;
}

uint64_t StringWindow::Entries() {
  for (const auto& seg : _encoded) {
    if (seg->config.shape == Shape::Dictionary) {
      return seg->entries;
    }
  }
  return Rows(Shape::Dictionary).profile.codes.size();
}

size_t StringWindow::TrialCompress(const Config& config) {
  const auto index = Find(config);
  if (index == _encoded.size()) {
    _encoded.emplace_back(
      _compressor.TrialCompress(config, _acc, _begin, _end));
  }
  return index;
}

size_t StringWindow::Find(const Config& config) const noexcept {
  const auto it = absl::c_find_if(
    _encoded, [&](const PooledSegment& seg) { return seg->config == config; });
  return static_cast<size_t>(it - _encoded.begin());
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
    auto& dedup = _compressor.Dedup();
    dedup.Begin(_acc.values.size());
    rows.locals.resize(_end - _begin);
    for (auto row = _begin; row < _end; ++row) {
      const auto code = _acc.codes[row];
      uint32_t local = 0;
      if (code != 0) {
        const auto mapped =
          dedup.Local(code, static_cast<uint32_t>(rows.codes.size()) + 1);
        local = mapped.local;
        if (mapped.fresh) {
          rows.codes.push_back(code);
          add(code);
        }
      }
      rows.locals[row - _begin] = local;
    }
    p.codes = rows.codes;
  }
  return rows;
}

uint64_t StringWindow::CodesSize(Measured& rows) {
  if (rows.profile.shape != Shape::Dictionary) {
    return 0;
  }
  if (!rows.coded) {
    rows.coded = true;
    auto tuning = _codes;
    CompressCodes(rows.locals, tuning, [&](CodecType, std::string_view bytes) {
      rows.codes_bytes = bytes.size();
    });
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

const blob::FrameSpans& StringWindow::Split(Measured& rows,
                                            blob::FrameShape frame) {
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
