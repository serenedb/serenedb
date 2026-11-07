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

#include "iresearch/formats/column/codecs/segment_encoder.hpp"

#include <algorithm>
#include <type_traits>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

template<typename Encoder>
void RetrainEncoder(std::optional<Encoder>& encoder) noexcept {
  if (encoder) {
    encoder->Retrain();
  }
}

}  // namespace

template<typename F>
auto SegmentEncoder::With(const Config& config, F&& f) {
  switch (config.choice.leaf) {
    case ByteCodec::Lz4:
      if (EffectiveLevel<Lz4>(config.level) > 1) {
        return f(Get(_encoders.lz4_hc));
      }
      return f(Get(_encoders.lz4));
    case ByteCodec::Zstd:
      return f(Get(_encoders.zstd));
    case ByteCodec::Zxc:
      return f(Get(_encoders.zxc));
    case ByteCodec::Fsst:
      return f(Get(_encoders.fsst));
    case ByteCodec::FsstPlus:
      return f(Get(_encoders.fsst_plus));
    case ByteCodec::OnPair:
      return f(Get(_encoders.onpair));
    case ByteCodec::Fsst12:
      return f(Get(_encoders.fsst12));
  }
  SDB_UNREACHABLE();
}

template<typename Encoder>
Encoder& SegmentEncoder::Get(std::optional<Encoder>& slot) {
  if (!slot) {
    if constexpr (Sorted(Encoder::kCodec)) {
      slot.emplace(_tuning, _params.segment_target);
    } else {
      slot.emplace(_tuning, _dictionaries);
    }
  }
  return *slot;
}

template<typename Cursor>
Segment* SegmentEncoder::CutWith(const Config& config, Cursor& cursor,
                                 uint64_t begin, uint64_t end,
                                 size_t distinct) {
  auto* seg = Acquire();
  With(config, [&](auto& enc) {
    _builder.Begin(enc, config, begin, distinct);
    _builder.Cut(enc, cursor, end, _params.segment_target);
    _builder.Finish(enc, *seg);
  });
  return seg;
}

Segment* SegmentEncoder::Cut(const Config& config, const StringAccumulator& acc,
                             uint64_t begin) {
  auto cursor = acc.At(begin);
  return CutWith(config, cursor, begin, acc.Rows(), acc.entries.size());
}

Segment* SegmentEncoder::Cut(const Config& config, ChunkCursor& cursor,
                             uint64_t begin, uint64_t end) {
  return CutWith(config, cursor, begin, end, 0);
}

Segment* SegmentEncoder::Encode(const Config& config,
                                const StringAccumulator& acc, uint64_t begin,
                                uint64_t end) {
  auto* seg = Acquire();
  auto cursor = acc.At(begin);
  With(config, [&](auto& enc) {
    _builder.Begin(enc, config, begin, acc.entries.size());
    _builder.AddUntil(enc, cursor, end);
    _builder.Finish(enc, *seg);
  });
  return seg;
}

uint64_t SegmentEncoder::Price(const Config& config,
                               const StringAccumulator& acc, uint64_t begin,
                               uint64_t end) {
  const auto& profile = Measure(acc, config.choice.shape, begin, end);
  return With(config, [&](auto& enc) -> uint64_t {
    if constexpr (Sorted(std::remove_reference_t<decltype(enc)>::kCodec)) {
      SDB_UNREACHABLE();
    } else {
      return enc.Price(profile, config.level, config.layout, begin);
    }
  });
}

void SegmentEncoder::Retrain() noexcept {
  RetrainEncoder(_encoders.fsst);
  RetrainEncoder(_encoders.fsst_plus);
  RetrainEncoder(_encoders.onpair);
  RetrainEncoder(_encoders.fsst12);
}

const Profile& SegmentEncoder::Measure(const StringAccumulator& acc,
                                       Shape shape, uint64_t begin,
                                       uint64_t end) {
  auto& p = _profile;
  if (_measured && p.shape == shape && _profile_begin == begin &&
      _profile_end == end) {
    return p;
  }
  _measured = true;
  _profile_begin = begin;
  _profile_end = end;
  p.shape = shape;
  p.rows = end - begin;
  p.runs = 0;
  p.raw = 0;
  p.max_len = 0;
  p.values = acc.entries;
  const auto add = [&](uint32_t code) {
    const auto size = static_cast<uint32_t>(acc.entries[code - 1].GetSize());
    p.raw += size;
    p.max_len = std::max(p.max_len, size);
  };
  if (shape == Shape::Plain) {
    p.codes = std::span{acc.codes}.subspan(begin, end - begin);
    for (const auto code : p.codes) {
      if (code != 0) {
        add(code);
      }
    }
    return p;
  }
  _profile_codes.clear();
  _dedup.Begin(acc.entries.size());
  uint32_t last = 0;
  for (auto row = begin; row < end; ++row) {
    const auto code = acc.codes[row];
    uint32_t local = 0;
    if (code != 0) {
      const auto mapped =
        _dedup.Local(code, static_cast<uint32_t>(_profile_codes.size()) + 1);
      local = mapped.local;
      if (mapped.fresh) {
        _profile_codes.push_back(code);
        add(code);
      }
    }
    if (row == begin || local != last) {
      ++p.runs;
    }
    last = local;
  }
  p.codes = _profile_codes;
  return p;
}

Segment* SegmentEncoder::Acquire() {
  if (_free.empty()) {
    return _pool.emplace_back(std::make_unique<Segment>()).get();
  }
  auto* seg = _free.back();
  _free.pop_back();
  return seg;
}

}  // namespace irs::codecs::text
