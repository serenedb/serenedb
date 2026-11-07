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
#include <duckdb/common/vector/vector_iterator.hpp>
#include <type_traits>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

template<typename State>
void RetrainState(std::optional<State>& state) noexcept {
  if (state) {
    state->Retrain();
  }
}

template<typename State>
IRS_NO_INLINE void CutRows(State& state, const Config& config,
                           const StringAccumulator& acc, uint64_t begin,
                           uint32_t target, Segment& seg) {
  const std::span<const uint32_t> codes = acc.codes;
  const std::span<const duckdb::string_t* const> values = acc.values;
  state.Begin(config, begin, values.size(), seg);
  for (auto row = begin; row < codes.size();) {
    state.AddCode(codes[row++], values);
    if (state.Full(target)) {
      break;
    }
  }
  state.Finish();
}

template<typename State>
IRS_NO_INLINE void SealRows(State& state, const Config& config,
                            std::span<const WriteChunk> chunks, uint32_t target,
                            Segment& seg, SegmentSink sink) {
  const auto seal = [&] {
    state.Finish();
    sink(seg.config.choice, std::move(*seg.stats), seg.rows, seg.bytes.View());
  };
  uint64_t row = 0;
  uint64_t first = 0;
  state.Begin(config, first, 0, seg);
  for (const auto& chunk : chunks) {
    for (const auto entry : chunk.data.Values<duckdb::string_t>()) {
      state.AddValue(entry.IsValid() ? &entry.GetValueUnsafe() : nullptr);
      ++row;
      if (state.Full(target)) {
        seal();
        first = row;
        state.Begin(config, first, 0, seg);
      }
    }
  }
  if (row != first) {
    seal();
  }
}

template<typename State>
IRS_NO_INLINE void EncodeRows(State& state, const Config& config,
                              const StringAccumulator& acc, uint64_t begin,
                              uint64_t end, Segment& seg) {
  const std::span<const uint32_t> codes = acc.codes;
  const std::span<const duckdb::string_t* const> values = acc.values;
  state.Begin(config, begin, values.size(), seg);
  for (auto row = begin; row < end; ++row) {
    state.AddCode(codes[row], values);
  }
  state.Finish();
}

}  // namespace

template<typename F>
auto SegmentEncoder::With(const Config& config, F&& f) {
  switch (config.choice.leaf) {
    case ByteCodec::Lz4:
      if (EffectiveLevel<Lz4>(config.level) > 1) {
        return f(Get(_states.lz4_hc));
      }
      return f(Get(_states.lz4));
    case ByteCodec::Zstd:
      return f(Get(_states.zstd));
    case ByteCodec::Zxc:
      return f(Get(_states.zxc));
    case ByteCodec::Fsst:
      return f(Get(_states.fsst));
    case ByteCodec::FsstPlus:
      return f(Get(_states.fsst_plus));
    case ByteCodec::OnPair:
      return f(Get(_states.onpair));
    case ByteCodec::Fsst12:
      return f(Get(_states.fsst12));
  }
  SDB_UNREACHABLE();
}

template<typename State>
State& SegmentEncoder::Get(std::optional<State>& slot) {
  if (!slot) {
    if constexpr (Sorted(State::kCodec)) {
      slot.emplace(_type, _dedup, _tuning, _params.segment_target);
    } else {
      slot.emplace(_type, _dedup, _tuning, _dictionaries);
    }
  }
  return *slot;
}

Segment* SegmentEncoder::Cut(const Config& config, const StringAccumulator& acc,
                             uint64_t begin) {
  auto* seg = Acquire();
  With(config, [&](auto& state) {
    CutRows(state, config, acc, begin, _params.segment_target, *seg);
  });
  return seg;
}

void SegmentEncoder::SealChunks(const Config& config,
                                std::span<const WriteChunk> chunks,
                                SegmentSink sink) {
  auto* seg = Acquire();
  With(config, [&](auto& state) {
    SealRows(state, config, chunks, _params.segment_target, *seg, sink);
  });
  Release(seg);
}

Segment* SegmentEncoder::Encode(const Config& config,
                                const StringAccumulator& acc, uint64_t begin,
                                uint64_t end) {
  auto* seg = Acquire();
  With(config,
       [&](auto& state) { EncodeRows(state, config, acc, begin, end, *seg); });
  return seg;
}

uint64_t SegmentEncoder::Price(const Config& config,
                               const StringAccumulator& acc, uint64_t begin,
                               uint64_t end) {
  const auto& profile = Measure(acc, config.choice.shape, begin, end);
  return With(config, [&](auto& state) -> uint64_t {
    if constexpr (Sorted(std::remove_reference_t<decltype(state)>::kCodec)) {
      SDB_UNREACHABLE();
    } else {
      return state.Price(profile, config.level, config.layout, begin);
    }
  });
}

void SegmentEncoder::Retrain() noexcept {
  RetrainState(_states.fsst);
  RetrainState(_states.fsst_plus);
  RetrainState(_states.fsst12);
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
  p.values = acc.values;
  const auto add = [&](uint32_t code) {
    const auto size = static_cast<uint32_t>(acc.values[code - 1]->GetSize());
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
  _dedup.Begin(acc.values.size());
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
    return _pool.emplace_back(std::make_unique<Segment>(_buffers)).get();
  }
  auto* seg = _free.back();
  _free.pop_back();
  return seg;
}

}  // namespace irs::codecs::text
