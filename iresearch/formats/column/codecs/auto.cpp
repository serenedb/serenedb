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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <array>
#include <duckdb/common/types.hpp>
#include <magic_enum/magic_enum.hpp>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

constexpr CodecType kOrder[] = {
  CodecType::Uncompressed, CodecType::Rle,       CodecType::Bitpacking,
  CodecType::Alp,          CodecType::AlpRd,     CodecType::Roaring,
  CodecType::Ffor,         CodecType::DeltaFfor, CodecType::RleFfor,
  CodecType::DictFfor,
};

std::span<const CompressionFunction* const> FunctionsOf(PhysicalType physical) {
  static const auto kFunctions = [] {
    std::array<std::vector<const CompressionFunction*>, 256> functions;
    for (size_t physical = 0; physical < functions.size(); ++physical) {
      auto& list = functions[physical];
      const auto add = [&](CodecType type) {
        const auto* codec = GetCodec(type, static_cast<PhysicalType>(physical));
        if (codec && codec->compression &&
            !absl::c_linear_search(list, codec->compression)) {
          list.push_back(codec->compression);
        }
      };
      for (const auto type : kOrder) {
        add(type);
      }
      for (const auto type : magic_enum::enum_values<CodecType>()) {
        add(type);
      }
    }
    return functions;
  }();
  return kFunctions[std::to_underlying(physical)];
}

class Contest {
 public:
  Contest(std::span<const CompressionFunction* const> functions,
          ColumnDataCheckpointData& data)
    : _functions{functions},
      _data{data},
      _reuse{data.tuning->reuse},
      _named{data.only != CodecType::Auto} {}

  void Run() {
    const bool due = _reuse.Due();
    if (due || _data.params.tier == WriteTier::Merge) {
      Measure();
    }
    if (due || !Reuse()) {
      Pick();
    }
  }

 private:
  void Measure() {
    if (_named || _measured) {
      return;
    }
    _measured = true;
    for (const auto* function : _functions) {
      if (!function->exact) {
        continue;
      }
      if (const auto estimate = function->analyze(_data, _bar.Score(), 0);
          estimate.Score() < _bar.Score()) {
        _bar = estimate;
        _exact = function;
      }
    }
  }

  bool Reuse() {
    SDB_ASSERT(_reuse.pick);
    const auto* held = _reuse.pick->function;
    if (!absl::c_linear_search(_functions, held)) {
      return false;
    }
    if (_exact && held->exact) {
      if (held != _exact) {
        return false;
      }
      Stream(*held);
      return true;
    }
    const auto encoded = held->encode(_data, nullptr);
    if (encoded.streams) {
      Stream(*held);
      return true;
    }
    _drift =
      encoded.Failed() ||
      (encoded.input != 0 &&
       _reuse.Drifted(static_cast<double>(encoded.bytes),
                      static_cast<double>(encoded.input), held->growth_only));
    if (encoded.Failed() ||
        !(_exact ? encoded.Ratio() * encoded.cost < _bar.Score() : !_drift)) {
      if (!_exact && !held->exact) {
        _reuse.clock.Restart();
      }
      return false;
    }
    if (_drift) {
      _reuse.clock.Restart();
    }
    held->flush(_data);
    return true;
  }

  void Pick() {
    Measure();
    auto best = _bar;
    const auto* winner = _exact;
    if (_named) {
      winner = _functions.front();
      best = {.codec = _data.only};
    } else {
      uint8_t rounds = 1;
      for (uint8_t round = 0; round < rounds; ++round) {
        for (const auto* function : _functions) {
          if (function->exact || round >= function->rounds) {
            continue;
          }
          rounds = std::max(rounds, function->rounds);
          if (const auto estimate =
                function->analyze(_data, best.Score(), round);
              estimate.Score() < best.Score()) {
            best = estimate;
            winner = function;
          }
        }
      }
    }
    Encoded encoded;
    if (winner) {
      encoded = winner->encode(_data, &best);
      if (_exact && winner != _exact && !encoded.streams &&
          (encoded.Failed() ||
           encoded.Ratio() * encoded.cost >= _bar.Score())) {
        winner = _exact;
        best = _bar;
        encoded = winner->encode(_data, &best);
      }
    }
    if (!winner || encoded.Failed()) {
      const auto* uncompressed =
        GetCodec(CodecType::Uncompressed, _data.type.InternalType());
      SDB_ENSURE(uncompressed && uncompressed->compression,
                 "column writer: no codec accepted the row group for ",
                 _data.type.ToString());
      winner = uncompressed->compression;
      best = {.codec = CodecType::Uncompressed};
      encoded = {.streams = true};
    }
    const auto written = Flush(*winner);
    const HeldCodec pick{winner, best.codec};
    const bool same =
      _reuse.pick && !encoded.changed &&
      (*_reuse.pick == pick || (winner->exact && _reuse.pick->function->exact));
    _reuse.Settle(
      pick, _reuse.calibrated && same && (winner->growth_only || !_drift),
      encoded.streams ? written : static_cast<double>(encoded.bytes),
      static_cast<double>(encoded.streams ? _data.input : encoded.input));
  }

  void Stream(const CompressionFunction& function) {
    if (_reuse.Drifted(Flush(function), static_cast<double>(_data.input),
                       function.growth_only)) {
      _reuse.clock.Restart();
    }
  }

  double Flush(const CompressionFunction& function) {
    const auto written = _data.written;
    function.flush(_data);
    return static_cast<double>(_data.written - written);
  }

  std::span<const CompressionFunction* const> _functions;
  ColumnDataCheckpointData& _data;
  ReusePolicy<HeldCodec>& _reuse;
  bool _named;
  bool _measured = false;
  bool _drift = false;
  Estimate _bar;
  const CompressionFunction* _exact = nullptr;
};

}  // namespace

void Compress(std::span<const CompressionFunction* const> functions,
              ColumnDataCheckpointData& data) {
  SDB_ASSERT(data.tuning && data.input != 0);
  Contest{functions, data}.Run();
}

void Compress(CodecType method, ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  ColumnTuning untuned;
  const absl::Cleanup restore = [&data, tuning = data.tuning] {
    data.tuning = tuning;
  };
  if (!data.tuning) {
    data.tuning = &untuned;
  }
  data.rows = CountRows(data.chunks);
  data.input = std::max<uint64_t>(data.rows, 1);
  if (method != CodecType::Auto) {
    const auto* codec = GetCodec(method, physical);
    SDB_ASSERT(codec && codec->compression);
    data.only = method;
    const CompressionFunction* const functions[] = {codec->compression};
    Compress(functions, data);
    return;
  }
  if (!CompressConstant(data)) {
    Compress(FunctionsOf(physical), data);
  } else if (data.params.tier == WriteTier::Merge) {
    data.tuning->reuse.clock.Restart();
  }
}

bool Writes(CodecType type, PhysicalType physical,
            const CodecSet& disabled) noexcept {
  if (type == CodecType::Auto || disabled.test(std::to_underlying(type))) {
    return false;
  }
  const auto* codec = GetCodec(type, physical);
  return codec && codec->compression;
}

}  // namespace irs::codecs
