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

#pragma once

#include <cstdint>
#include <duckdb/common/typedefs.hpp>
#include <optional>

#include "iresearch/formats/column/codecs/numeric/delta.hpp"
#include "iresearch/formats/column/codecs/numeric/plain.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs {

class CodesReader {
 public:
  CodesReader(duckdb::const_data_ptr_t base, uint64_t size, uint64_t rows,
              CodecType codec) {
    const auto frames = ParseFrames(base, size, codec);
    SDB_ENSURE(frames.row_count == rows, "col codec: corrupted row codes");
    _shape = ShapeOf(codec);
    switch (_shape) {
      case Shape::Plain:
        _plain.emplace(base, frames, codec);
        break;
      case Shape::Delta:
        _delta.emplace(base, frames, codec);
        break;
      default:
        _runs.emplace(base, frames, codec);
        break;
    }
  }

  template<typename F>
  IRS_FORCE_INLINE decltype(auto) Visit(F&& f) {
    switch (_shape) {
      case Shape::Plain:
        return f(*_plain);
      case Shape::Delta:
        return f(*_delta);
      default:
        return f(*_runs);
    }
  }

  void Read(uint64_t row, uint64_t count, uint32_t* out) {
    Visit([&](auto& codes) { codes.Read(row, count, out); });
  }

  IRS_FORCE_INLINE uint32_t Fetch(uint64_t row, uint64_t next) {
    return Visit([&](auto& codes)
                   IRS_FORCE_INLINE { return codes.Fetch(row, next); });
  }

 private:
  Shape _shape = Shape::Plain;
  std::optional<PlainReader<uint32_t>> _plain;
  std::optional<DeltaReader<uint32_t>> _delta;
  std::optional<RunsReader<uint32_t>> _runs;
};

}  // namespace irs::codecs
