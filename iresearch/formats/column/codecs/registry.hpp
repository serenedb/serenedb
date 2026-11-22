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

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <span>
#include <string_view>

#include "iresearch/formats/column/codecs/compression_function.hpp"

namespace irs::codecs {

const Codec* GetCodec(CodecType type, duckdb::PhysicalType physical);

struct Candidate {
  CodecType codec = CodecType::Uncompressed;
  double price = 1.0;
  bool always = false;
};

struct Candidates {
  std::span<const Candidate> flush;
  std::span<const Candidate> merge;
  Features (*features)(ColumnDataCheckpointData& data) = nullptr;
  double least_shift = 1.0;
};

const Candidates& CandidatesOf(duckdb::PhysicalType physical) noexcept;

bool Writes(CodecType type, duckdb::PhysicalType physical) noexcept;

inline constexpr auto kNoCodec =
  static_cast<CodecType>(magic_enum::enum_count<CodecType>());

CodecType FindCodec(std::string_view name);
CodecType FindCodec(duckdb::CompressionType type);

uint8_t MaxLevel(CodecType type) noexcept;

}  // namespace irs::codecs
