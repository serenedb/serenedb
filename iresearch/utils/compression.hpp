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

#include <absl/container/inlined_vector.h>

#include <cstdint>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <memory>
#include <span>

#include "iresearch/types.hpp"
#include "iresearch/utils/parallel_for.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::compression {

enum class Codec : uint8_t {
  Lz4Hc = 0,
  Zstd = 1,
};

struct Method {
  Codec codec = Codec::Zstd;
  int level = 0;
};

inline constexpr Method kLz4Hc9{.codec = Codec::Lz4Hc, .level = 9};
inline constexpr Method kZstd1{.codec = Codec::Zstd, .level = 1};

inline constexpr uint32_t kFrameBytes = uint32_t{4} << 20;
inline constexpr uint32_t kMaxFrameBytes = uint32_t{256} << 20;

struct Frames {
  Codec codec = Codec::Zstd;
  uint64_t raw_bytes = 0;
  uint32_t frame_bytes = kFrameBytes;
  absl::InlinedVector<uint32_t, 1> sizes;
};

struct Compressed {
  Frames frames;
  std::unique_ptr<byte_type[]> bytes;
};

struct Packed {
  Frames frames;
  bytes_view stored;
};

uint64_t StoredBytes(const Frames& frames) noexcept;

Compressed Compress(Method method, bytes_view raw,
                    uint32_t frame_bytes = kFrameBytes,
                    ParallelFor parallel = SerialFor);

void Decompress(const Frames& frames, bytes_view stored,
                std::span<byte_type> raw, ParallelFor parallel = SerialFor);

std::unique_ptr<byte_type[]> Decompress(const Packed& packed,
                                        ParallelFor parallel = SerialFor);

void WritePacked(duckdb::BinarySerializer& object, const Compressed& packed);

Packed ReadPacked(duckdb::BinaryDeserializer& object,
                  duckdb::MemoryStream& stream);

}  // namespace irs::compression
