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
#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/parallel_for.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::compression {

enum class Codec : uint8_t {
  Lz4 = 0,
  Zstd = 1,
  Strip = 2,
};

struct Method {
  Codec codec = Codec::Zstd;
  int level = 0;
};

inline constexpr Method kLz4{.codec = Codec::Lz4};
inline constexpr Method kLz4Hc9{.codec = Codec::Lz4, .level = 9};
inline constexpr Method kZstd1{.codec = Codec::Zstd, .level = 1};
inline constexpr uint32_t kStripPadding = std::numeric_limits<uint32_t>::max();

constexpr Method Strip(uint32_t slots) noexcept {
  return {.codec = Codec::Strip, .level = static_cast<int>(slots)};
}

inline constexpr uint32_t kFrameBytes = uint32_t{4} << 20;
inline constexpr uint32_t kMaxFrameBytes = uint32_t{256} << 20;

struct Frames {
  Codec codec = Codec::Zstd;
  uint64_t raw_bytes = 0;
  uint32_t frame_bytes = kFrameBytes;
  absl::InlinedVector<uint32_t, 1> sizes;
  absl::InlinedVector<uint32_t, 1> raw_sizes;
};

struct Compressed {
  Frames frames;
  std::unique_ptr<byte_type[]> slots;
};

struct Packed {
  Frames frames;
  bytes_view stored;
};

struct FrameStart {
  uint64_t stored = 0;
  uint64_t raw = 0;
};

uint64_t StoredBytes(const Frames& frames) noexcept;

absl::InlinedVector<FrameStart, 1> FrameStarts(const Frames& frames);

using FrameVisitor = absl::FunctionRef<void(bytes_view)>;
using FrameSource = absl::FunctionRef<void(FrameVisitor)>;

Compressed Compress(Method method, bytes_view raw,
                    uint32_t frame_bytes = kFrameBytes,
                    ParallelFor parallel = SerialFor);

std::vector<Compressed> Compress(
  Method method, uint32_t frame_bytes, size_t blobs,
  absl::FunctionRef<void(size_t blob, FrameVisitor visit)> frames,
  ParallelFor parallel = SerialFor);

Frames Compress(Method method, bytes_view raw, uint32_t frame_bytes,
                std::span<byte_type> slots, ParallelFor parallel = SerialFor);

void ForEachStored(const Frames& frames, const byte_type* slots,
                   absl::FunctionRef<void(bytes_view)> visit);

void Decompress(const Frames& frames, bytes_view stored,
                std::span<byte_type> raw, ParallelFor parallel = SerialFor);

void Decompress(const Frames& frames, size_t first, size_t last,
                bytes_view stored, std::span<byte_type> raw,
                ParallelFor parallel = SerialFor);

std::unique_ptr<byte_type[]> Decompress(const Packed& packed,
                                        ParallelFor parallel = SerialFor);

void WriteFrames(duckdb::BinarySerializer& serializer, const Frames& frames);

Frames ReadFrames(duckdb::BinaryDeserializer& deserializer);

void WritePacked(duckdb::BinarySerializer& object, const Compressed& packed);

uint64_t PackedBytes(const Compressed& packed);

bool Pays(const Compressed& packed);

void KeepPlain(Compressed& packed, FrameSource frames);

bool IsPlain(const Compressed& packed) noexcept;

Packed ReadPacked(duckdb::BinaryDeserializer& object,
                  duckdb::MemoryStream& stream);

Packed ReadPacked(duckdb::BinaryDeserializer& object,
                  duckdb::InputFileReader& stream);

}  // namespace irs::compression
