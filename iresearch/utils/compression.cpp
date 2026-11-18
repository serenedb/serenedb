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

#include "iresearch/utils/compression.hpp"

#include <absl/algorithm/container.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <magic_enum/magic_enum.hpp>
#include <string_view>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::compression {
namespace {

struct Field {
  duckdb::field_id_t id = 0;
  const char* tag = nullptr;
};

constexpr Field kFramesCodec{.id = 0, .tag = "codec"};
constexpr Field kFramesRawBytes{.id = 1, .tag = "raw_bytes"};
constexpr Field kFramesFrameBytes{.id = 2, .tag = "frame_bytes"};
constexpr Field kFramesSizes{.id = 3, .tag = "sizes"};

constexpr Field kPackedFrames{.id = 0, .tag = "frames"};
constexpr Field kPackedBytes{.id = 1, .tag = "bytes"};

[[noreturn]] void Corrupted(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("compressed data: ", what));
}

size_t FrameCount(uint64_t raw_bytes, uint32_t frame_bytes) noexcept {
  return static_cast<size_t>(raw_bytes / frame_bytes +
                             (raw_bytes % frame_bytes != 0));
}

size_t Encode(Method method, bytes_view in, byte_type* out) noexcept {
  const auto capacity = in.size() - 1;
  size_t size = 0;
  switch (method.codec) {
    case Codec::Lz4Hc: {
      const auto written = LZ4_compress_HC(
        reinterpret_cast<const char*>(in.data()), reinterpret_cast<char*>(out),
        static_cast<int>(in.size()), static_cast<int>(capacity), method.level);
      size = written > 0 ? static_cast<size_t>(written) : 0;
      break;
    }
    case Codec::Zstd: {
      const auto written =
        ZSTD_compress(out, capacity, in.data(), in.size(), method.level);
      size = ZSTD_isError(written) ? 0 : written;
      break;
    }
  }
  if (size == 0) {
    std::memcpy(out, in.data(), in.size());
    return in.size();
  }
  return size;
}

void Decode(Codec codec, bytes_view in, std::span<byte_type> out) {
  if (in.size() == out.size()) {
    std::memcpy(out.data(), in.data(), in.size());
    return;
  }
  bool decoded = false;
  switch (codec) {
    case Codec::Lz4Hc:
      decoded = LZ4_decompress_safe(reinterpret_cast<const char*>(in.data()),
                                    reinterpret_cast<char*>(out.data()),
                                    static_cast<int>(in.size()),
                                    static_cast<int>(out.size())) ==
                static_cast<int>(out.size());
      break;
    case Codec::Zstd: {
      const auto size =
        ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
      decoded = !ZSTD_isError(size) && size == out.size();
      break;
    }
  }
  if (!decoded) {
    Corrupted("a frame does not decode to its size");
  }
}

uint32_t FrameRawBytes(const Frames& frames, size_t frame) noexcept {
  const auto begin = uint64_t{frame} * frames.frame_bytes;
  SDB_ASSERT(begin < frames.raw_bytes);
  return static_cast<uint32_t>(
    std::min<uint64_t>(frames.frame_bytes, frames.raw_bytes - begin));
}

void WriteFrames(duckdb::BinarySerializer& serializer, const Frames& frames) {
  serializer.WriteProperty<uint8_t>(kFramesCodec.id, kFramesCodec.tag,
                                    static_cast<uint8_t>(frames.codec));
  serializer.WriteProperty<uint64_t>(kFramesRawBytes.id, kFramesRawBytes.tag,
                                     frames.raw_bytes);
  serializer.WriteProperty<uint32_t>(kFramesFrameBytes.id,
                                     kFramesFrameBytes.tag, frames.frame_bytes);
  serializer.WriteList(
    kFramesSizes.id, kFramesSizes.tag, frames.sizes.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t frame) {
      list.WriteElement<uint32_t>(frames.sizes[frame]);
    });
}

Frames ReadFrames(duckdb::BinaryDeserializer& deserializer) {
  Frames frames;
  const auto codec = magic_enum::enum_cast<Codec>(
    deserializer.ReadProperty<uint8_t>(kFramesCodec.id, kFramesCodec.tag));
  if (!codec) {
    Corrupted("an unknown codec");
  }
  frames.codec = *codec;
  frames.raw_bytes = deserializer.ReadProperty<uint64_t>(kFramesRawBytes.id,
                                                         kFramesRawBytes.tag);
  frames.frame_bytes = deserializer.ReadProperty<uint32_t>(
    kFramesFrameBytes.id, kFramesFrameBytes.tag);
  if (frames.frame_bytes == 0 || frames.frame_bytes > kMaxFrameBytes) {
    Corrupted("a frame size out of range");
  }
  deserializer.ReadList(
    kFramesSizes.id, kFramesSizes.tag,
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      frames.sizes.push_back(list.ReadElement<uint32_t>());
    });
  if (frames.sizes.size() != FrameCount(frames.raw_bytes, frames.frame_bytes)) {
    Corrupted("a frame count that does not match its size");
  }
  for (size_t frame = 0; frame != frames.sizes.size(); ++frame) {
    const auto stored = frames.sizes[frame];
    if (stored == 0 || stored > FrameRawBytes(frames, frame)) {
      Corrupted("a frame larger than its raw bytes");
    }
  }
  return frames;
}

}  // namespace

uint64_t StoredBytes(const Frames& frames) noexcept {
  return absl::c_accumulate(frames.sizes, uint64_t{0});
}

Compressed Compress(Method method, bytes_view raw, uint32_t frame_bytes,
                    ParallelFor parallel) {
  SDB_ASSERT(frame_bytes != 0 && frame_bytes <= kMaxFrameBytes);
  Compressed out{
    .frames = {.codec = method.codec,
               .raw_bytes = raw.size(),
               .frame_bytes = frame_bytes},
    .bytes = std::make_unique_for_overwrite<byte_type[]>(raw.size()),
  };
  auto& sizes = out.frames.sizes;
  sizes.resize(FrameCount(raw.size(), frame_bytes));
  auto* bytes = out.bytes.get();
  parallel(sizes.size(), [&](size_t frame) {
    const auto begin = uint64_t{frame} * frame_bytes;
    sizes[frame] = static_cast<uint32_t>(
      Encode(method, raw.substr(begin, frame_bytes), bytes + begin));
  });
  uint64_t used = 0;
  for (size_t frame = 0; frame != sizes.size(); ++frame) {
    std::memmove(bytes + used, bytes + uint64_t{frame} * frame_bytes,
                 sizes[frame]);
    used += sizes[frame];
  }
  return out;
}

void Decompress(const Frames& frames, bytes_view stored,
                std::span<byte_type> raw, ParallelFor parallel) {
  if (raw.size() != frames.raw_bytes || stored.size() != StoredBytes(frames)) {
    Corrupted("its sizes do not match its frames");
  }
  absl::InlinedVector<uint64_t, 1> offsets(frames.sizes.size());
  uint64_t offset = 0;
  for (size_t frame = 0; frame != offsets.size(); ++frame) {
    offsets[frame] = offset;
    offset += frames.sizes[frame];
  }
  parallel(offsets.size(), [&](size_t frame) {
    Decode(frames.codec, stored.substr(offsets[frame], frames.sizes[frame]),
           raw.subspan(uint64_t{frame} * frames.frame_bytes,
                       FrameRawBytes(frames, frame)));
  });
}

std::unique_ptr<byte_type[]> Decompress(const Packed& packed,
                                        ParallelFor parallel) {
  const auto size = packed.frames.raw_bytes;
  auto raw = std::make_unique_for_overwrite<byte_type[]>(size);
  Decompress(packed.frames, packed.stored, {raw.get(), size}, parallel);
  return raw;
}

void WritePacked(duckdb::BinarySerializer& object, const Compressed& packed) {
  object.WriteObject(kPackedFrames.id, kPackedFrames.tag,
                     [&](duckdb::BinarySerializer& frames) {
                       WriteFrames(frames, packed.frames);
                     });
  object.WriteProperty(kPackedBytes.id, kPackedBytes.tag, packed.bytes.get(),
                       StoredBytes(packed.frames));
}

Packed ReadPacked(duckdb::BinaryDeserializer& object,
                  duckdb::MemoryStream& stream) {
  Packed packed;
  object.ReadObject(kPackedFrames.id, kPackedFrames.tag,
                    [&](duckdb::BinaryDeserializer& frames) {
                      packed.frames = ReadFrames(frames);
                    });
  object.OnPropertyBegin(kPackedBytes.id, kPackedBytes.tag);
  const auto size = object.ReadUnsignedInt64();
  const auto at = stream.GetPosition();
  if (size != StoredBytes(packed.frames) || size > stream.GetCapacity() - at) {
    Corrupted("its bytes do not match its frames");
  }
  packed.stored = {stream.GetData() + at, size};
  stream.SetPosition(at + size);
  object.OnPropertyEnd();
  return packed;
}

}  // namespace irs::compression
