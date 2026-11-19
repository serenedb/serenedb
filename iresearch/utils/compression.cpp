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
#include <absl/functional/function_ref.h>
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
constexpr Field kFramesRawSizes{.id = 4, .tag = "raw_sizes"};

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
  if (!frames.raw_sizes.empty()) {
    return frames.raw_sizes[frame];
  }
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
  serializer.WriteOptionalList(
    kFramesRawSizes.id, kFramesRawSizes.tag, frames.raw_sizes.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t frame) {
      list.WriteElement<uint32_t>(frames.raw_sizes[frame]);
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
  deserializer.ReadOptionalList(
    kFramesRawSizes.id, kFramesRawSizes.tag,
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      frames.raw_sizes.push_back(list.ReadElement<uint32_t>());
    });
  if (frames.raw_sizes.empty()) {
    if (frames.sizes.size() !=
        FrameCount(frames.raw_bytes, frames.frame_bytes)) {
      Corrupted("a frame count that does not match its size");
    }
  } else {
    if (frames.raw_sizes.size() != frames.sizes.size()) {
      Corrupted("a frame count that does not match its size");
    }
    uint64_t raw_bytes = 0;
    for (const auto raw_size : frames.raw_sizes) {
      if (raw_size == 0 || raw_size > frames.frame_bytes) {
        Corrupted("a frame size out of range");
      }
      raw_bytes += raw_size;
    }
    if (raw_bytes != frames.raw_bytes) {
      Corrupted("frame sizes that do not add up to its size");
    }
  }
  for (size_t frame = 0; frame != frames.sizes.size(); ++frame) {
    const auto stored = frames.sizes[frame];
    if (stored == 0 || stored > FrameRawBytes(frames, frame)) {
      Corrupted("a frame larger than its raw bytes");
    }
  }
  return frames;
}

using FrameSource =
  absl::FunctionRef<void(absl::FunctionRef<void(bytes_view)>)>;

Compressed CompressFrames(Method method, uint32_t frame_bytes,
                          FrameSource for_each_frame, ParallelFor parallel) {
  SDB_ASSERT(frame_bytes != 0 && frame_bytes <= kMaxFrameBytes);
  absl::InlinedVector<bytes_view, 1> raws;
  absl::InlinedVector<uint64_t, 1> offsets;
  uint64_t raw_bytes = 0;
  bool full = true;
  for_each_frame([&](bytes_view raw) {
    SDB_ASSERT(!raw.empty() && raw.size() <= frame_bytes);
    full = full && (raws.empty() || raws.back().size() == frame_bytes);
    raws.push_back(raw);
    offsets.push_back(raw_bytes);
    raw_bytes += raw.size();
  });
  Compressed out{
    .frames = {.codec = method.codec,
               .raw_bytes = raw_bytes,
               .frame_bytes = frame_bytes},
    .bytes = std::make_unique_for_overwrite<byte_type[]>(raw_bytes),
  };
  auto& frames = out.frames;
  frames.sizes.resize(raws.size());
  if (!full) {
    for (const auto raw : raws) {
      frames.raw_sizes.push_back(static_cast<uint32_t>(raw.size()));
    }
  }
  auto* bytes = out.bytes.get();
  parallel(raws.size(), [&](size_t frame) {
    frames.sizes[frame] = static_cast<uint32_t>(
      Encode(method, raws[frame], bytes + offsets[frame]));
  });
  uint64_t used = 0;
  for (size_t frame = 0; frame != raws.size(); ++frame) {
    std::memmove(bytes + used, bytes + offsets[frame], frames.sizes[frame]);
    used += frames.sizes[frame];
  }
  return out;
}

}  // namespace

FrameWriter::FrameWriter(uint32_t frame_bytes) : _frame_bytes{frame_bytes} {
  SDB_ASSERT(frame_bytes != 0 && frame_bytes <= kMaxFrameBytes);
}

uint64_t FrameWriter::Room() const noexcept {
  return _pieces.empty() ? 0 : _pieces.back().end - _raw_bytes;
}

void FrameWriter::AddPiece(uint64_t size) {
  const auto capacity = (size + _frame_bytes - 1) / _frame_bytes * _frame_bytes;
  _pieces.push_back({
    .data = std::make_unique_for_overwrite<byte_type[]>(capacity),
    .begin = _raw_bytes,
    .end = _raw_bytes + capacity,
  });
}

void FrameWriter::WriteData(duckdb::const_data_ptr_t data, duckdb::idx_t size) {
  if (const auto head = std::min<uint64_t>(size, Room())) {
    std::memcpy(Alloc(head), data, head);
    data += head;
    size -= head;
  }
  if (size != 0) {
    std::memcpy(Alloc(size), data, size);
  }
}

duckdb::data_ptr_t FrameWriter::Alloc(duckdb::idx_t size) {
  if (size == 0) {
    return nullptr;
  }
  if (Room() < size) {
    AddPiece(size);
  }
  auto& piece = _pieces.back();
  auto* at = piece.data.get() + (_raw_bytes - piece.begin);
  _raw_bytes += size;
  return at;
}

void FrameWriter::ForEachFrame(
  absl::FunctionRef<void(bytes_view)> visit) const {
  for (size_t p = 0; p != _pieces.size(); ++p) {
    const auto& piece = _pieces[p];
    const auto used =
      (p + 1 < _pieces.size() ? _pieces[p + 1].begin : _raw_bytes) -
      piece.begin;
    for (uint64_t at = 0; at < used; at += _frame_bytes) {
      visit({piece.data.get() + at,
             static_cast<size_t>(std::min<uint64_t>(_frame_bytes, used - at))});
    }
  }
}

uint64_t StoredBytes(const Frames& frames) noexcept {
  return absl::c_accumulate(frames.sizes, uint64_t{0});
}

Compressed Compress(Method method, bytes_view raw, uint32_t frame_bytes,
                    ParallelFor parallel) {
  return CompressFrames(
    method, frame_bytes,
    [&](absl::FunctionRef<void(bytes_view)> visit) {
      for (size_t at = 0; at < raw.size(); at += frame_bytes) {
        visit(raw.substr(at, frame_bytes));
      }
    },
    parallel);
}

Compressed Compress(Method method, const FrameWriter& raw,
                    ParallelFor parallel) {
  return CompressFrames(
    method, raw.FrameBytes(),
    [&](absl::FunctionRef<void(bytes_view)> visit) { raw.ForEachFrame(visit); },
    parallel);
}

void Decompress(const Frames& frames, bytes_view stored,
                std::span<byte_type> raw, ParallelFor parallel) {
  if (raw.size() != frames.raw_bytes || stored.size() != StoredBytes(frames)) {
    Corrupted("its sizes do not match its frames");
  }
  struct Offsets {
    uint64_t stored = 0;
    uint64_t raw = 0;
  };
  absl::InlinedVector<Offsets, 1> offsets(frames.sizes.size());
  Offsets at;
  for (size_t frame = 0; frame != offsets.size(); ++frame) {
    offsets[frame] = at;
    at.stored += frames.sizes[frame];
    at.raw += FrameRawBytes(frames, frame);
  }
  parallel(offsets.size(), [&](size_t frame) {
    Decode(frames.codec,
           stored.substr(offsets[frame].stored, frames.sizes[frame]),
           raw.subspan(offsets[frame].raw, FrameRawBytes(frames, frame)));
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
