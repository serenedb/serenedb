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
#include <absl/base/internal/endian.h>
#include <absl/functional/function_ref.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <magic_enum/magic_enum.hpp>
#include <string_view>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_packing.hpp"
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

constexpr size_t kStripHeaderBytes = sizeof(uint32_t) + sizeof(uint8_t);
constexpr uint32_t kStripBlock = packed::kBlockSize32;

uint64_t StripBlockBytes(uint64_t values, uint32_t width) noexcept {
  return (values + kStripBlock - 1) / kStripBlock * width * sizeof(uint32_t);
}

class StripWriter {
 public:
  StripWriter(byte_type* at, uint32_t width) noexcept
    : _at{at}, _width{width} {}

  void Append(const byte_type* values, uint32_t count) noexcept {
    while (count != 0) {
      const auto take = std::min(count, kStripBlock - _size);
      std::memcpy(_values.data() + _size, values, take * sizeof(uint32_t));
      values += take * sizeof(uint32_t);
      count -= take;
      _size += take;
      if (_size == kStripBlock) {
        Flush();
      }
    }
  }

  byte_type* Finish() noexcept {
    if (_size != 0) {
      std::fill(_values.begin() + _size, _values.end(), 0);
      Flush();
    }
    return _at;
  }

 private:
  void Flush() noexcept {
    std::array<uint32_t, kStripBlock> words{};
    packed::PackBlock(_values.data(), words.data(), _width);
    for (uint32_t word = 0; word != _width; ++word) {
      absl::little_endian::Store32(_at, words[word]);
      _at += sizeof(uint32_t);
    }
    _size = 0;
  }

  byte_type* _at;
  uint32_t _width;
  uint32_t _size = 0;
  std::array<uint32_t, kStripBlock> _values{};
};

class StripReader {
 public:
  StripReader(bytes_view in, uint32_t width) noexcept
    : _in{in}, _width{width} {}

  bool Copy(byte_type* out, uint32_t count) noexcept {
    while (count != 0) {
      if (_used == kStripBlock && !Refill()) {
        return false;
      }
      const auto take = std::min(count, kStripBlock - _used);
      std::memcpy(out, _values.data() + _used, take * sizeof(uint32_t));
      out += take * sizeof(uint32_t);
      count -= take;
      _used += take;
    }
    return true;
  }

  bool AtEnd() const noexcept { return _at == _in.size(); }

 private:
  bool Refill() noexcept {
    const auto bytes = size_t{_width} * sizeof(uint32_t);
    if (_in.size() - _at < bytes) {
      return false;
    }
    std::array<uint32_t, kStripBlock> words{};
    for (uint32_t word = 0; word != _width; ++word) {
      words[word] =
        absl::little_endian::Load32(_in.data() + _at + word * sizeof(uint32_t));
    }
    _at += bytes;
    packed::UnpackBlock(words.data(), _values.data(), _width);
    _used = 0;
    return true;
  }

  bytes_view _in;
  uint32_t _width;
  size_t _at = 0;
  uint32_t _used = kStripBlock;
  std::array<uint32_t, kStripBlock> _values{};
};

uint32_t LoadId(const byte_type* at) noexcept {
  uint32_t value = 0;
  std::memcpy(&value, at, sizeof(value));
  return value;
}

size_t EncodeStrip(uint32_t slots, bytes_view in, byte_type* out,
                   size_t capacity) noexcept {
  const auto list_bytes = uint64_t{slots} * sizeof(uint32_t);
  if (slots == 0 || in.size() % list_bytes != 0) {
    return 0;
  }
  const auto lists = in.size() / list_bytes;
  const auto count_width = static_cast<uint32_t>(std::bit_width(slots));
  const auto counts_bytes = StripBlockBytes(lists, count_width);
  if (kStripHeaderBytes + counts_bytes > capacity) {
    return 0;
  }
  const auto* ids = in.data();
  StripWriter counts{out + kStripHeaderBytes, count_width};
  uint32_t max_id = 0;
  uint64_t kept = 0;
  for (size_t list = 0; list != lists; ++list) {
    const auto* slot = ids + list * list_bytes;
    uint32_t count = 0;
    for (; count != slots; ++count) {
      const auto id = LoadId(slot + uint64_t{count} * sizeof(uint32_t));
      if (id == kStripPadding) {
        break;
      }
      max_id = std::max(max_id, id);
    }
    for (auto rest = count; rest != slots; ++rest) {
      if (LoadId(slot + uint64_t{rest} * sizeof(uint32_t)) != kStripPadding) {
        return 0;
      }
    }
    counts.Append(reinterpret_cast<const byte_type*>(&count), 1);
    kept += count;
  }
  counts.Finish();
  const auto width =
    std::max<uint32_t>(1, static_cast<uint32_t>(std::bit_width(max_id)));
  const auto size =
    kStripHeaderBytes + counts_bytes + StripBlockBytes(kept, width);
  if (size > capacity) {
    return 0;
  }
  absl::little_endian::Store32(out, slots);
  out[sizeof(slots)] = static_cast<byte_type>(width);
  StripReader written{{out + kStripHeaderBytes, counts_bytes}, count_width};
  StripWriter writer{out + kStripHeaderBytes + counts_bytes, width};
  for (size_t list = 0; list != lists; ++list) {
    uint32_t count = 0;
    written.Copy(reinterpret_cast<byte_type*>(&count), 1);
    writer.Append(ids + list * list_bytes, count);
  }
  [[maybe_unused]] const auto* end = writer.Finish();
  SDB_ASSERT(static_cast<size_t>(end - out) == size);
  return size;
}

bool DecodeStrip(bytes_view in, std::span<byte_type> out) noexcept {
  if (in.size() < kStripHeaderBytes) {
    return false;
  }
  const auto slots = absl::little_endian::Load32(in.data());
  const uint32_t width = in[sizeof(slots)];
  const auto list_bytes = uint64_t{slots} * sizeof(uint32_t);
  if (slots == 0 || width == 0 || width > 32 || out.size() % list_bytes != 0) {
    return false;
  }
  const auto lists = out.size() / list_bytes;
  const auto count_width = static_cast<uint32_t>(std::bit_width(slots));
  const auto counts_bytes = StripBlockBytes(lists, count_width);
  if (in.size() - kStripHeaderBytes < counts_bytes) {
    return false;
  }
  StripReader counts{in.substr(kStripHeaderBytes, counts_bytes), count_width};
  StripReader ids{in.substr(kStripHeaderBytes + counts_bytes), width};
  constexpr uint32_t kPadding = kStripPadding;
  auto* at = out.data();
  for (size_t list = 0; list != lists; ++list) {
    uint32_t count = 0;
    if (!counts.Copy(reinterpret_cast<byte_type*>(&count), 1) ||
        count > slots || !ids.Copy(at, count)) {
      return false;
    }
    for (auto slot = count; slot != slots; ++slot) {
      std::memcpy(at + uint64_t{slot} * sizeof(uint32_t), &kPadding,
                  sizeof(kPadding));
    }
    at += list_bytes;
  }
  return ids.AtEnd();
}

size_t Encode(Method method, bytes_view in, byte_type* out) noexcept {
  const auto capacity = in.size() - 1;
  size_t size = 0;
  switch (method.codec) {
    case Codec::Strip:
      size =
        EncodeStrip(static_cast<uint32_t>(method.level), in, out, capacity);
      break;
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
    case Codec::Strip:
      decoded = DecodeStrip(in, out);
      break;
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

}  // namespace

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

namespace {

using FrameSource =
  absl::FunctionRef<void(absl::FunctionRef<void(bytes_view)>)>;

struct Plan {
  Frames frames;
  absl::InlinedVector<bytes_view, 1> raws;
  absl::InlinedVector<uint64_t, 1> offsets;
  byte_type* slots = nullptr;
};

Plan PlanFrames(Method method, uint32_t frame_bytes,
                FrameSource for_each_frame) {
  SDB_ASSERT(frame_bytes != 0 && frame_bytes <= kMaxFrameBytes);
  Plan plan;
  uint64_t raw_bytes = 0;
  bool full = true;
  for_each_frame([&](bytes_view raw) {
    SDB_ASSERT(!raw.empty() && raw.size() <= frame_bytes);
    full =
      full && (plan.raws.empty() || plan.raws.back().size() == frame_bytes);
    plan.raws.push_back(raw);
    plan.offsets.push_back(raw_bytes);
    raw_bytes += raw.size();
  });
  plan.frames = {
    .codec = method.codec,
    .raw_bytes = raw_bytes,
    .frame_bytes = frame_bytes,
  };
  plan.frames.sizes.resize(plan.raws.size());
  if (!full) {
    for (const auto raw : plan.raws) {
      plan.frames.raw_sizes.push_back(static_cast<uint32_t>(raw.size()));
    }
  }
  return plan;
}

void EncodeFrame(Method method, Plan& plan, size_t frame) noexcept {
  plan.frames.sizes[frame] = static_cast<uint32_t>(
    Encode(method, plan.raws[frame], plan.slots + plan.offsets[frame]));
}

struct FrameRef {
  size_t plan = 0;
  size_t frame = 0;
};

void EncodePlans(Method method, std::span<Plan> plans, ParallelFor parallel) {
  if (plans.size() == 1) {
    auto& plan = plans.front();
    parallel(plan.raws.size(),
             [&](size_t frame) { EncodeFrame(method, plan, frame); });
    return;
  }
  std::vector<FrameRef> refs;
  for (size_t p = 0; p != plans.size(); ++p) {
    for (size_t frame = 0; frame != plans[p].raws.size(); ++frame) {
      refs.push_back({.plan = p, .frame = frame});
    }
  }
  parallel(refs.size(), [&](size_t i) {
    EncodeFrame(method, plans[refs[i].plan], refs[i].frame);
  });
}

class CountingStream final : public duckdb::WriteStream {
 public:
  void WriteData(duckdb::const_data_ptr_t, duckdb::idx_t size) final {
    _bytes += size;
  }

  uint64_t Bytes() const noexcept { return _bytes; }

 private:
  uint64_t _bytes = 0;
};

void SplitFrames(bytes_view raw, uint32_t frame_bytes,
                 absl::FunctionRef<void(bytes_view)> visit) {
  for (size_t at = 0; at < raw.size(); at += frame_bytes) {
    visit(raw.substr(at, frame_bytes));
  }
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
  auto plan = PlanFrames(method, frame_bytes,
                         [&](absl::FunctionRef<void(bytes_view)> visit) {
                           SplitFrames(raw, frame_bytes, visit);
                         });
  Compressed out;
  out.slots = std::make_unique_for_overwrite<byte_type[]>(raw.size());
  plan.slots = out.slots.get();
  EncodePlans(method, {&plan, 1}, parallel);
  out.frames = std::move(plan.frames);
  return out;
}

std::vector<Compressed> Compress(Method method,
                                 std::span<const FrameWriter> raws,
                                 ParallelFor parallel) {
  std::vector<Plan> plans;
  plans.reserve(raws.size());
  std::vector<Compressed> out(raws.size());
  for (size_t i = 0; i != raws.size(); ++i) {
    const auto& raw = raws[i];
    auto& plan = plans.emplace_back(PlanFrames(
      method, raw.FrameBytes(), [&](absl::FunctionRef<void(bytes_view)> visit) {
        raw.ForEachFrame(visit);
      }));
    out[i].slots =
      std::make_unique_for_overwrite<byte_type[]>(plan.frames.raw_bytes);
    plan.slots = out[i].slots.get();
  }
  EncodePlans(method, plans, parallel);
  for (size_t i = 0; i != raws.size(); ++i) {
    out[i].frames = std::move(plans[i].frames);
  }
  return out;
}

Frames Compress(Method method, bytes_view raw, uint32_t frame_bytes,
                std::span<byte_type> slots, ParallelFor parallel) {
  auto plan = PlanFrames(method, frame_bytes,
                         [&](absl::FunctionRef<void(bytes_view)> visit) {
                           SplitFrames(raw, frame_bytes, visit);
                         });
  SDB_ASSERT(plan.frames.raw_bytes <= slots.size());
  plan.slots = slots.data();
  EncodePlans(method, {&plan, 1}, parallel);
  return std::move(plan.frames);
}

void ForEachStored(const Frames& frames, const byte_type* slots,
                   absl::FunctionRef<void(bytes_view)> visit) {
  uint64_t raw = 0;
  for (size_t frame = 0; frame != frames.sizes.size(); ++frame) {
    visit({slots + raw, frames.sizes[frame]});
    raw += FrameRawBytes(frames, frame);
  }
}

absl::InlinedVector<FrameStart, 1> FrameStarts(const Frames& frames) {
  absl::InlinedVector<FrameStart, 1> starts(frames.sizes.size() + 1);
  for (size_t frame = 0; frame != frames.sizes.size(); ++frame) {
    starts[frame + 1] = {
      .stored = starts[frame].stored + frames.sizes[frame],
      .raw = starts[frame].raw + FrameRawBytes(frames, frame),
    };
  }
  return starts;
}

void Decompress(const Frames& frames, size_t first, size_t last,
                bytes_view stored, std::span<byte_type> raw,
                ParallelFor parallel) {
  SDB_ASSERT(first <= last && last <= frames.sizes.size());
  absl::InlinedVector<FrameStart, 1> starts(last - first + 1);
  for (size_t frame = first; frame != last; ++frame) {
    const auto& start = starts[frame - first];
    starts[frame - first + 1] = {
      .stored = start.stored + frames.sizes[frame],
      .raw = start.raw + FrameRawBytes(frames, frame),
    };
  }
  if (stored.size() != starts.back().stored ||
      raw.size() != starts.back().raw) {
    Corrupted("its sizes do not match its frames");
  }
  parallel(last - first, [&](size_t i) {
    const auto frame = first + i;
    Decode(frames.codec, stored.substr(starts[i].stored, frames.sizes[frame]),
           raw.subspan(starts[i].raw, FrameRawBytes(frames, frame)));
  });
}

void Decompress(const Frames& frames, bytes_view stored,
                std::span<byte_type> raw, ParallelFor parallel) {
  if (raw.size() != frames.raw_bytes) {
    Corrupted("its sizes do not match its frames");
  }
  Decompress(frames, 0, frames.sizes.size(), stored, raw, parallel);
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
  object.WritePropertyInParts(
    kPackedBytes.id, kPackedBytes.tag, StoredBytes(packed.frames),
    [&](auto put, auto) {
      ForEachStored(packed.frames, packed.slots.get(),
                    [&](bytes_view frame) { put(frame.data(), frame.size()); });
    });
}

bool Pays(const Compressed& packed) {
  CountingStream counter;
  duckdb::BinarySerializer serializer{counter};
  serializer.Begin();
  WritePacked(serializer, packed);
  serializer.End();
  return counter.Bytes() < packed.frames.raw_bytes;
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
