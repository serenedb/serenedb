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

#include "iresearch/formats/hnsw/hnsw_graph.hpp"

#include <absl/algorithm/container.h>
#include <absl/random/distributions.h>

#include <cmath>
#include <iterator>
#include <string_view>

#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

template<typename T, typename A>
void WriteArray(DataOutput& out, const std::vector<T, A>& v) {
  out.WriteU64(v.size());
  if (!v.empty()) {
    out.WriteData(reinterpret_cast<const byte_type*>(v.data()),
                  sizeof(T) * v.size());
  }
}

template<typename T, typename A>
void ReadArray(IndexInput& in, std::vector<T, A>& v) {
  const auto size = static_cast<size_t>(in.ReadI64());
  v.resize(size);
  if (size != 0) {
    ReadOnce(in, reinterpret_cast<byte_type*>(v.data()), sizeof(T) * size);
  }
}

constexpr uint64_t kPackBatchBytes = uint64_t{64} << 20;

template<typename T, typename A>
bytes_view Bytes(const std::vector<T, A>& v) noexcept {
  return {reinterpret_cast<const byte_type*>(v.data()), v.size() * sizeof(T)};
}

uint32_t ListFrameBytes(uint32_t slots) noexcept {
  const auto list = uint64_t{slots} * sizeof(uint32_t);
  const auto frame =
    std::max<uint64_t>(1, compression::kFrameBytes / list) * list;
  SDB_ASSERT(frame <= compression::kMaxFrameBytes);
  return static_cast<uint32_t>(frame);
}

uint64_t PackBatchBytes(bytes_view raw, uint32_t frame_bytes) noexcept {
  return std::min<uint64_t>(
    raw.size(),
    std::max<uint64_t>(1, kPackBatchBytes / frame_bytes) * frame_bytes);
}

compression::Frames WritePacked(DataOutput& out, compression::Method method,
                                bytes_view raw, uint32_t frame_bytes,
                                std::span<byte_type> slots,
                                ParallelFor parallel) {
  compression::Frames frames{
    .codec = method.codec, .raw_bytes = raw.size(), .frame_bytes = frame_bytes};
  const auto batch = PackBatchBytes(raw, frame_bytes);
  for (uint64_t at = 0; at < raw.size(); at += batch) {
    const auto packed = compression::Compress(method, raw.substr(at, batch),
                                              frame_bytes, slots, parallel);
    compression::ForEachStored(packed, slots.data(), [&](bytes_view frame) {
      out.WriteData(frame.data(), frame.size());
    });
    absl::c_copy(packed.sizes, std::back_inserter(frames.sizes));
  }
  return frames;
}

[[noreturn]] void Corrupted(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("HNSW graph: ", what));
}

template<typename T, typename A>
void ReadPacked(IndexInput& in, const compression::Frames& frames,
                std::vector<T, A>& v, ParallelFor parallel) {
  if (frames.raw_bytes % sizeof(T) != 0) {
    Corrupted("an array size that is not a whole number of elements");
  }
  v.resize(frames.raw_bytes / sizeof(T));
  const std::span<byte_type> raw{reinterpret_cast<byte_type*>(v.data()),
                                 frames.raw_bytes};
  const auto starts = compression::FrameStarts(frames);
  const auto count = frames.sizes.size();
  std::unique_ptr<byte_type[]> stored;
  uint64_t capacity = 0;
  for (size_t first = 0, last = 0; first != count; first = last) {
    last = first + 1;
    while (last != count &&
           starts[last + 1].stored - starts[first].stored <= kPackBatchBytes) {
      ++last;
    }
    const auto size = starts[last].stored - starts[first].stored;
    if (size > capacity) {
      stored = std::make_unique_for_overwrite<byte_type[]>(size);
      capacity = size;
    }
    ReadOnce(in, stored.get(), size);
    compression::Decompress(
      frames, first, last, {stored.get(), size},
      raw.subspan(starts[first].raw, starts[last].raw - starts[first].raw),
      parallel);
  }
}

}  // namespace

void HnswGraphWriter::Reset(size_t nodes, uint32_t m) {
  SDB_ASSERT(m != 0);
  _graph._m = m;
  _graph._m0 = 2 * _graph._m;
  _graph._levels.assign(nodes, 0);
  _graph._offsets.clear();
  _graph._base.clear();
  _graph._upper.clear();
  _graph._entry = kHnswInvalidNode;
  _graph._max_level = 0;
}

uint64_t HnswGraph::ComputeOffsets() {
  _offsets.resize(_levels.size() + 1);
  uint64_t total = 0;
  for (size_t i = 0; i < _levels.size(); ++i) {
    _offsets[i] = total;
    if (_levels[i] > 1) {
      total += static_cast<uint64_t>(_levels[i] - 1) * _m;
    }
  }
  _offsets.back() = total;
  return total;
}

void HnswGraphWriter::AllocateLinks() {
  auto& levels = _graph._levels;
  _graph._base.assign(levels.size() * size_t{_graph._m0}, kHnswInvalidNode);
  _graph._upper.assign(_graph.ComputeOffsets(), kHnswInvalidNode);

  _proc_offsets.resize(levels.size() + 1);
  uint32_t slots = 0;
  for (size_t i = 0; i < levels.size(); ++i) {
    _proc_offsets[i] = slots;
    slots += levels[i];
  }
  _proc_offsets.back() = slots;
  _processed.assign(slots, 0);
}

void HnswGraph::Serialize(DataOutput& out) const {
  out.WriteU32(_m);
  out.WriteU32(_entry);
  out.WriteU32(_max_level);
  WriteArray(out, _levels);
  WriteArray(out, _base);
  WriteArray(out, _upper);
}

HnswGraphFrames HnswGraph::SerializePacked(DataOutput& out,
                                           ParallelFor parallel) const {
  out.WriteU32(_m);
  out.WriteU32(_entry);
  out.WriteU32(_max_level);
  const auto levels = Bytes(_levels);
  const auto base = Bytes(_base);
  const auto upper = Bytes(_upper);
  const auto base_frame = ListFrameBytes(_m0);
  const auto upper_frame = ListFrameBytes(_m);
  const auto size = std::max({
    PackBatchBytes(levels, compression::kFrameBytes),
    PackBatchBytes(base, base_frame),
    PackBatchBytes(upper, upper_frame),
  });
  const auto buffer = std::make_unique_for_overwrite<byte_type[]>(size);
  const std::span slots{buffer.get(), size};
  HnswGraphFrames frames;
  frames.levels = WritePacked(out, compression::kLz4Hc9, levels,
                              compression::kFrameBytes, slots, parallel);
  frames.base = WritePacked(out, compression::Strip(_m0), base, base_frame,
                            slots, parallel);
  frames.upper = WritePacked(out, compression::Strip(_m), upper, upper_frame,
                             slots, parallel);
  return frames;
}

HnswGraph HnswGraph::Deserialize(IndexInput& in) {
  HnswGraph g;
  g._m = static_cast<uint32_t>(in.ReadI32());
  g._m0 = 2 * g._m;
  g._entry = static_cast<uint32_t>(in.ReadI32());
  g._max_level = static_cast<uint32_t>(in.ReadI32());
  ReadArray(in, g._levels);
  g.ComputeOffsets();
  ReadArray(in, g._base);
  ReadArray(in, g._upper);
  return g;
}

HnswGraph HnswGraph::DeserializePacked(IndexInput& in,
                                       const HnswGraphFrames& frames,
                                       ParallelFor parallel) {
  HnswGraph g;
  g._m = static_cast<uint32_t>(in.ReadI32());
  g._m0 = 2 * g._m;
  g._entry = static_cast<uint32_t>(in.ReadI32());
  g._max_level = static_cast<uint32_t>(in.ReadI32());
  ReadPacked(in, frames.levels, g._levels, parallel);
  const auto upper = g.ComputeOffsets();
  if (frames.base.raw_bytes !=
        uint64_t{g._levels.size()} * g._m0 * sizeof(uint32_t) ||
      frames.upper.raw_bytes != upper * sizeof(uint32_t)) {
    Corrupted("link arrays that do not match its levels");
  }
  ReadPacked(in, frames.base, g._base, parallel);
  ReadPacked(in, frames.upper, g._upper, parallel);
  return g;
}

uint32_t HnswRandomLevel(pcg32& rng, uint32_t m) noexcept {
  const double u =
    absl::Uniform<double>(absl::IntervalOpenClosed, rng, 0.0, 1.0);
  const double factor = 1.0 / std::log(static_cast<double>(std::max(m, 2U)));
  const double sample = -std::log(u) * factor;
  return std::min(static_cast<uint32_t>(std::lround(sample)) + 1,
                  kHnswMaxLevel);
}

void HnswSuffixNorms(const float* v, uint32_t d, uint32_t width, uint32_t first,
                     float* out) noexcept {
  float sum = 0.f;
  for (uint32_t level = (d + width - 1) / width; level-- > first;) {
    const uint32_t b = level * width;
    sum += faiss::fvec_norm_L2sqr(v + b, std::min(width, d - b));
    out[level - first] = std::sqrt(sum);
  }
}

void HnswPanoramaTails(const float* q, uint32_t d, float* tails) noexcept {
  tails[HnswPanoramaLevels(d)] = 0.f;
  HnswSuffixNorms(q, d, kHnswPanoramaLevel, 0, tails);
}

}  // namespace irs
