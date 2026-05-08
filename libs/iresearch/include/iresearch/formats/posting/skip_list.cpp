////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "skip_list.hpp"

#include <cstdint>
#include <iresearch/types.hpp>

#include "basics/assert.h"
#include "basics/math_utils.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "basics/system-compiler.h"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// returns maximum number of skip levels needed to store specified
// count of objects for skip list with
// step skip_0 for 0 level, step skip_n for other levels
constexpr size_t CountMaxLevels(uint64_t skip_0, uint64_t skip_n,
                                uint64_t count) noexcept {
  return skip_0 < count ? 1 + math::Log(count / skip_0, skip_n) : 0;
}

static_assert(CountMaxLevels(doc_limits::kBlockSize, doc_limits::kSkipSize,
                             doc_limits::eof()) == doc_limits::kMaxSkipLevels);

constexpr uint32_t ByteSize124ForSkipEntry(uint32_t value) {
  if (value < (uint32_t{1} << 8)) {
    return 1;
  }
  if (value < (uint32_t{1} << 16)) {
    return 2;
  }
  return 3;  // actually should be 4, but return 3 to encode correctly
}

template<typename Output>
void Serialize124ForSkipEntry(uint32_t code, uint32_t value, Output& out) {
  switch (code) {
    case 1:
    case 2:
      out.WriteBytes(reinterpret_cast<byte_type*>(&value), code);
      break;
    case 3:
      out.WriteU32(value);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

constexpr uint32_t ByteSize1248ForSkipEntry(uint64_t value) {
  /*
    0 - 1 byte
    1 - 2 bytes
    2 - 4 bytes
    3 - 8 bytes
  */
  if (value < (uint64_t{1} << 8)) {
    return 0;
  }
  if (value < (uint64_t{1} << 16)) {
    return 1;
  }
  if (value < (uint64_t{1} << 32)) {
    return 2;
  }
  return 3;
}

template<typename Output>
void Serialize1248ForSkipEntry(uint32_t code, uint64_t value, Output& out) {
  switch (code) {
    case 0:
      out.WriteByte(value);
      break;
    case 1:
      out.WriteU16(value);
      break;
    case 2:
      out.WriteU32(value);
      break;
    case 3:
      out.WriteU64(value);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

template<typename Output>
void SerializeFor1234(uint32_t code, uint32_t value, Output& out) {
  out.WriteBytes(reinterpret_cast<byte_type*>(&value), code);
}

}  // namespace

void NewSkipWriter::Prepare(size_t) {
  _level1.Reset();
  _count_entries_level1 = 0;
}

void NewSkipWriter::WriteInlineSkipEntry(const InlineSkipEntry& skip_entry,
                                         IndexOutput& out) {
  auto max_doc_delta_size = ByteSize1234(skip_entry.max_doc_delta);
  uint32_t wand_freq_code = 0;
  uint32_t wand_norm_code = 0;
  if (skip_entry.wand_data) {
    wand_freq_code = ByteSize124ForSkipEntry(skip_entry.wand_data->freq);
    if (skip_entry.wand_data->norm) {
      wand_norm_code = ByteSize124ForSkipEntry(*skip_entry.wand_data->norm);
    }
  }

  out.WriteByte((max_doc_delta_size - 1) | (wand_freq_code << 2) |
                (wand_norm_code << 4));  // encoding byte
  out.WriteBytes(reinterpret_cast<const byte_type*>(&skip_entry.max_doc_delta),
                 max_doc_delta_size);
  if (wand_freq_code > 0) {
    Serialize124ForSkipEntry(wand_freq_code, skip_entry.wand_data->freq, out);
  }

  if (wand_norm_code > 0) {
    Serialize124ForSkipEntry(wand_norm_code, *skip_entry.wand_data->norm, out);
  }
  out.WriteU16(skip_entry.rest_block_size);
}

void NewSkipWriter::WriteInlinePosPayMetadata(const PosPayMetadata& meta,
                                              const Features& features,
                                              IndexOutput& out) {
  if (features.HasPosition()) {
    uint32_t pos_pay_enc = 0;
    uint32_t pos_ptr_size = ByteSize1234(meta.pos_ptr);
    pos_pay_enc |= (pos_ptr_size - 1);
    out.WriteBytes(reinterpret_cast<const byte_type*>(&meta.pos_ptr),
                   pos_ptr_size);

    uint32_t pay_ptr_size = ByteSize1234(meta.pay_ptr);
    if (features.HasOffset()) {
      pos_pay_enc |= ((pay_ptr_size - 1) << 2);
      out.WriteBytes(reinterpret_cast<const byte_type*>(&meta.pay_ptr),
                     pay_ptr_size);
    }

    out.WriteByte(meta.pos_block_idx);
    out.WriteByte(pos_pay_enc);
  }
}

void NewSkipWriter::WriteSkipEntry(const SkipEntry& skip_entry,
                                   const Features& features,
                                   MemoryIndexOutput& out) {
  auto max_doc_delta_code = ByteSize1234(skip_entry.max_doc_delta);
  auto doc_ptr_code = ByteSize1248ForSkipEntry(skip_entry.doc_ptr);
  uint32_t pos_ptr_code = 0;
  uint32_t pay_ptr_code = 0;
  if (features.HasPosition()) {
    pos_ptr_code = ByteSize1248ForSkipEntry(skip_entry.meta.pos_ptr);
    if (features.HasOffset()) {
      pay_ptr_code = ByteSize1248ForSkipEntry(skip_entry.meta.pay_ptr);
    }
  }

  out.WriteByte((max_doc_delta_code - 1) | (doc_ptr_code << 2) |
                (pos_ptr_code << 4) | (pay_ptr_code << 6));

  SerializeFor1234(max_doc_delta_code, skip_entry.max_doc_delta, out);
  Serialize1248ForSkipEntry(doc_ptr_code, skip_entry.doc_ptr, out);
  if (features.HasPosition()) {
    Serialize1248ForSkipEntry(pos_ptr_code, skip_entry.meta.pos_ptr, out);
    if (features.HasOffset()) {
      Serialize1248ForSkipEntry(pay_ptr_code, skip_entry.meta.pay_ptr, out);
    }
    out.WriteByte(skip_entry.meta.pos_block_idx);
  }
}

void NewSkipWriter::WriteWandData(WandWriter::WandData data, IndexOutput& out) {
  uint32_t freq_size = ByteSize1234(data.freq);
  uint32_t norm_code = (data.norm ? ByteSize124ForSkipEntry(*data.norm) : 0);
  out.WriteByte((freq_size - 1) | (norm_code << 2));

  SerializeFor1234(freq_size, data.freq, out);
  if (norm_code > 0) {
    Serialize124ForSkipEntry(norm_code, *data.norm, out);
  }
}

size_t NewSkipWriter::CalculatePosPayMetaSize(PosPayMetadata meta,
                                              const Features& features) {
  size_t byte_count = 0;
  if (features.HasPosition()) {
    ++byte_count;  // encoding byte
    ++byte_count;  // pos_block_idx
    byte_count += ByteSize1234(meta.pos_ptr);
    if (features.HasOffset()) {
      byte_count += ByteSize1234(meta.pay_ptr);
    }
  }
  return byte_count;
}

void NewSkipWriter::FlushLevel(size_t num_levels, IndexOutput& out) {
  SDB_ASSERT(num_levels <= kMaxLevels);
  if (num_levels == 2) {
    auto& stream = _level1.stream;
    stream.Flush();  // update length of each buffer
    out.WriteU32(_count_entries_level1);
    _level1.file >> out;
  }
}

void SkipWriter::Prepare(size_t max_levels, size_t count) {
  _max_levels = std::min(max_levels, CountMaxLevels(_skip_0, _skip_n, count));
  _levels.reserve(_max_levels);

  // reset existing skip levels
  for (auto& level : _levels) {
    level.Reset();
  }

  // add new skip levels if needed
  for (auto size = std::size(_levels); size < _max_levels; ++size) {
    _levels.emplace_back(_levels.get_allocator().Manager());
  }
}

uint32_t SkipWriter::CountLevels() const {
  auto level = std::make_reverse_iterator(std::begin(_levels) + _max_levels);
  const auto rend = std::rend(_levels);

  // find first filled level
  level = std::find_if(level, rend, [](const MemoryOutput& level) {
    return level.stream.Position();
  });

  // count number of levels
  const auto num_levels = static_cast<uint32_t>(std::distance(level, rend));
  return num_levels;
}

void SkipWriter::FlushLevels(uint32_t num_levels, IndexOutput& out) {
  // write number of levels
  out.WriteV32(num_levels);

  // write levels from n downto 0
  auto level = std::make_reverse_iterator(std::begin(_levels) + num_levels);
  const auto rend = std::rend(_levels);
  for (; level != rend; ++level) {
    auto& stream = level->stream;
    stream.Flush();  // update length of each buffer

    const uint64_t length = stream.Position();
    SDB_ASSERT(length);
    out.WriteV64(length);
    level->file >> out;
  }
}

}  // namespace irs
