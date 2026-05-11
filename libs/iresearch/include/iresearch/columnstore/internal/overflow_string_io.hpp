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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/base/internal/endian.h>

#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"

// Tight-packed overflow-string layout backed by an iresearch IndexOutput /
// IndexInput. Replaces DuckDB's 256KB-block-chained WriteOverflowStringsToDisk
// for the standalone columnstore driver.
//
// On-disk format (per overflow string):
//   uint32_t length  (little-endian)
//   bytes...         (length bytes)
//
// Strings are concatenated end-to-end at the IndexOutput's current cursor.
// The segment dictionary entry's `block_id` carries the file offset of the
// length prefix; `offset` is unused (always 0) and ignored by the reader.

namespace irs::columnstore {

// Write side. Plugged into DuckDB's UncompressedStringSegmentState via the
// OverflowStringWriter virtual; called from string_uncompressed.cpp's
// WriteString path for any string that doesn't fit inline in the segment
// dictionary block.
class IndexOutputOverflowWriter final : public duckdb::OverflowStringWriter {
 public:
  explicit IndexOutputOverflowWriter(IndexOutput& out) noexcept : _out{&out} {}

  void WriteString(duckdb::UncompressedStringSegmentState& /*state*/,
                   duckdb::string_t string, duckdb::block_id_t& result_block,
                   int32_t& result_offset) final {
    // block_id = file offset of the length prefix; offset unused.
    result_block = static_cast<duckdb::block_id_t>(_out->Position());
    result_offset = 0;
    const auto len = string.GetSize();
    SDB_ENSURE(len <= std::numeric_limits<uint32_t>::max(), sdb::ERROR_INTERNAL,
               "string too long for overflow format");
    _out->WriteU32(len);
    _out->WriteBytes(reinterpret_cast<const byte_type*>(string.GetData()), len);
  }

  // Nothing buffered -- bytes are written through immediately, so segment
  // close has no work to do. Kept to satisfy the virtual.
  void Flush() final {}

 private:
  IndexOutput* _out;
};

// Read side. Installed on UncompressedStringSegmentState::overflow_reader at
// segment open time (column_reader.cpp). DuckDB's ReadOverflowString routes
// here before its built-in block-chain dispatch.
//
// Stateless: the only member is the borrowed IndexInput pointer. ReadString
// uses the positional ReadData/ReadBytes overloads so concurrent calls on
// the same reader (multiple SegmentReaderImpl instances may share the cs
// IndexInput via UpdateMeta) don't race on a shared cursor.
//
// Length prefix is always read by copy (ReadBytes) -- it's 4 bytes, the
// copy cost is negligible, and it lets us decide on the body strategy
// from the decoded length without a wasted ReadData on the prefix.
//
// Body strategy:
//   - ReadData(pos+4, len) returns a stable pointer -> zero-copy string_t
//     pointing into that region. Covers the mmap dir (whole file mapped)
//     and memory-backed dirs that can serve a contiguous slice of the
//     requested length. The cs IndexInput is owned by the SegmentReader
//     snapshot, which outlives any result Vector produced from it -- the
//     same lifetime invariant the rest of the columnstore read path
//     relies on. No per-Vector aux holder needed.
//   - ReadData returns nullptr (buffered dir, or memory dir whose chunks
//     are smaller than `len`) -> fall back to ReadBytes into the result
//     Vector's StringHeap. Matches DuckDB's existing per-string copy cost.
class IndexInputOverflowReader final : public duckdb::OverflowStringReader {
 public:
  explicit IndexInputOverflowReader(IndexInput& in) noexcept : _in{&in} {}

  duckdb::string_t ReadString(duckdb::Vector& result, duckdb::block_id_t block,
                              int32_t offset) final {
    // WriteString always writes offset = 0; a non-zero value here means a
    // different layout was written -- catch it instead of reading garbage.
    SDB_ASSERT(offset == 0);
    const auto pos = static_cast<uint64_t>(block);
    uint32_t len;
    _in->ReadBytes(pos, reinterpret_cast<byte_type*>(&len), sizeof(len));
    len = absl::little_endian::Load32(reinterpret_cast<byte_type*>(&len));
    if (const auto* body = _in->ReadData(pos + sizeof(len), len)) {
      // Stable backing region (mmap, or a memory dir whose chunk covers
      // the full payload). Zero-copy string_t into it.
      return duckdb::string_t{reinterpret_cast<const char*>(body), len};
    }
    // Fallback: copy payload into the result Vector's StringHeap.
    // Either a buffered-I/O dir, or a memory dir whose chunk size is
    // smaller than the payload.
    auto out = duckdb::StringVector::EmptyString(result, len);
    _in->ReadBytes(pos + sizeof(len),
                   reinterpret_cast<byte_type*>(out.GetDataWriteable()), len);
    out.Finalize();
    return out;
  }

 private:
  IndexInput* _in;
};

}  // namespace irs::columnstore
