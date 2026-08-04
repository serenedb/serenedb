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
#include <memory>
#include <span>

#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_writer.hpp"

namespace irs::term_dict {

// A field's dictionary layout: which planes it has and how its leaves encode
// keys.
enum class LayoutKind : uint8_t {
  // Leaf blocks of front-coded entries, optionally FSST-compressed suffixes,
  // plus flat prefix-truncated separators and a block directory.
  Var = 0,
  // As `Var`, with leaves holding a sorted fixed-stride key array instead of
  // front-coding varints.
  FixedStride = 1,
  // No dictionary at all: the field *is* its one term, so the field header
  // carries that term's record directly and no term bytes, block or
  // separator is stored.
  Single = 2,
};

// Mean sampled suffix length, in bytes, above which the per-field FSST codec
// stays off however well the sample compresses. Sequential iteration pays one
// decode per entry, and that cost tracks the encoded suffix length.
inline constexpr uint32_t kFsstMaxMeanSuffix = 16;

// Mean sampled suffix length below which the codec stays off without being
// trained. An FSST code is a byte, so the encoding is never shorter than one
// byte per suffix and a suffix this short has nothing left over to pay for the
// symbol table. Every shape that reaches this floor -- the numeric precision
// ladders, byte ngrams, hex ngrams -- trains a table and measures 99-106%
// against a 115% threshold, i.e. builds it and throws it away.
inline constexpr uint32_t kFsstMinMeanSuffix = 2;

inline constexpr uint32_t kRestartIntervalDefault = 16;

// A row group larger than any segment: one row group spans the segment, so
// row-group-local ids are the segment's doc ids and no rg-lists are written.
// A test convenience, never a production setting.
inline constexpr uint32_t kRowGroupSizeUnbounded = 2'000'000'000;

struct WriterOptions {
  // Rows per row group. Postings of a term are cut at row group boundaries
  // and the doc ids handed to the postings writer are row-group local.
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  uint32_t block_byte_target = 4096;
  uint32_t restart_interval = kRestartIntervalDefault;
  // Minimal sampled suffix compression ratio, scaled by 100, at which the
  // per-field FSST codec is enabled.
  uint32_t fsst_min_ratio_pct = 115;
  // Mean sampled suffix length above which FSST stays off regardless of the
  // ratio: decode work grows with the encoded suffix, and full-dictionary
  // iteration pays it per entry.
  uint32_t fsst_max_mean_suffix = kFsstMaxMeanSuffix;
  // Mean sampled suffix length below which FSST stays off without training.
  uint32_t fsst_min_mean_suffix = kFsstMinMeanSuffix;
  bool fsst_enabled = true;
};

class FieldWriter final {
 public:
  using ptr = std::unique_ptr<FieldWriter>;

  FieldWriter(PostingsWriter::ptr pw, bool compaction, IResourceManager& rm,
              const DictOptions& dict = {});
  ~FieldWriter();

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept;
  void SetOptions(const WriterOptions& options) noexcept;
  void prepare(const FlushState& state);
  void write(const BasicTermReader& reader);
  void end();

 private:
  class Impl;
  std::unique_ptr<Impl> _impl;
};

class FieldReader final {
 public:
  using ptr = std::shared_ptr<FieldReader>;

  FieldReader(PostingsReader::ptr pr, IResourceManager& rm);
  ~FieldReader();

  FieldReader(const FieldReader&) = delete;
  FieldReader& operator=(const FieldReader&) = delete;

  uint64_t CountMappedMemory() const;
  void prepare(const ReaderState& state);

  const TermReader* field(field_id id) const;

  std::span<const field_id> field_ids() const noexcept;

  size_t size() const noexcept;

  // The segment's row-group grid, as the writer cut it.
  RowGroupLayout RowGroups() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> _impl;
};

#ifdef SDB_DEV
// Leaf blocks an automaton walk has settled since process start: `decided` in
// one pass over the block's stored bytes, `walked` one key at a time.
// Debug-only: nothing in the system reads them.
struct AutomatonBlocks {
  size_t decided;
  size_t walked;
};

AutomatonBlocks AutomatonBlockCounts() noexcept;
#endif

}  // namespace irs::term_dict
