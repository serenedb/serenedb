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

#include <fsst.h>

#include <array>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kPage = 4096;
inline constexpr uint32_t kRestartInterval = 16;
inline constexpr uint32_t kRecordInterval = 8;
inline constexpr uint32_t kHeadBytes = sizeof(uint32_t);
inline constexpr uint32_t kRecordOffset = sizeof(uint16_t);
inline constexpr uint32_t kMaxStride = 32;

enum class BlockKind : uint8_t {
  Var = 0,
  FixedStride = 1,
  VarFsst = 2,
};

struct FsstEncoderDelete {
  void operator()(duckdb_fsst_encoder_t* encoder) const noexcept;
};

using FsstEncoder = std::unique_ptr<duckdb_fsst_encoder_t, FsstEncoderDelete>;

struct RecordBytes {
  std::array<byte_type, 128> bytes;
};

class RecordOutput final : private RecordBytes, public BufferedOutput {
 public:
  RecordOutput() noexcept
    : RecordBytes{},
      BufferedOutput{bytes.data(), bytes.data() + bytes.size()} {}

  bytes_view View() const noexcept { return {_buf, Length()}; }
  void Clear() noexcept { _pos = _buf; }

 private:
  void WriteDirect(const byte_type* b, size_t len) final;
};

constexpr uint64_t RecordGroups(uint64_t count) noexcept {
  return (count + kRecordInterval - 1) / kRecordInterval;
}

struct VarColumns {
  bstring headers;
  bstring suffixes;
  std::vector<std::array<uint16_t, 2>> restarts;
};

struct BlockBuilder {
  bstring keys;
  std::vector<uint32_t> ends;
  bstring records;
  std::vector<uint16_t> record_starts;
  VarColumns raw;
  VarColumns coded;
  uint64_t size = 0;
  uint64_t coded_size = std::numeric_limits<uint64_t>::max();
  uint32_t width = 0;
  uint32_t prefix = 0;
  uint32_t head_skip = 0;
  BlockKind kind = BlockKind::Var;
};

inline constexpr size_t kMaxRangeLevels = 8;

class RangeWriter final {
 public:
  bool Active() const noexcept { return _pw != nullptr; }
  void Begin(PostingsWriter& pw);
  void Add(bytes_view term, const PostingMeta& meta);
  void Finish(uint64_t terms);
  void Write(bstring& region, size_t prefix) const;
  void Clear() noexcept;

 private:
  struct Piece {
    bstring key;
    std::vector<doc_id_t> docs;
    PostingMeta meta;
    uint64_t docs_count = 0;
    uint32_t first = 0;
    bool join = false;
  };

  struct Level {
    bstring starts;
    std::vector<uint32_t> firsts;
    std::vector<uint32_t> ends;
    bstring records;
    Piece group;
    Piece held;
    uint32_t count = 0;
    uint32_t children = 0;
    bool holding = false;
  };

  struct Child {
    bytes_view key;
    const PostingMeta& meta;
    std::span<const doc_id_t> docs;
    uint64_t docs_count;
    uint32_t index;
  };

  void Push(size_t level, const Child& child);
  void Close(size_t level);
  void Release(size_t level);
  void TeeLeaf() noexcept;

  std::array<Level, kMaxRangeLevels> _levels;
  PostingsWriter* _pw = nullptr;
  RecordOutput _record;
  size_t _height = 0;
};

class FieldWriter final {
 public:
  explicit FieldWriter(bool compaction) noexcept : _compaction{compaction} {}

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }
  void prepare(const FlushState& state);
  void write(const BasicTermReader& reader);
  void end();

 private:
  void TrainFsst(const BasicTermReader& reader);
  bytes_view EncodeSuffix(bytes_view suffix);
  void Add(bytes_view term, const PostingMeta& meta);
  bool Append(bytes_view term, const PostingMeta& meta);
  bool AppendFixedKey(bytes_view term, uint64_t records);
  bool AppendVarKey(bytes_view term, uint64_t records);
  void Seal();
  void Cut();
  void WriteBlock(bytes_view block);
  void EndField(field_id id, const FieldProperties& props,
                uint64_t total_doc_freq, uint64_t total_term_freq);
  void Reset() noexcept;

  PostingsWriter _pw;
  RecordOutput _record;
  BlockBuilder _block;
  RangeWriter _ranges;
  bstring _region;
  bstring _inline;
  FsstEncoder _fsst;
  bstring _fsst_table;
  bstring _coded;
  bstring _page;
  bstring _directory;
  bstring _separators;
  bstring _separator;
  bstring _last;
  bstring _min;
  IdxWriter* _idx = nullptr;
  IndexOutput* _out = nullptr;
  uint64_t _terms = 0;
  uint32_t _blocks = 0;
  uint32_t _width = 0;
  bool _fsst_used = false;
  const bool _compaction;
};

class FieldReader;

struct BlockBuffer {
  IndexInput::ptr in;
  bstring copy;
};

struct RangeLevel {
  const byte_type* starts;
  const byte_type* firsts;
  const byte_type* offsets;
  const byte_type* records;
  uint32_t count;
};

class TermReaderImpl final : public TermReader {
 public:
  TermReaderImpl(const FieldReader& owner, field_id id,
                 const TermDictMeta& meta, IndexInput& in);
  ~TermReaderImpl() final;

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr IteratorUntil(bytes_view upper) const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const RegexpConjunction& a) const final;
  SeekTermIterator::ptr iterator(const FuzzyConjunction& a) const final;

  PostingMeta Lookup(bytes_view term) const final;
  void LookupBatch(std::span<const bytes_view> terms,
                   std::span<PostingMeta> out) const final;
  void ReadDocs(bytes_view term, Acceptor acceptor) const final;
  void VisitRange(bytes_view from, bytes_view to,
                  RangeVisitor visit) const final;

  std::unique_ptr<IndexInput> ReopenPayload() const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return _terms; }
  uint64_t docs_count() const noexcept final { return _docs; }
  bytes_view min() const noexcept final { return _min; }
  bytes_view max() const noexcept final { return _max; }
  uint32_t KeyWidth() const noexcept final { return _width; }
  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final;
  bool HasScoreBounds() const noexcept final { return _has_score_bounds; }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  const FieldReader& Owner() const noexcept { return *_owner; }
  uint32_t Blocks() const noexcept { return _blocks; }
  bool Small() const noexcept { return _small != nullptr; }
  const duckdb_fsst_decoder_t* Fsst() const noexcept { return _fsst.get(); }
  const Root& GetRoot() const noexcept { return _root; }
  bytes_view BlockPrefix(size_t block) const noexcept {
    const bytes_view lower = block == 0 ? min() : _root.Separator(block);
    const bytes_view upper =
      block + 1 == _blocks ? max() : _root.Separator(block + 1);
    return lower.substr(0, CommonPrefixLength(lower, upper));
  }
  const byte_type* Block(size_t block, BlockBuffer& buffer) const;
  uint64_t BlockOffset(size_t block) const noexcept {
    return uint64_t{_pages[block]} * kPage;
  }
  uint64_t BlocksEnd() const noexcept {
    return uint64_t{_pages[_blocks]} * kPage;
  }
  std::span<const RangeLevel> RangeLevels() const noexcept { return _levels; }
  uint32_t RangePrefix() const noexcept { return _range_prefix; }
  uint64_t InlineBegin() const noexcept { return _inline_offset; }
  uint64_t InlineEnd() const noexcept { return _inline_offset + _inline_size; }
  void ReadInline(PostingMeta& meta, BlockBuffer& buffer) const;

 private:
  bool Excludes(bytes_view term) const noexcept;
  uint64_t ReadDictionary(IndexInput& in, uint8_t flags);
  uint64_t ReadRanges(IndexInput& in);
  uint64_t MapInline(IndexInput& in);

  const FieldReader* _owner;
  FieldMeta _meta;
  uint64_t _terms;
  uint64_t _docs;
  bstring _min;
  bstring _max;
  bstring _small_copy;
  const byte_type* _small = nullptr;
  std::unique_ptr<duckdb_fsst_decoder_t> _fsst;
  uint32_t _blocks = 0;
  uint32_t _width = 0;
  bool _has_score_bounds;
  FreqAttr _freq;
  Root _root;
  std::vector<uint32_t> _pages;
  std::vector<RangeLevel> _levels;
  bstring _ranges_copy;
  uint32_t _range_prefix = 0;
  uint64_t _root_bytes = 0;
  bstring _inline_copy;
  const byte_type* _inline = nullptr;
  uint64_t _inline_offset = 0;
  uint64_t _inline_size = 0;
};

class FieldReader final {
 public:
  explicit FieldReader(IResourceManager& rm) noexcept : _rm{&rm} {}

  FieldReader(const FieldReader&) = delete;
  FieldReader& operator=(const FieldReader&) = delete;

  uint64_t CountMappedMemory() const;
  void prepare(const ReaderState& state);

  const TermReader* field(field_id id) const;
  std::span<const field_id> field_ids() const noexcept { return _ids; }
  size_t size() const noexcept { return _fields.size(); }

  const PostingsReader& Postings() const noexcept { return _pr; }
  const IndexInput& Input() const noexcept { return *_in; }
  const byte_type* Base() const noexcept { return _base; }
  IResourceManager& ResourceManager() const noexcept { return *_rm; }

 private:
  std::deque<TermReaderImpl> _fields;
  containers::FlatHashMap<field_id, const TermReaderImpl*> _by_id;
  std::vector<field_id> _ids;
  PostingsReader _pr;
  IndexInput::ptr _in;
  const byte_type* _base = nullptr;
  IResourceManager* _rm;
};

}  // namespace irs::term_dict
