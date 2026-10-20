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

#include <array>
#include <cstdint>
#include <deque>
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
inline constexpr uint32_t kMaxStride = 32;

enum class BlockKind : uint8_t {
  Var = 0,
  FixedStride = 1,
};

struct RecordBytes {
  std::array<byte_type, 128> bytes;
};

class RecordOutput final : private RecordBytes, public BufferedOutput {
 public:
  RecordOutput() noexcept
    : RecordBytes{}, BufferedOutput{bytes.data(), bytes.data() + bytes.size()} {}

  bytes_view View() const noexcept { return {_buf, Length()}; }
  void Clear() noexcept { _pos = _buf; }

 private:
  void WriteDirect(const byte_type* b, size_t len) final;
};

struct BlockBuilder {
  bstring keys;
  std::vector<uint32_t> ends;
  bstring headers;
  bstring suffixes;
  bstring records;
  std::vector<std::array<uint16_t, 4>> restarts;
  uint64_t size = 0;
  uint64_t inline_base = 0;
  uint32_t width = 0;
  uint32_t prefix = 0;
  bool uniform = true;
  BlockKind kind = BlockKind::Var;
};

class FieldWriter final {
 public:
  FieldWriter(bool compaction, IResourceManager& rm);

  FieldWriter(const FieldWriter&) = delete;
  FieldWriter& operator=(const FieldWriter&) = delete;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }
  void prepare(const FlushState& state);
  void write(const BasicTermReader& reader);
  void end();

 private:
  void Add(bytes_view term, const PostingMeta& meta);
  bool Append(bytes_view term, const PostingMeta& meta);
  void Cut();
  void WriteBlock(bytes_view block);
  void EndField(field_id id, const FieldProperties& props,
                uint64_t total_doc_freq, uint64_t total_term_freq);

  PostingsWriter _pw;
  RecordOutput _record;
  BlockBuilder _block;
  bstring _page;
  bstring _region;
  bstring _inline;
  bstring _directory;
  bstring _separators;
  bstring _separator;
  bstring _last;
  bstring _min;
  IdxWriter* _idx = nullptr;
  IndexOutput* _out = nullptr;
  uint64_t _terms = 0;
  uint32_t _blocks = 0;
  const bool _compaction;
};

class FieldReader;

struct BlockBuffer {
  IndexInput::ptr in;
  bstring copy;
};

class TermReaderImpl final : public TermReader {
 public:
  TermReaderImpl(const FieldReader& owner, field_id id,
                 const TermDictMeta& meta, const byte_type* data);
  ~TermReaderImpl();

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const RegexpConjunction& a) const final;
  SeekTermIterator::ptr iterator(const FuzzyConjunction& a) const final;

  PostingMeta Lookup(bytes_view term) const final;
  void ReadDocs(bytes_view term, Acceptor acceptor) const final;

  std::unique_ptr<IndexInput> ReopenPayload() const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return _terms; }
  uint64_t docs_count() const noexcept final { return _docs; }
  bytes_view min() const noexcept final { return _min; }
  bytes_view max() const noexcept final { return _max; }
  bool HasScoreBounds() const noexcept final { return _has_score_bounds; }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  const FieldReader& Owner() const noexcept { return *_owner; }
  uint32_t Blocks() const noexcept { return _blocks; }
  bool Small() const noexcept { return _small != nullptr; }
  uint32_t RestartInterval() const noexcept { return _restart_interval; }
  const Root& GetRoot() const noexcept { return _root; }
  const byte_type* Inline() const noexcept { return _inline; }
  const byte_type* Block(size_t block, BlockBuffer& buffer) const;
  uint64_t BlockOffset(size_t block) const noexcept {
    return uint64_t{_pages[block]} * kPage;
  }
  uint64_t BlocksEnd() const noexcept {
    return uint64_t{_pages[_blocks]} * kPage;
  }

 private:
  const FieldReader* _owner;
  FieldMeta _meta;
  uint64_t _terms;
  uint64_t _docs;
  bytes_view _min;
  bytes_view _max;
  const byte_type* _inline = nullptr;
  const byte_type* _small = nullptr;
  uint32_t _blocks = 0;
  uint32_t _restart_interval = kRestartInterval;
  bool _has_score_bounds;
  FreqAttr _freq;
  Root _root;
  std::vector<uint32_t> _pages;
  uint64_t _root_bytes = 0;
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
  bstring _region;
  IResourceManager* _rm;
};

}  // namespace irs::term_dict
