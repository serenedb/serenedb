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

#pragma once

#include <absl/algorithm/container.h>

#include <bit>
#include <limits>
#include <memory>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "basics/down_cast.h"
#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "columnstore2.hpp"
#include "format_utils.hpp"
#include "formats.hpp"
#include "formats_attributes.hpp"
#include "formats_burst_trie.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/formats/skip_list.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/bitpack.hpp"
#include "iresearch/utils/type_limits.hpp"

extern "C" {
#include <simdbitpacking.h>
#include <simdintegratedbitpacking.h>
}

namespace irs {

inline constexpr IndexFeatures kPos = IndexFeatures::Freq | IndexFeatures::Pos;

inline void WriteStrings(IndexOutput& out, const auto& strings) {
  SDB_ASSERT(strings.size() < std::numeric_limits<uint32_t>::max());

  out.WriteV32(static_cast<uint32_t>(strings.size()));
  for (const auto& s : strings) {
    WriteStr(out, s);
  }
}

inline std::vector<std::string> ReadStrings(IndexInput& in) {
  const size_t size = in.ReadV32();

  if (size > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
    throw IoError{absl::StrCat("Too many strings to read: ", size)};
  }

  std::vector<std::string> strings(size);
  for (auto& s : strings) {
    s = ReadString<std::string>(in);
  }

  return strings;
}

template<typename T, typename M>
std::string FileName(const M& meta);

inline void PrepareOutput(std::string& str, IndexOutput::ptr& out,
                          const FlushState& state, std::string_view ext,
                          std::string_view format, const int32_t version) {
  SDB_ASSERT(!out);
  irs::FileName(str, state.name, ext);
  out = state.dir->create(str);

  if (!out) {
    throw IoError{absl::StrCat("Failed to create file, path: ", str)};
  }

  format_utils::WriteHeader(*out, format, version);
}

inline void PrepareInput(std::string& str, IndexInput::ptr& in, IOAdvice advice,
                         const ReaderState& state, std::string_view ext,
                         std::string_view format, const int32_t min_ver,
                         const int32_t max_ver) {
  SDB_ASSERT(!in);
  irs::FileName(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", str)};
  }

  format_utils::CheckHeader(*in, format, min_ver, max_ver);
}

// Buffer for storing skip data
struct SkipBuffer {
  explicit SkipBuffer(uint64_t* skip_ptr) noexcept : skip_ptr{skip_ptr} {}

  void Reset() noexcept { start = end = 0; }

  uint64_t* skip_ptr;  // skip data
  uint64_t start{};    // start position of block
  uint64_t end{};      // end position of block
};

// Buffer for storing doc data
struct DocBuffer : SkipBuffer {
  DocBuffer(std::span<doc_id_t>& docs, std::span<uint32_t>& freqs,
            doc_id_t* skip_doc, uint64_t* skip_ptr) noexcept
    : SkipBuffer{skip_ptr}, docs{docs}, freqs{freqs}, skip_doc{skip_doc} {}

  bool Full() const noexcept { return doc == std::end(docs); }

  bool Empty() const noexcept { return doc == std::begin(docs); }

  void Push(doc_id_t doc, uint32_t freq) noexcept {
    *this->doc = doc;
    ++this->doc;
    *this->freq = freq;
    ++this->freq;
    last = doc;
  }

  std::span<doc_id_t> docs;
  std::span<uint32_t> freqs;
  uint32_t* skip_doc;
  std::span<doc_id_t>::iterator doc{docs.begin()};
  std::span<uint32_t>::iterator freq{freqs.begin()};
  doc_id_t last{doc_limits::invalid()};        // last buffered document id
  doc_id_t block_last{doc_limits::invalid()};  // last document id in a block
};

// Buffer for storing positions
struct PosBuffer : SkipBuffer {
  explicit PosBuffer(std::span<uint32_t> buf, uint64_t* skip_ptr) noexcept
    : SkipBuffer{skip_ptr}, buf{buf} {}

  bool Full() const noexcept { return buf.size() == size; }

  void Next(uint32_t pos) noexcept {
    last = pos;
    ++size;
  }

  void Pos(uint32_t pos) noexcept { buf[size] = pos; }

  void Reset() noexcept {
    SkipBuffer::Reset();
    last = 0;
    block_last = 0;
    size = 0;
  }

  std::span<uint32_t> buf;  // buffer to store position deltas
  uint32_t last{};          // last buffered position
  uint32_t block_last{};    // last position in a block
  uint32_t size{};          // number of buffered elements
};

// Buffer for storing payload data
struct PayBuffer : SkipBuffer {
  PayBuffer(uint32_t* offs_start_buf, uint32_t* offs_len_buf,
            uint64_t* skip_ptr) noexcept
    : SkipBuffer{skip_ptr},
      offs_start_buf{offs_start_buf},
      offs_len_buf{offs_len_buf} {}

  void PushOffset(uint32_t i, uint32_t start, uint32_t end) noexcept {
    SDB_ASSERT(start >= last && start <= end);

    offs_start_buf[i] = start - last;
    offs_len_buf[i] = end - start;
    last = start;
  }

  void Reset() noexcept {
    SkipBuffer::Reset();
    last = 0;
  }

  uint32_t* offs_start_buf;  // buffer to store start offsets
  uint32_t* offs_len_buf;    // buffer to store offset lengths
  uint32_t last{};           // last start offset
};

inline std::vector<WandWriter::ptr> PrepareWandWriters(ScorersView scorers,
                                                       size_t max_levels) {
  std::vector<WandWriter::ptr> writers(std::min(scorers.size(), kMaxScorers));
  auto scorer = scorers.begin();
  for (auto& writer : writers) {
    writer = (*scorer)->prepare_wand_writer(max_levels);
    ++scorer;
  }
  return writers;
}

enum class TermsFormat : int32_t {
  Min = 0,
  Max = Min,
};

enum class PostingsFormat : int32_t {
  Min = 0,

  // store competitive scores in blocks
  Wand = Min,

  // store block max scores, sse used
  WandSimd,

  Max = WandSimd,
};

// Assume that doc_count = 28, skip_n = skip_0 = 12
//
//  |       block#0       | |      block#1        | |vInts|
//  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
//                          ^                       ^       (level 0 skip point)
class PostingsWriterBase : public PostingsWriter {
 public:
  static constexpr uint32_t kMaxSkipLevels = 9;
  static constexpr uint32_t kSkipN = 8;

  static constexpr std::string_view kDocFormatName =
    "iresearch_10_postings_documents";
  static constexpr std::string_view kDocExt = "doc";
  static constexpr std::string_view kPosFormatName =
    "iresearch_10_postings_positions";
  static constexpr std::string_view kPosExt = "pos";
  static constexpr std::string_view kPayFormatName =
    "iresearch_10_postings_payloads";
  static constexpr std::string_view kPayExt = "pay";
  static constexpr std::string_view kTermsFormatName =
    "iresearch_10_postings_terms";

 protected:
  PostingsWriterBase(doc_id_t block_size, std::span<doc_id_t> docs,
                     std::span<uint32_t> freqs, doc_id_t* skip_doc,
                     uint64_t* doc_skip_ptr, std::span<uint32_t> prox_buf,
                     uint64_t* prox_skip_ptr, uint32_t* offs_start_buf,
                     uint32_t* offs_len_buf, uint64_t* pay_skip_ptr,
                     uint32_t* enc_buf, PostingsFormat postings_format_version,
                     TermsFormat terms_format_version, IResourceManager& rm)
    : _skip{block_size, kSkipN, rm},
      _doc{docs, freqs, skip_doc, doc_skip_ptr},
      _pos{prox_buf, prox_skip_ptr},
      _pay{offs_start_buf, offs_len_buf, pay_skip_ptr},
      _buf{enc_buf},
      _postings_format_version{postings_format_version},
      _terms_format_version{terms_format_version} {
    SDB_ASSERT(postings_format_version >= PostingsFormat::Min &&
               postings_format_version <= PostingsFormat::Max);
    SDB_ASSERT(terms_format_version >= TermsFormat::Min &&
               terms_format_version <= TermsFormat::Max);
  }

 public:
  void begin_field(const FieldProperties& meta) final {
    _features.Reset(meta.index_features);
    PrepareWriters(meta);
    _docs.clear();
    _last_state.clear();
  }

  FieldStats end_field() noexcept final {
    const auto count = _docs.count();
    SDB_ASSERT(count < doc_limits::eof());
    return {.wand_mask = _writers_mask,
            .docs_count = static_cast<doc_id_t>(count)};
  }

  void begin_block() final {
    // clear state in order to write
    // absolute address of the first
    // entry in the block
    _last_state.clear();
  }

  void prepare(IndexOutput& out, const FlushState& state) final;
  void encode(BufferedOutput& out, const TermMeta& attrs) final;
  void end() final;

 protected:
  class Features {
   public:
    void Reset(IndexFeatures features) noexcept {
      _has_freq = (IndexFeatures::None != (features & IndexFeatures::Freq));
      _has_pos = (IndexFeatures::None != (features & IndexFeatures::Pos));
      _has_offs = (IndexFeatures::None != (features & IndexFeatures::Offs));
    }

    bool HasFrequency() const noexcept { return _has_freq; }
    bool HasPosition() const noexcept { return _has_pos; }
    bool HasOffset() const noexcept { return _has_offs; }

   private:
    bool _has_freq{};
    bool _has_pos{};
    bool _has_offs{};
  };

  struct Attributes final : AttributeProvider {
    DocAttr doc;
    FreqAttr freq_value;

    const FreqAttr* freq{};
    PosAttr* pos{};
    const OffsAttr* offs{};

    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<DocAttr>::id()) {
        return &doc;
      }

      if (type == irs::Type<FreqAttr>::id()) {
        return const_cast<FreqAttr*>(freq);
      }

      return nullptr;
    }

    void Reset(AttributeProvider& attrs) noexcept {
      pos = &PosAttr::empty();
      offs = nullptr;

      freq = irs::get<FreqAttr>(attrs);
      if (freq) {
        if (auto* p = irs::GetMutable<PosAttr>(&attrs)) {
          pos = p;
          offs = irs::get<OffsAttr>(*pos);
        }
      }
    }
  };

  void WriteSkip(size_t level, MemoryIndexOutput& out) const;
  void BeginTerm();
  void EndTerm(TermMetaImpl& meta);
  void EndDocument();
  void PrepareWriters(const FieldProperties& meta);

  template<typename Func>
  void ApplyWriters(Func&& func) {
    for (auto* writer : _valid_writers) {
      func(*writer);
    }
  }

  SkipWriter _skip;
  TermMetaImpl _last_state;   // Last final term state
  bitset _docs;               // Set of all processed documents
  IndexOutput::ptr _doc_out;  // Postings (doc + freq)
  IndexOutput::ptr _pos_out;  // Positions
  IndexOutput::ptr _pay_out;  // Payload (pay + offs)
  DocBuffer _doc;             // Document stream
  PosBuffer _pos;             // Proximity stream
  PayBuffer _pay;             // Payloads and offsets stream
  uint32_t* _buf;             // Buffer for encoding
  Attributes _attrs;          // Set of attributes
  const ColumnProvider* _columns{};
  std::vector<WandWriter::ptr> _writers;    // List of wand writers
  std::vector<WandWriter*> _valid_writers;  // Valid wand writers
  uint64_t _writers_mask{};
  Features _features;  // Features supported by current field
  const PostingsFormat _postings_format_version;
  const TermsFormat _terms_format_version;
};

inline void PostingsWriterBase::PrepareWriters(const FieldProperties& meta) {
  _writers_mask = 0;
  _valid_writers.clear();

  if (!_columns) [[unlikely]] {
    return;
  }

  // Make frequency avaliable for Prepare
  _attrs.freq = _features.HasFrequency() ? &_attrs.freq_value : nullptr;

  for (size_t i = 0; auto& writer : _writers) {
    const bool valid = writer && writer->Prepare(*_columns, meta, _attrs);
    if (valid) {
      SetBit(_writers_mask, i);
      _valid_writers.emplace_back(writer.get());
    }
    ++i;
  }
}

inline void PostingsWriterBase::WriteSkip(size_t level,
                                          MemoryIndexOutput& out) const {
  const doc_id_t doc_delta = _doc.block_last;  //- doc_.skip_doc[level];
  const uint64_t doc_ptr = _doc_out->Position();

  out.WriteV32(doc_delta);
  out.WriteV64(doc_ptr - _doc.skip_ptr[level]);

  _doc.skip_doc[level] = _doc.block_last;
  _doc.skip_ptr[level] = doc_ptr;

  if (_features.HasPosition()) {
    const uint64_t pos_ptr = _pos_out->Position();

    out.WriteV32(_pos.block_last);
    out.WriteV64(pos_ptr - _pos.skip_ptr[level]);

    _pos.skip_ptr[level] = pos_ptr;

    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);

      const uint64_t pay_ptr = _pay_out->Position();

      out.WriteV64(pay_ptr - _pay.skip_ptr[level]);
      _pay.skip_ptr[level] = pay_ptr;
    }
  }
}

inline void PostingsWriterBase::prepare(IndexOutput& out,
                                        const FlushState& state) {
  SDB_ASSERT(state.dir);
  SDB_ASSERT(!IsNull(state.name));

  std::string name;

  // Prepare document stream
  PrepareOutput(name, _doc_out, state, kDocExt, kDocFormatName,
                static_cast<int32_t>(_postings_format_version));

  if (IndexFeatures::None != (state.index_features & IndexFeatures::Pos)) {
    // Prepare proximity stream
    _pos.Reset();
    PrepareOutput(name, _pos_out, state, kPosExt, kPosFormatName,
                  static_cast<int32_t>(_postings_format_version));

    if (IndexFeatures::None != (state.index_features & IndexFeatures::Offs)) {
      // Prepare payload stream
      _pay.Reset();
      PrepareOutput(name, _pay_out, state, kPayExt, kPayFormatName,
                    static_cast<int32_t>(_postings_format_version));
    }
  }

  _skip.Prepare(kMaxSkipLevels, state.doc_count);

  format_utils::WriteHeader(out, kTermsFormatName,
                            static_cast<int32_t>(_terms_format_version));
  out.WriteV32(_skip.Skip0());  // Write postings block size

  // Prepare wand writers
  _writers = PrepareWandWriters(state.scorers, kMaxSkipLevels);
  _valid_writers.reserve(_writers.size());
  _columns = state.columns;

  // Prepare documents bitset
  _docs.reset(doc_limits::min() + state.doc_count);
}

inline void PostingsWriterBase::encode(BufferedOutput& out,
                                       const TermMeta& state) {
  const auto& meta = static_cast<const TermMetaImpl&>(state);

  out.WriteV32(meta.docs_count);
  if (meta.freq) {
    SDB_ASSERT(meta.freq >= meta.docs_count);
    out.WriteV32(meta.freq - meta.docs_count);
  }

  out.WriteV64(meta.doc_start - _last_state.doc_start);
  if (_features.HasPosition()) {
    out.WriteV64(meta.pos_start - _last_state.pos_start);
    if (address_limits::valid(meta.pos_end)) {
      out.WriteV64(meta.pos_end);
    }
    if (_features.HasOffset()) {
      out.WriteV64(meta.pay_start - _last_state.pay_start);
    }
  }

  if (1 == meta.docs_count) {
    out.WriteV32(meta.e_single_doc);
  } else if (_skip.Skip0() < meta.docs_count) {
    out.WriteV64(meta.e_skip_start);
  }

  _last_state = meta;
}

inline void PostingsWriterBase::end() {
  format_utils::WriteFooter(*_doc_out);
  _doc_out.reset();  // ensure stream is closed

  if (_pos_out) {
    format_utils::WriteFooter(*_pos_out);
    _pos_out.reset();  // ensure stream is closed
  }

  if (_pay_out) {
    format_utils::WriteFooter(*_pay_out);
    _pay_out.reset();  // ensure stream is closed
  }
}

inline void PostingsWriterBase::BeginTerm() {
  _doc.start = _doc_out->Position();
  std::fill_n(_doc.skip_ptr, kMaxSkipLevels, _doc.start);
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    _pos.start = _pos_out->Position();
    std::fill_n(_pos.skip_ptr, kMaxSkipLevels, _pos.start);
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      _pay.start = _pay_out->Position();
      std::fill_n(_pay.skip_ptr, kMaxSkipLevels, _pay.start);
    }
  }

  _doc.last = doc_limits::invalid();
  _doc.block_last = doc_limits::invalid();
  _skip.Reset();
}

inline void PostingsWriterBase::EndDocument() {
  if (_doc.Full()) {
    _doc.block_last = _doc.last;
    _doc.end = _doc_out->Position();
    if (_features.HasPosition()) {
      SDB_ASSERT(_pos_out);
      _pos.end = _pos_out->Position();
      // documents stream is full, but positions stream is not
      // save number of positions to skip before the next block
      _pos.block_last = _pos.size;
      if (_features.HasOffset()) {
        SDB_ASSERT(_pay_out);
        _pay.end = _pay_out->Position();
      }
    }

    _doc.doc = _doc.docs.begin();
    _doc.freq = _doc.freqs.begin();
  }
}

inline void PostingsWriterBase::EndTerm(TermMetaImpl& meta) {
  if (meta.docs_count == 0) {
    return;  // no documents to write
  }

  const bool has_skip_list = _skip.Skip0() < meta.docs_count;
  auto write_max_score = [&](size_t level) {
    ApplyWriters([&](auto& writer) {
      const uint8_t size = writer.SizeRoot(level);
      _doc_out->WriteByte(size);
    });
    ApplyWriters([&](auto& writer) { writer.WriteRoot(level, *_doc_out); });
  };

  if (1 == meta.docs_count) {
    meta.e_single_doc = _doc.docs[0] - doc_limits::min();
  } else {
    // write remaining documents using
    // variable length encoding
    auto& out = *_doc_out;
    auto doc = _doc.docs.begin();
    auto prev = _doc.block_last;

    if (!has_skip_list) {
      write_max_score(0);
    }
    // TODO(mbkkt) using bits not full block encoding
    if (_features.HasFrequency()) {
      auto doc_freq = _doc.freqs.begin();
      for (; doc < _doc.doc; ++doc) {
        const uint32_t freq = *doc_freq;
        const doc_id_t delta = *doc - prev;

        if (1 == freq) {
          out.WriteV32(ShiftPack32(delta, true));
        } else {
          out.WriteV32(ShiftPack32(delta, false));
          out.WriteV32(freq);
        }

        ++doc_freq;
        prev = *doc;
      }
    } else {
      for (; doc < _doc.doc; ++doc) {
        out.WriteV32(*doc - prev);
        prev = *doc;
      }
    }
  }

  meta.pos_end = address_limits::invalid();

  // write remaining position using
  // variable length encoding
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);

    if (meta.freq > _skip.Skip0()) {
      meta.pos_end = _pos_out->Position() - _pos.start;
    }

    if (_pos.size > 0) {
      auto& out = *_pos_out;
      uint32_t last_offs_len = std::numeric_limits<uint32_t>::max();
      for (uint32_t i = 0; i < _pos.size; ++i) {
        const uint32_t pos_delta = _pos.buf[i];
        out.WriteV32(pos_delta);

        if (_features.HasOffset()) {
          SDB_ASSERT(_pay_out);

          const uint32_t pay_offs_delta = _pay.offs_start_buf[i];
          const uint32_t len = _pay.offs_len_buf[i];
          if (len == last_offs_len) {
            out.WriteV32(ShiftPack32(pay_offs_delta, false));
          } else {
            out.WriteV32(ShiftPack32(pay_offs_delta, true));
            out.WriteV32(len);
            last_offs_len = len;
          }
        }
      }
    }
  }

  // if we have flushed at least
  // one block there was buffered
  // skip data, so we need to flush it
  if (has_skip_list) {
    meta.e_skip_start = _doc_out->Position() - _doc.start;
    const auto num_levels = _skip.CountLevels();
    write_max_score(num_levels);
    _skip.FlushLevels(num_levels, *_doc_out);
  }

  _doc.doc = _doc.docs.begin();
  _doc.freq = _doc.freqs.begin();
  _doc.last = doc_limits::invalid();
  meta.doc_start = _doc.start;

  if (_pos_out) {
    _pos.size = 0;
    meta.pos_start = _pos.start;
  }

  if (_pay_out) {
    _pay.last = 0;
    meta.pay_start = _pay.start;
  }
}

template<typename FormatTraits>
class PostingsWriterImpl final : public PostingsWriterBase {
 public:
  explicit PostingsWriterImpl(PostingsFormat version, bool volatile_attributes,
                              IResourceManager& rm)
    : PostingsWriterBase{FormatTraits::kBlockSize,
                         std::span{_doc_buf.docs},
                         std::span{_doc_buf.freqs},
                         _doc_buf.skip_doc,
                         _doc_buf.skip_ptr,
                         std::span{_prox_buf.buf},
                         _prox_buf.skip_ptr,
                         _pay_buf.offs_start_buf,
                         _pay_buf.offs_len_buf,
                         _pay_buf.skip_ptr,
                         _encbuf.buf,
                         version,
                         TermsFormat::Max,
                         rm},
      _volatile_attributes{volatile_attributes} {}

  void write(DocIterator& docs, TermMeta& base_meta) final;

 private:
  void AddPosition(uint32_t pos);
  void BeginDocument();

  struct {
    // Buffer for document deltas
    doc_id_t docs[FormatTraits::kBlockSize]{};
    // Buffer for frequencies
    uint32_t freqs[FormatTraits::kBlockSize]{};
    // Buffer for skip documents
    doc_id_t skip_doc[kMaxSkipLevels]{};
    // Buffer for skip pointers
    uint64_t skip_ptr[kMaxSkipLevels]{};
  } _doc_buf;
  struct {
    // Buffer for position deltas
    uint32_t buf[FormatTraits::kBlockSize]{};
    // Buffer for skip pointers
    uint64_t skip_ptr[kMaxSkipLevels]{};
  } _prox_buf;
  struct {
    // Buffer for start offsets
    uint32_t offs_start_buf[FormatTraits::kBlockSize]{};
    // Buffer for offset lengths
    uint32_t offs_len_buf[FormatTraits::kBlockSize]{};
    // Buffer for skip pointers
    uint64_t skip_ptr[kMaxSkipLevels]{};
  } _pay_buf;
  struct {
    // Buffer for encoding (worst case)
    uint32_t buf[FormatTraits::kBlockSize];
  } _encbuf;
  bool _volatile_attributes;
};

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::BeginDocument() {
  if (const auto id = _attrs.doc.value; _doc.last < id) [[likely]] {
    _doc.Push(id, _attrs.freq_value.value);

    if (_doc.Full()) {
      FormatTraits::write_block_delta(*_doc_out, _doc.docs.data(),
                                      _doc.block_last, _buf);
      if (_features.HasFrequency()) {
        FormatTraits::write_block(*_doc_out, _doc.freqs.data(), _buf);
      }
    }

    _docs.set(id);

    // First position offsets now is format dependent
    _pos.last = pos_limits::invalid();
    _pay.last = 0;
  } else {
    throw IndexError{
      absl::StrCat("While beginning document in postings_writer, error: "
                   "docs out of order '",
                   id, "' < '", _doc.last, "'")};
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::AddPosition(uint32_t pos) {
  // at least positions stream should be created
  SDB_ASSERT(_features.HasPosition() && _pos_out);
  SDB_ASSERT(!_attrs.offs || _attrs.offs->start <= _attrs.offs->end);

  _pos.Pos(pos - _pos.last);

  if (_attrs.offs) {
    _pay.PushOffset(_pos.size, _attrs.offs->start, _attrs.offs->end);
  }

  _pos.Next(pos);

  if (_pos.Full()) {
    FormatTraits::write_block(*_pos_out, _pos.buf.data(), _buf);
    _pos.size = 0;

    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      FormatTraits::write_block(*_pay_out, _pay.offs_start_buf, _buf);
      FormatTraits::write_block(*_pay_out, _pay.offs_len_buf, _buf);
    }
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::write(DocIterator& docs,
                                             TermMeta& base_meta) {
  auto refresh = [this, no_freq = FreqAttr{}](auto& attrs) noexcept {
    _attrs.Reset(attrs);
    if (!_attrs.freq) {
      _attrs.freq = &no_freq;
    }
  };

  if (!_volatile_attributes) {
    refresh(docs);
  } else {
    auto* subscription = irs::get<AttrProviderChangeAttr>(docs);
    SDB_ASSERT(subscription);

    subscription->Subscribe(refresh);
  }

  auto& meta = static_cast<TermMetaImpl&>(base_meta);

  BeginTerm();
  ApplyWriters([&](auto& writer) { writer.Reset(); });

  uint32_t docs_count = 0;
  uint32_t total_freq = 0;

  while (true) {
    const auto doc = docs.advance();
    SDB_ASSERT(doc_limits::valid(doc));
    if (doc_limits::eof(doc)) {
      break;
    }
    SDB_ASSERT(_attrs.freq);
    _attrs.doc.value = doc;
    _attrs.freq_value.value = _attrs.freq->value;

    if (doc_limits::valid(_doc.last) && _doc.Empty()) {
      _skip.Skip(docs_count, [this](size_t level, MemoryIndexOutput& out) {
        WriteSkip(level, out);

        // FIXME(gnusi): optimize for 1 writer case? compile? maybe just 1
        // composite wand writer?
        ApplyWriters([&](auto& writer) {
          const uint8_t size = writer.Size(level);
          SDB_ASSERT(size <= WandWriter::kMaxSize);
          out.WriteByte(size);
        });
        ApplyWriters([&](auto& writer) { writer.Write(level, out); });
      });
    }

    BeginDocument();
    ApplyWriters([](auto& writer) { writer.Update(); });
    SDB_ASSERT(_attrs.pos);
    while (_attrs.pos->next()) {
      SDB_ASSERT(pos_limits::valid(_attrs.pos->value()));
      AddPosition(_attrs.pos->value());
    }
    ++docs_count;
    total_freq += _attrs.freq_value.value;
    EndDocument();
  }

  // FIXME(gnusi): do we need to write terminal skip if present?

  meta.docs_count = docs_count;
  meta.freq = total_freq;
  EndTerm(meta);
}

struct SkipState {
  // pointer to the payloads of the first document in a document block
  uint64_t pay_ptr{};
  // pointer to the positions of the first document in a document block
  uint64_t pos_ptr{};
  // positions to skip before new document block
  size_t pend_pos{};
  // pointer to the beginning of document block
  uint64_t doc_ptr{};
  // last document in a previous block
  doc_id_t doc{doc_limits::invalid()};
};

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to, const SkipState& from) noexcept {
  if constexpr (IteratorTraits::Offset()) {
    to = from;
  } else {
    if constexpr (IteratorTraits::Position()) {
      to.pos_ptr = from.pos_ptr;
      to.pend_pos = from.pend_pos;
    }
    to.doc_ptr = from.doc_ptr;
    to.doc = from.doc;
  }
}

template<typename FieldTraits>
IRS_FORCE_INLINE void ReadState(SkipState& state, IndexInput& in) {
  state.doc = in.ReadV32();
  state.doc_ptr += in.ReadV64();

  if constexpr (FieldTraits::Position()) {
    state.pend_pos = in.ReadV32();
    state.pos_ptr += in.ReadV64();

    if constexpr (FieldTraits::Offset()) {
      state.pay_ptr += in.ReadV64();
    }
  }
}

struct DocState {
  const IndexInput* pos_in;
  const IndexInput* pay_in;
  const TermMetaImpl* term_state;
  const uint32_t* freq;
  uint32_t* enc_buf;
  uint64_t tail_start;
  size_t tail_length;
};

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to,
                                const TermMetaImpl& from) noexcept {
  to.doc_ptr = from.doc_start;
  if constexpr (IteratorTraits::Position()) {
    to.pos_ptr = from.pos_start;
    if constexpr (IteratorTraits::Offset()) {
      to.pay_ptr = from.pay_start;
    }
  }
}

template<typename IteratorTraits>
class PositionImpl final : public PosAttr {
 public:
  void Init(bool field_has_offset) { _field_has_offset = field_has_offset; }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (IteratorTraits::Offset()) {
      return irs::Type<OffsAttr>::id() == type ? &_offs : nullptr;
    } else {
      return nullptr;
    }
  }

  value_t seek(value_t target) final {
    const uint32_t freq = *_freq;
    if (_pend_pos > freq) {
      Skip(static_cast<uint32_t>(_pend_pos - freq));
      _pend_pos = freq;
    }
    while (_value < target && _pend_pos) {
      if (_buf_pos == IteratorTraits::kBlockSize) {
        Refill();
        _buf_pos = 0;
      }
      _value += _pos_deltas[_buf_pos];
      SDB_ASSERT(pos_limits::valid(_value));
      ReadAttributes();

      ++_buf_pos;
      --_pend_pos;
    }
    if (0 == _pend_pos && _value < target) {
      _value = pos_limits::eof();
    }
    return _value;
  }

  bool next() final {
    if (0 == _pend_pos) {
      _value = pos_limits::eof();

      return false;
    }

    const uint32_t freq = *_freq;

    if (_pend_pos > freq) {
      Skip(static_cast<uint32_t>(_pend_pos - freq));
      _pend_pos = freq;
    }

    if (_buf_pos == IteratorTraits::kBlockSize) {
      Refill();
      _buf_pos = 0;
    }
    _value += _pos_deltas[_buf_pos];
    SDB_ASSERT(pos_limits::valid(_value));
    ReadAttributes();

    ++_buf_pos;
    --_pend_pos;
    return true;
  }

  void reset() final {
    _value = pos_limits::invalid();
    if (std::numeric_limits<size_t>::max() != _cookie.file_pointer) {
      _buf_pos = IteratorTraits::kBlockSize;
      _pend_pos = _cookie.pend_pos;
      _pos_in->Seek(_cookie.file_pointer);
    }
  }

  // prepares iterator to work
  void Prepare(const DocState& state) {
    _pos_in = state.pos_in->Reopen();  // reopen thread-safe stream

    if (!_pos_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen positions input");

      throw IoError("failed to reopen positions input");
    }

    _cookie.file_pointer = state.term_state->pos_start;
    _pos_in->Seek(state.term_state->pos_start);
    _freq = state.freq;
    _enc_buf = state.enc_buf;
    _tail_start = state.tail_start;
    _tail_length = state.tail_length;

    if constexpr (IteratorTraits::Offset()) {
      _pay_in = state.pay_in->Reopen();  // reopen thread-safe stream

      if (!_pay_in) {
        // implementation returned wrong pointer
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen payload input");

        throw IoError("failed to reopen payload input");
      }

      _pay_in->Seek(state.term_state->pay_start);
    }
  }

  // notifies iterator that doc iterator has skipped to a new block
  void Prepare(const SkipState& state) {
    _pos_in->Seek(state.pos_ptr);
    _pend_pos = state.pend_pos;
    _buf_pos = IteratorTraits::kBlockSize;
    _cookie.file_pointer = state.pos_ptr;
    _cookie.pend_pos = _pend_pos;

    if constexpr (IteratorTraits::Offset()) {
      _pay_in->Seek(state.pay_ptr);
    }
  }

  // notify iterator that corresponding DocIterator has moved forward
  void Notify(uint32_t n) {
    _pend_pos += n;
    _cookie.pend_pos += n;
  }

  void Clear() noexcept {
    _value = pos_limits::invalid();
    ClearAttributes();
  }

 private:
  IRS_FORCE_INLINE void Refill() {
    if (_pos_in->Position() != _tail_start) {
      ReadBlock();
    } else {
      ReadTailBlock();
    }
  }

  void Skip(uint32_t count) {
    auto left = IteratorTraits::kBlockSize - _buf_pos;
    if (count >= left) {
      count -= left;
      while (count >= IteratorTraits::kBlockSize) {
        SkipBlock();
        count -= IteratorTraits::kBlockSize;
      }
      Refill();
      _buf_pos = 0;
      left = IteratorTraits::kBlockSize;
    }

    if (count < left) {
      _buf_pos += count;
    }
    Clear();
  }

  void ReadAttributes() noexcept {
    if constexpr (IteratorTraits::Offset()) {
      _offs.start += _offs_start_deltas[_buf_pos];
      _offs.end = _offs.start + _offs_lengths[_buf_pos];
    }
  }

  void ClearAttributes() noexcept {
    if constexpr (IteratorTraits::Offset()) {
      _offs.clear();
    }
  }

  void ReadBlock() {
    IteratorTraits::read_block(*_pos_in, _enc_buf, _pos_deltas);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::read_block(*_pay_in, _enc_buf, _offs_start_deltas);
      IteratorTraits::read_block(*_pay_in, _enc_buf, _offs_lengths);
    }
  }

  void ReadTailBlock() {
    for (uint16_t i = 0; i < _tail_length; ++i) {
      _pos_deltas[i] = _pos_in->ReadV32();

      if constexpr (IteratorTraits::Offset()) {
        if (ShiftUnpack32(_pos_in->ReadV32(), _offs_start_deltas[i])) {
          _offs_lengths[i] = _pos_in->ReadV32();
        } else {
          SDB_ASSERT(i > 0);
          _offs_lengths[i] = _offs_lengths[i - 1];
        }
      } else if (_field_has_offset) {
        uint32_t delta;
        if (ShiftUnpack32(_pos_in->ReadV32(), delta)) {
          _pos_in->ReadV32();
        }
      }
    }
  }

  void SkipBlock() {
    IteratorTraits::skip_block(*_pos_in);
    if constexpr (IteratorTraits::Offset()) {
      IteratorTraits::skip_block(*_pay_in);
      IteratorTraits::skip_block(*_pay_in);
    }
  }

  struct Cookie {
    uint64_t pend_pos{};
    uint64_t file_pointer = std::numeric_limits<uint64_t>::max();
  };

  template<typename T>
  using ForOffset = utils::Need<IteratorTraits::Offset(), T>;

  uint32_t _pos_deltas[IteratorTraits::kBlockSize];
  [[no_unique_address]] ForOffset<uint32_t[IteratorTraits::kBlockSize]>
    _offs_start_deltas;
  [[no_unique_address]] ForOffset<uint32_t[IteratorTraits::kBlockSize]>
    _offs_lengths;
  const uint32_t* _freq;   // lenght of the posting list for a document
  uint32_t* _enc_buf;      // auxillary buffer to decode data
  uint64_t _pend_pos = 0;  // how many positions "behind" we are
  uint64_t _tail_start;    // file pointer where the last pos delta block is
  bool _field_has_offset = false;
  uint16_t _tail_length;  // number of positions in the last pos delta block
  uint16_t _buf_pos = IteratorTraits::kBlockSize;  // position in pos_deltas_
  Cookie _cookie;
  IndexInput::ptr _pos_in;
  [[no_unique_address]] ForOffset<IndexInput::ptr> _pay_in;
  [[no_unique_address]] ForOffset<OffsAttr> _offs;
};

template<typename IteratorTraits>
using AttributesImpl = std::conditional_t<
  IteratorTraits::Position(),
  std::tuple<DocAttr, FreqAttr, FreqBlockAttr, CostAttr,
             PositionImpl<IteratorTraits>>,
  std::conditional_t<IteratorTraits::Frequency(),
                     std::tuple<DocAttr, FreqAttr, FreqBlockAttr, CostAttr>,
                     std::tuple<DocAttr, CostAttr>>>;

template<typename IteratorTraits>
class PostingIteratorBase : public DocIterator {
 public:
  static_assert(IteratorTraits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

  ~PostingIteratorBase() {
    if constexpr (IteratorTraits::Frequency()) {
      if (_doc_in) {
        std::allocator<uint32_t>{}.deallocate(_collected_freqs, kScoreBlock);
      }
    }
  }

  IRS_NO_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  IRS_FORCE_INLINE doc_id_t advance() final;

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) final;

  uint32_t count() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    doc_value = doc_limits::eof();
    const auto left_in_leaf = std::exchange(_left_in_leaf, 0);
    const auto left_in_list = std::exchange(_left_in_list, 0);
    return left_in_leaf + left_in_list;
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask, CollectScoreContext score,
                                      CollectMatchContext match) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (!score.score || score.score->IsDefault()) {
      score.merge_type = ScoreMergeType::Noop;
    }

    auto process_score = [&](size_t left_in_leaf) IRS_FORCE_INLINE {
      SDB_ASSERT(score.merge_type != ScoreMergeType::Noop);
      if (score.collector) {
        score.collector->Collect(
          std::span{std::end(this->_docs) - left_in_leaf, left_in_leaf});
      }
      if constexpr (IteratorTraits::Frequency()) {
        std::get<FreqBlockAttr>(_attrs).value =
          std::end(this->_freqs) - left_in_leaf;
      }
      score.score->Score(
        reinterpret_cast<score_t*>(std::end(this->_enc_buf) - left_in_leaf),
        left_in_leaf);
    };

    return ResolveBool(match.matches != nullptr, [&]<bool TrackMatch> {
      return ResolveMergeType(score.merge_type, [&]<ScoreMergeType MergeType> {
        bool empty = true;

        const doc_id_t base = min;
        auto* IRS_RESTRICT const doc_mask = mask;
        [[maybe_unused]] auto* IRS_RESTRICT const score_window =
          score.score_window;

        auto process_doc =
          [&](doc_id_t doc, [[maybe_unused]] score_t score_val)
            IRS_FORCE_INLINE {
              doc -= base;
              if constexpr (TrackMatch) {
                SDB_ASSERT(match.matches);
                const bool has_match =
                  ++match.matches[doc] >= match.min_match_count;
                empty &= !has_match;
                if (has_match) {
                  SetBit(doc_mask[doc / BitsRequired<uint64_t>()],
                         doc % BitsRequired<uint64_t>(), has_match);

                  if constexpr (MergeType != ScoreMergeType::Noop) {
                    Merge<MergeType>(score_window[doc], score_val);
                  }
                }
              } else {
                SetBit(doc_mask[doc / BitsRequired<uint64_t>()],
                       doc % BitsRequired<uint64_t>());
                empty = false;
                if constexpr (MergeType != ScoreMergeType::Noop) {
                  Merge<MergeType>(score_window[doc], score_val);
                }
              }
            };

        [[maybe_unused]] const auto* score_end =
          std::bit_cast<score_t*>(std::end(this->_enc_buf));
        [[maybe_unused]] const auto* score_ptr =
          score_end - this->_left_in_leaf;

        if (!this->_doc_in) [[unlikely]] {
          SDB_ASSERT(this->_left_in_list == 0);
          if (this->_left_in_leaf == 0) {
            return std::pair{doc_value = doc_limits::eof(), true};
          }

          doc_value = *(std::end(this->_docs) - 1);

          if (doc_value >= max) {
            return std::pair{doc_value, true};
          }

          if constexpr (MergeType != ScoreMergeType::Noop) {
            process_score(1);
          }
          process_doc(doc_value, *score_ptr);
          this->_left_in_leaf = 0;
          return std::pair{doc_value, empty};
        }

        const auto* const doc_end = std::end(this->_docs);
        const auto* doc_ptr = doc_end - this->_left_in_leaf;
        doc_id_t doc = doc_value;

        while (true) {
          if (doc_ptr == doc_end) [[unlikely]] {
            if (this->_left_in_list == 0) [[unlikely]] {
              doc = doc_limits::eof();
              break;
            }

            this->_left_in_leaf = 0;
            this->Refill(doc);
            doc_ptr = doc_end - this->_left_in_leaf;
            score_ptr = score_end - this->_left_in_leaf;
            if constexpr (MergeType != ScoreMergeType::Noop) {
              process_score(this->_left_in_leaf);
            }
          }

          doc = *doc_ptr;

          if (doc >= max) {
            break;
          }

          process_doc(doc, *score_ptr);
          ++doc_ptr;
          ++score_ptr;
        }

        doc_value = doc;
        this->_left_in_leaf = static_cast<uint32_t>(doc_end - doc_ptr);

        if constexpr (IteratorTraits::Frequency()) {
          std::get<FreqBlockAttr>(_attrs).value = this->_collected_freqs;
        }
        return std::pair{doc_value, empty};
      });
    });
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = this->_field,
      .doc_attrs = *this,
      .collector = ctx.collector,
      .stats = this->_stats,
      .boost = this->_boost,
    });
  }

  uint32_t Collect(const ScoreFunction& scorer, ColumnCollector& columns,
                   std::span<doc_id_t, kScoreBlock> docs,
                   std::span<score_t, kScoreBlock> scores) final {
    // TODO(gnusi): optimize
    SDB_ASSERT(kScoreBlock <= docs.size());
    return DocIterator::Collect(*this, scorer, columns, docs, scores);
  }

  void FetchScoreArgs(uint16_t index) final {
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(this->_collected_freqs);
      this->_collected_freqs[index] = std::get<FreqAttr>(this->_attrs).value;
    }
  }

  void Init(const PostingCookie& cookie) noexcept {
    this->_field = cookie.field;
    this->_stats = cookie.stats;
    this->_boost = cookie.boost;
  }

 protected:
  using Attributes = AttributesImpl<IteratorTraits>;
  using Position = PositionImpl<IteratorTraits>;

  PostingIteratorBase([[maybe_unused]] bool field_has_offset) {
    if constexpr (IteratorTraits::Position()) {
      std::get<Position>(_attrs).Init(field_has_offset);
    }
  }

  virtual void Refill(doc_id_t prev_doc) = 0;
  virtual void SeekToBlock(doc_id_t target) = 0;

  FieldProperties _field;
  const byte_type* _stats = nullptr;
  score_t _boost = kNoBoost;
  uint32_t _enc_buf[IteratorTraits::kBlockSize];  // buffer for encoding
  [[no_unique_address]] utils::Need<IteratorTraits::Frequency(), uint32_t*>
    _collected_freqs;
  [[no_unique_address]] utils::Need<
    IteratorTraits::Frequency(), uint32_t[IteratorTraits::kBlockSize]> _freqs;
  doc_id_t _docs[IteratorTraits::kBlockSize];
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  bool _field_has_frequency;
  IndexInput::ptr _doc_in;
  Attributes _attrs;
};

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::advance() {
  auto& doc_value = std::get<DocAttr>(_attrs).value;

  if (_left_in_leaf == 0) [[unlikely]] {
    if (_left_in_list == 0) [[unlikely]] {
      return doc_value = doc_limits::eof();
    }

    Refill(doc_value);
  }

  doc_value = *(std::end(_docs) - _left_in_leaf);

  if constexpr (IteratorTraits::Frequency()) {
    auto& freq_value = std::get<FreqAttr>(_attrs).value;
    freq_value = *(std::end(_freqs) - _left_in_leaf);

    if constexpr (IteratorTraits::Position()) {
      auto& pos = std::get<Position>(_attrs);
      pos.Notify(freq_value);
      pos.Clear();
    }
  }

  --_left_in_leaf;
  return doc_value;
}

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::seek(doc_id_t target) {
  auto& doc_value = std::get<DocAttr>(_attrs).value;

  if (target <= doc_value) [[unlikely]] {
    return doc_value;
  }

  if (_max_in_leaf < target) [[unlikely]] {
    if (!IteratorTraits::Position() &&
        target - _max_in_leaf <= IteratorTraits::kBlockSize) {
      doc_value = _max_in_leaf;
    } else {
      SeekToBlock(target);
    }

    if (_left_in_list == 0) [[unlikely]] {
      _left_in_leaf = 0;
      return doc_value = doc_limits::eof();
    }

    Refill(doc_value);
  }

  [[maybe_unused]] uint32_t notify = 0;

  while (_left_in_leaf != 0) {
    const auto doc = *(std::end(_docs) - _left_in_leaf);

    if constexpr (IteratorTraits::Position()) {
      notify += *(std::end(_freqs) - _left_in_leaf);
    }

    --_left_in_leaf;

    if (target <= doc) {
      if constexpr (IteratorTraits::Frequency()) {
        auto& freq_value = std::get<FreqAttr>(_attrs).value;
        freq_value = *(std::end(_freqs) - (_left_in_leaf + 1));
      }

      if constexpr (IteratorTraits::Position()) {
        auto& pos = std::get<Position>(_attrs);
        pos.Notify(notify);
        pos.Clear();
      }

      return doc_value = doc;
    }
  }

  return doc_value = doc_limits::eof();
}

static_assert(kMaxScorers < WandContext::kDisable);

template<uint8_t Value>
struct Extent {
  static constexpr uint8_t GetExtent() noexcept { return Value; }
};

template<>
struct Extent<WandContext::kDisable> {
  Extent(uint8_t value) noexcept : value{value} {}

  constexpr uint8_t GetExtent() const noexcept { return value; }

  uint8_t value;
};

using DynamicExtent = Extent<WandContext::kDisable>;

template<uint8_t PossibleMin, typename Func>
auto ResolveExtent(uint8_t extent, Func&& func) {
  if constexpr (PossibleMin == WandContext::kDisable) {
    return std::forward<Func>(func)(Extent<0>{});
  } else {
    switch (extent) {
      case 1:
        return std::forward<Func>(func)(Extent<1>{});
      case 0:
        if constexpr (PossibleMin <= 0) {
          return std::forward<Func>(func)(Extent<0>{});
        }
        [[fallthrough]];
      default:
        return std::forward<Func>(func)(DynamicExtent{extent});
    }
  }
}

// TODO(mbkkt) Make it overloads
// Remove to many Readers implementations

template<typename WandExtent>
void CommonSkipWandData(WandExtent extent, IndexInput& in) {
  switch (auto count = extent.GetExtent(); count) {
    case 0:
      return;
    case 1:
      in.Skip(in.ReadByte());
      return;
    default: {
      uint64_t skip{};
      for (; count; --count) {
        skip += in.ReadByte();
      }
      in.Skip(skip);
      return;
    }
  }
}

template<typename WandExtent>
void CommonReadWandData(WandExtent wextent, uint8_t index,
                        const ScoreFunction& func, WandSource& ctx,
                        IndexInput& in, score_t& score) {
  const auto extent = wextent.GetExtent();
  SDB_ASSERT(extent);
  SDB_ASSERT(index < extent);
  if (extent == 1) [[likely]] {
    const auto size = in.ReadByte();
    ctx.Read(in, size);
    func.Score(&score);
    return;
  }

  uint8_t i = 0;
  uint64_t scorer_offset = 0;
  for (; i < index; ++i) {
    scorer_offset += in.ReadByte();
  }
  const auto size = in.ReadByte();
  ++i;
  uint64_t block_offset = 0;
  for (; i < extent; ++i) {
    block_offset += in.ReadByte();
  }

  if (scorer_offset) {
    in.Skip(scorer_offset);
  }
  ctx.Read(in, size);
  func.Score(&score);
  if (block_offset) {
    in.Skip(block_offset);
  }
}

// Iterator over posting list.
// IteratorTraits defines requested features.
// FieldTraits defines requested features.
template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
class PostingIteratorImpl : public PostingIteratorBase<IteratorTraits> {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());

  using Base = PostingIteratorBase<IteratorTraits>;
  using typename Base::Position;

  static_assert(IteratorTraits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

 public:
  PostingIteratorImpl(WandExtent extent)
    : Base{FieldTraits::Offset()},
      _skip{IteratorTraits::kBlockSize, PostingsWriterBase::kSkipN,
            ReadSkip{extent}} {}

  void Prepare(const PostingCookie& meta, const IndexInput* doc_in,
               const IndexInput* pos_in, const IndexInput* pay_in,
               uint8_t wand_index = WandContext::kDisable);

 private:
  class ReadSkip : private WandExtent {
   public:
    explicit ReadSkip(WandExtent extent) : WandExtent{extent}, _skip_levels(1) {
      Disable();  // Prevent using skip-list by default
    }

    void ReadMaxScore(uint8_t index, const ScoreFunction& func, WandSource& ctx,
                      IndexInput& in, score_t& score) {
      CommonReadWandData(static_cast<WandExtent>(*this), index, func, ctx, in,
                         score);
    }

    void Disable() noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      SDB_ASSERT(!doc_limits::valid(_skip_levels.back().doc));
      _skip_levels.back().doc = doc_limits::eof();
    }

    void Enable(const TermMetaImpl& state) noexcept {
      SDB_ASSERT(state.docs_count > IteratorTraits::kBlockSize);

      // Since we store pointer deltas, add postings offset
      auto& top = _skip_levels.front();
      CopyState<IteratorTraits>(top, state);
      Enable();
    }

    void Init(size_t num_levels) {
      SDB_ASSERT(num_levels);
      _skip_levels.resize(num_levels);
    }

    bool IsLess(size_t level, doc_id_t target) const noexcept {
      return _skip_levels[level].doc < target;
    }

    void MoveDown(size_t level) noexcept {
      auto& next = _skip_levels[level];
      // Move to the more granular level
      SDB_ASSERT(_prev);
      CopyState<IteratorTraits>(next, *_prev);
    }

    void Read(size_t level, IndexInput& in);
    void Seal(size_t level);
    size_t AdjustLevel(size_t level) const noexcept { return level; }

    void Reset(SkipState& state) noexcept {
      SDB_ASSERT(absl::c_is_sorted(
        _skip_levels,
        [](const auto& lhs, const auto& rhs) { return lhs.doc > rhs.doc; }));

      _prev = &state;
    }

    doc_id_t UpperBound() const noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      return _skip_levels.back().doc;
    }

    void SkipWandData(IndexInput& in) {
      CommonSkipWandData(static_cast<WandExtent>(*this), in);
    }

   private:
    void Enable() noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      SDB_ASSERT(doc_limits::eof(_skip_levels.back().doc));
      _skip_levels.back().doc = doc_limits::invalid();
    }

    std::vector<SkipState> _skip_levels;
    SkipState* _prev{};  // Pointer to skip context used by skip reader
  };

  IRS_FORCE_INLINE void Refill(doc_id_t prev_doc) final;
  IRS_FORCE_INLINE void ReadBlock(doc_id_t prev_doc);
  IRS_FORCE_INLINE void ReadTailBlock(doc_id_t prev_doc);
  IRS_FORCE_INLINE void SeekToBlock(doc_id_t target) final;

  uint64_t _skip_offs{};
  SkipReader<ReadSkip> _skip;
  uint32_t _docs_count{};
};

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits,
                         WandExtent>::ReadSkip::Read(size_t level,
                                                     IndexInput& in) {
  auto& next = _skip_levels[level];

  // Store previous step on the same level
  CopyState<IteratorTraits>(*_prev, next);

  ReadState<FieldTraits>(next, in);

  SkipWandData(in);
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits,
                         WandExtent>::ReadSkip::Seal(size_t level) {
  auto& next = _skip_levels[level];

  // Store previous step on the same level
  CopyState<IteratorTraits>(*_prev, next);

  // Stream exhausted
  next.doc = doc_limits::eof();
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::Prepare(
  const PostingCookie& meta, const IndexInput* doc_in, const IndexInput* pos_in,
  const IndexInput* pay_in, uint8_t wand_index) {
  this->Init(meta);

  auto& term_state = sdb::basics::downCast<CookieImpl>(meta.cookie)->meta;
  std::get<CostAttr>(this->_attrs)
    .reset(term_state.docs_count);  // Estimate iterator

  if (term_state.docs_count > 1) {
    this->_left_in_list = term_state.docs_count;
    SDB_ASSERT(this->_left_in_leaf == 0);
    SDB_ASSERT(this->_max_in_leaf == doc_limits::invalid());

    if (!this->_doc_in) {
      this->_doc_in = doc_in->Reopen();  // Reopen thread-safe stream

      if (!this->_doc_in) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen document input");
        throw IoError("failed to reopen document input");
      }
    }

    if constexpr (IteratorTraits::Frequency()) {
      auto& freq_block = std::get<FreqBlockAttr>(this->_attrs);
      this->_collected_freqs = std::allocator<uint32_t>{}.allocate(kScoreBlock);
      freq_block.value = this->_collected_freqs;
    }

    this->_doc_in->Seek(term_state.doc_start);
    SDB_ASSERT(!this->_doc_in->IsEOF());
  } else {
    SDB_ASSERT(term_state.docs_count == 1);
    auto* doc = std::end(this->_docs) - 1;
    *doc = doc_limits::min() + term_state.e_single_doc;
    if constexpr (IteratorTraits::Frequency()) {
      auto* freq = std::end(this->_freqs) - 1;
      *freq = term_state.freq;

      this->_collected_freqs = freq;

      auto& freq_block = std::get<FreqBlockAttr>(this->_attrs);
      freq_block.value = freq;
    }
    this->_left_in_list = 0;
    this->_left_in_leaf = 1;
    this->_max_in_leaf = *doc;
  }

  SDB_ASSERT(!IteratorTraits::Frequency() || term_state.freq);
  if constexpr (IteratorTraits::Position()) {
    static_assert(IteratorTraits::Frequency());
    const auto term_freq = term_state.freq;

    const auto tail_start = [&] noexcept {
      if (term_freq < IteratorTraits::kBlockSize) {
        return term_state.pos_start;
      } else if (term_freq == IteratorTraits::kBlockSize) {
        return address_limits::invalid();
      } else {
        return term_state.pos_start + term_state.pos_end;
      }
    }();

    const DocState state{
      .pos_in = pos_in,
      .pay_in = pay_in,
      .term_state = &term_state,
      .freq = &std::get<FreqAttr>(this->_attrs).value,
      .enc_buf = this->_enc_buf,
      .tail_start = tail_start,
      .tail_length = term_freq % IteratorTraits::kBlockSize,
    };

    std::get<Position>(this->_attrs).Prepare(state);
  }

  if (term_state.docs_count > IteratorTraits::kBlockSize) {
    // Allow using skip-list for long enough postings
    _skip.Reader().Enable(term_state);
    _skip_offs = term_state.doc_start + term_state.e_skip_start;
  } else if (1 < term_state.docs_count &&
             term_state.docs_count < IteratorTraits::kBlockSize &&
             wand_index == WandContext::kDisable) {
    _skip.Reader().SkipWandData(*this->_doc_in);
  }
  _docs_count = term_state.docs_count;
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::Refill(
  doc_id_t prev_doc) {
  if (this->_left_in_list >= IteratorTraits::kBlockSize) [[likely]] {
    ReadBlock(prev_doc);
  } else {
    ReadTailBlock(prev_doc);
  }
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::ReadBlock(
  doc_id_t prev_doc) {
  IteratorTraits::read_block_delta(*this->_doc_in, this->_enc_buf, this->_docs,
                                   prev_doc);
  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = IteratorTraits::kBlockSize;
  this->_left_in_list -= IteratorTraits::kBlockSize;
  if constexpr (IteratorTraits::Frequency()) {
    IteratorTraits::read_block(*this->_doc_in, this->_enc_buf, this->_freqs);
  } else if (FieldTraits::Frequency()) {
    IteratorTraits::skip_block(*this->_doc_in);
  }
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits,
                         WandExtent>::ReadTailBlock(doc_id_t prev_doc) {
  auto* doc = std::end(this->_docs) - this->_left_in_list;

  [[maybe_unused]] uint32_t* freq;
  if constexpr (IteratorTraits::Frequency()) {
    freq = std::end(this->_freqs) - this->_left_in_list;
  }

  while (doc < std::end(this->_docs)) {
    if constexpr (IteratorTraits::Frequency()) {
      if (ShiftUnpack32(this->_doc_in->ReadV32(), *doc)) {
        *freq++ = 1;
      } else {
        *freq++ = this->_doc_in->ReadV32();
      }
    } else if (FieldTraits::Frequency()) {
      if (!ShiftUnpack32(this->_doc_in->ReadV32(), *doc)) {
        this->_doc_in->ReadV32();
      }
    } else {
      *doc = this->_doc_in->ReadV32();
    }
    const auto curr_doc = prev_doc + *doc;
    *doc++ = curr_doc;
    prev_doc = curr_doc;
  }
  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = this->_left_in_list;
  this->_left_in_list = 0;
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void PostingIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::SeekToBlock(
  doc_id_t target) {
  SkipState last;  // Where block starts
  _skip.Reader().Reset(last);

  // Init skip writer in lazy fashion
  if (_docs_count == 0) [[likely]] {
  seek_after_initialization:
    SDB_ASSERT(target > _skip.Reader().UpperBound());
    SDB_ASSERT(_skip.NumLevels());

    this->_left_in_list = _skip.Seek(target);
    // unnecessary: this->_left_in_leaf = 0;
    // unnecessary: this->_max_in_leaf = _skip.Reader().UpperBound();
    std::get<DocAttr>(this->_attrs).value = last.doc;

    this->_doc_in->Seek(last.doc_ptr);
    if constexpr (IteratorTraits::Position()) {
      auto& pos = std::get<Position>(this->_attrs);
      pos.Prepare(last);  // Notify positions
    }
    return;
  }

  // needed for short postings lists
  if (target <= _skip.Reader().UpperBound()) [[unlikely]] {
    return;
  }

  auto skip_in = this->_doc_in->Dup();

  if (!skip_in) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Failed to duplicate document input");

    throw IoError("Failed to duplicate document input");
  }

  SDB_ASSERT(!_skip.NumLevels());
  skip_in->Seek(_skip_offs);
  _skip.Reader().SkipWandData(*skip_in);
  _skip.Prepare(std::move(skip_in), _docs_count);
  _docs_count = 0;

  // initialize skip levels
  if (const auto num_levels = _skip.NumLevels();
      num_levels > 0 && num_levels <= PostingsWriterBase::kMaxSkipLevels)
    [[likely]] {
    SDB_ASSERT(!doc_limits::valid(_skip.Reader().UpperBound()));
    _skip.Reader().Init(num_levels);

    goto seek_after_initialization;
  } else {
    SDB_ASSERT(false);
    throw IndexError{absl::StrCat("Invalid number of skip levels ", num_levels,
                                  ", must be in range of [1, ",
                                  PostingsWriterBase::kMaxSkipLevels, "].")};
  }
}

struct IndexMetaWriterImpl final : public IndexMetaWriter {
  static constexpr std::string_view kFormatName = "iresearch_10_index_meta";
  static constexpr std::string_view kFormatPrefix = "segments_";
  static constexpr std::string_view kFormatPrefixTmp = "pending_segments_";

  static constexpr int32_t kFormatMin = 0;
  static constexpr int32_t kFormatMax = 1;

  enum { kHasPayload = 1 };

  static std::string FileName(uint64_t gen) {
    return FileName(kFormatPrefix, gen);
  }

  explicit IndexMetaWriterImpl(int32_t version) noexcept : _version{version} {
    SDB_ASSERT(_version >= kFormatMin && version <= kFormatMax);
  }

  // FIXME(gnusi): Better to split prepare into 2 methods and pass meta by
  // const reference
  bool prepare(Directory& dir, IndexMeta& meta, std::string& pending_filename,
               std::string& filename) final;
  bool commit() final;
  void rollback() noexcept final;

 private:
  static std::string FileName(std::string_view prefix, uint64_t gen) {
    SDB_ASSERT(index_gen_limits::valid(gen));
    return irs::FileName(prefix, gen);
  }

  static std::string PendingFileName(uint64_t gen) {
    return FileName(kFormatPrefixTmp, gen);
  }

  Directory* _dir{};
  uint64_t _pending_gen{index_gen_limits::invalid()};  // Generation to commit
  int32_t _version;
};

inline bool IndexMetaWriterImpl::prepare(Directory& dir, IndexMeta& meta,
                                         std::string& pending_filename,
                                         std::string& filename) {
  if (index_gen_limits::valid(_pending_gen)) {
    // prepare() was already called with no corresponding call to commit()
    return false;
  }

  ++meta.gen;  // Increment generation before generating filename
  pending_filename = PendingFileName(meta.gen);
  filename = FileName(meta.gen);

  auto out = dir.create(pending_filename);

  if (!out) {
    throw IoError{
      absl::StrCat("Failed to create file, path: ", pending_filename)};
  }

  {
    format_utils::WriteHeader(*out, kFormatName, _version);
    out->WriteV64(meta.gen);
    out->WriteU64(meta.seg_counter);
    SDB_ASSERT(meta.segments.size() <= std::numeric_limits<uint32_t>::max());
    out->WriteV32(static_cast<uint32_t>(meta.segments.size()));

    for (const auto& segment : meta.segments) {
      WriteStr(*out, segment.filename);
      WriteStr(*out, segment.meta.codec->type()().name());
    }

    if (_version > kFormatMin) {
      const auto payload = GetPayload(meta);
      const uint8_t flags = IsNull(payload) ? 0 : kHasPayload;
      out->WriteByte(flags);

      if (flags == kHasPayload) {
        WriteStr(*out, payload);
      }
    } else {
      // Earlier versions don't support payload.
      meta.payload.reset();
    }

    format_utils::WriteFooter(*out);
  }  // Important to close output here

  // Only noexcept operations below
  _dir = &dir;
  _pending_gen = meta.gen;

  return true;
}

inline bool IndexMetaWriterImpl::commit() {
  if (!index_gen_limits::valid(_pending_gen)) {
    return false;
  }

  const auto src = PendingFileName(_pending_gen);
  const auto dst = FileName(_pending_gen);

  if (!_dir->rename(src, dst)) {
    rollback();

    throw IoError{absl::StrCat("Failed to rename file, src path: '", src,
                               "' dst path: '", dst, "'")};
  }

  // only noexcept operations below
  // clear pending state
  _pending_gen = index_gen_limits::invalid();
  _dir = nullptr;

  return true;
}

inline void IndexMetaWriterImpl::rollback() noexcept {
  if (!index_gen_limits::valid(_pending_gen)) {
    return;
  }

  std::string seg_file;

  try {
    seg_file = PendingFileName(_pending_gen);
  } catch (const std::exception& e) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat(
        "Caught error while generating file name for index meta, reason: ",
        e.what()));
    return;
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while generating file name for index meta");
    return;
  }

  if (!_dir->remove(seg_file)) {  // suppress all errors
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to remove file, path: ", seg_file));
  }

  // clear pending state
  _dir = nullptr;
  _pending_gen = index_gen_limits::invalid();
}

inline uint64_t ParseGeneration(std::string_view file) noexcept {
  if (file.starts_with(IndexMetaWriterImpl::kFormatPrefix)) {
    constexpr size_t kPrefixLength = IndexMetaWriterImpl::kFormatPrefix.size();

    if (uint64_t gen; absl::SimpleAtoi(file.substr(kPrefixLength), &gen)) {
      return gen;
    }
  }

  return index_gen_limits::invalid();
}

struct IndexMetaReaderImpl : public IndexMetaReader {
  bool last_segments_file(const Directory& dir, std::string& name) const final;

  void read(const Directory& dir, IndexMeta& meta,
            std::string_view filename) final;
};

inline bool IndexMetaReaderImpl::last_segments_file(const Directory& dir,
                                                    std::string& out) const {
  uint64_t max_gen = index_gen_limits::invalid();
  Directory::visitor_f visitor = [&out, &max_gen](std::string_view name) {
    const uint64_t gen = ParseGeneration(name);

    if (gen > max_gen) {
      out = std::move(name);
      max_gen = gen;
    }
    return true;  // continue iteration
  };

  dir.visit(visitor);
  return index_gen_limits::valid(max_gen);
}

inline void IndexMetaReaderImpl::read(const Directory& dir, IndexMeta& meta,
                                      std::string_view filename) {
  std::string meta_file;
  if (IsNull(filename)) {
    meta_file = IndexMetaWriterImpl::FileName(meta.gen);
    filename = meta_file;
  }

  auto in = dir.open(filename, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  const auto checksum = format_utils::Checksum(*in);

  // check header
  const int32_t version = format_utils::CheckHeader(
    *in, IndexMetaWriterImpl::kFormatName, IndexMetaWriterImpl::kFormatMin,
    IndexMetaWriterImpl::kFormatMax);

  // read data from segments file
  auto gen = in->ReadV64();
  auto cnt = in->ReadI64();
  auto seg_count = in->ReadV32();
  std::vector<IndexSegment> segments(seg_count);

  for (size_t i = 0, count = segments.size(); i < count; ++i) {
    auto& segment = segments[i];

    segment.filename = ReadString<std::string>(*in);
    segment.meta.codec = formats::Get(ReadString<std::string>(*in));

    auto reader = segment.meta.codec->get_segment_meta_reader();

    reader->read(dir, segment.meta, segment.filename);
  }

  bool has_payload = false;
  bstring payload;
  if (version > IndexMetaWriterImpl::kFormatMin) {
    has_payload = (in->ReadByte() & IndexMetaWriterImpl::kHasPayload);

    if (has_payload) {
      payload = ReadString<bstring>(*in);
    }
  }

  format_utils::CheckFooter(*in, checksum);

  meta.gen = gen;
  meta.seg_counter = cnt;
  meta.segments = std::move(segments);
  if (has_payload) {
    meta.payload = std::move(payload);
  } else {
    meta.payload.reset();
  }
}

inline uint64_t WriteDocumentMask(IndexOutput& out, const auto& docs_mask) {
  // TODO(gnusi): better format
  uint32_t mask_size = docs_mask ? static_cast<uint32_t>(docs_mask->size()) : 0;
  SDB_ASSERT(mask_size < doc_limits::eof());

  if (!mask_size) {
    out.WriteV32(0);
    return 0;
  }

  out.WriteV32(mask_size);
  const auto pos = out.Position();
  for (auto mask : *docs_mask) {
    out.WriteV32(mask);
  }
  return out.Position() - pos;
}

inline std::pair<const std::shared_ptr<DocumentMask>, uint64_t>
ReadDocumentMask(IndexInput& in, IResourceManager& rm) {
  auto count = in.ReadV32();

  if (!count) {
    return {};
  }

  auto docs_mask = std::make_shared<DocumentMask>(rm);
  docs_mask->reserve(count);

  const auto pos = in.Position();
  while (count--) {
    static_assert(sizeof(doc_id_t) == sizeof(decltype(in.ReadV32())));

    docs_mask->insert(in.ReadV32());
  }

  return {std::move(docs_mask), in.Position() - pos};
}

struct SegmentMetaWriterImpl : public SegmentMetaWriter {
  static constexpr std::string_view kFormatExt = "sm";
  static constexpr std::string_view kFormatName = "iresearch_10_segment_meta";

  static constexpr int32_t kFormatMin = 0;
  static constexpr int32_t kFormatMax = 0;

  enum { kHasColumnStore = 1, kSorted = 2 };

  explicit SegmentMetaWriterImpl(int32_t version) noexcept : _version(version) {
    SDB_ASSERT(_version >= kFormatMin && version <= kFormatMax);
  }

  void write(Directory& dir, std::string& filename, SegmentMeta& meta) final;

 private:
  int32_t _version;
};

template<>
inline std::string FileName<SegmentMetaWriter, SegmentMeta>(
  const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version,
                       SegmentMetaWriterImpl::kFormatExt);
}

inline void SegmentMetaWriterImpl::write(Directory& dir, std::string& meta_file,
                                         SegmentMeta& meta) {
  if (meta.docs_count < meta.live_docs_count ||
      meta.docs_count - meta.live_docs_count != RemovalCount(meta))
    [[unlikely]] {
    throw IndexError{absl::StrCat("Invalid segment meta '", meta.name,
                                  "' detected : docs_count=", meta.docs_count,
                                  ", live_docs_count=", meta.live_docs_count)};
  }

  meta_file = FileName<SegmentMetaWriter>(meta);
  auto out = dir.create(meta_file);

  if (!out) [[unlikely]] {
    throw IoError{absl::StrCat("failed to create file, path: ", meta_file)};
  }

  uint8_t flags = meta.column_store ? kHasColumnStore : 0;
  if (field_limits::valid(meta.sort)) {
    flags |= kSorted;
  }

  SDB_ASSERT(meta.docs_mask_size <= meta.byte_size);
  const auto size_without_mask = meta.byte_size - meta.docs_mask_size;

  format_utils::WriteHeader(*out, kFormatName, _version);
  WriteStr(*out, meta.name);
  out->WriteV64(meta.version);
  out->WriteV32(meta.live_docs_count);
  const auto docs_mask_size = WriteDocumentMask(*out, meta.docs_mask);
  out->WriteV64(size_without_mask);
  out->WriteByte(flags);
  out->WriteV64(1 + meta.sort);  // max->0
  WriteStrings(*out, meta.files);
  format_utils::WriteFooter(*out);

  meta.byte_size = size_without_mask + docs_mask_size;
  meta.docs_mask_size = docs_mask_size;
}

struct SegmentMetaReaderImpl : public SegmentMetaReader {
  void read(const Directory& dir, SegmentMeta& meta,
            std::string_view filename = {}) final;  // null == use meta
};

inline void SegmentMetaReaderImpl::read(const Directory& dir, SegmentMeta& meta,
                                        std::string_view filename) {
  const std::string meta_file = IsNull(filename)
                                  ? FileName<SegmentMetaWriter>(meta)
                                  : std::string{filename};

  auto in = dir.open(meta_file, IOAdvice::SEQUENTIAL | IOAdvice::READONCE);

  if (!in) [[unlikely]] {
    throw IoError{absl::StrCat("Failed to open file, path: ", meta_file)};
  }

  const auto checksum = format_utils::Checksum(*in);

  std::ignore = format_utils::CheckHeader(
    *in, SegmentMetaWriterImpl::kFormatName, SegmentMetaWriterImpl::kFormatMin,
    SegmentMetaWriterImpl::kFormatMax);
  auto name = ReadString<std::string>(*in);
  const auto segment_version = in->ReadV64();
  const auto live_docs_count = in->ReadV32();
  auto [docs_mask, docs_mask_size] =
    ReadDocumentMask(*in, *dir.ResourceManager().readers);
  const auto docs_count =
    live_docs_count + static_cast<doc_id_t>(docs_mask ? docs_mask->size() : 0);
  const auto size = in->ReadV64();
  const auto flags = in->ReadByte();
  field_id sort = in->ReadV64() - 1;
  auto files = ReadStrings(*in);
  format_utils::CheckFooter(*in, checksum);

  if (docs_count < live_docs_count) [[unlikely]] {
    throw IndexError{absl::StrCat(
      "While reading segment meta '", name, "', error: docs_count(", docs_count,
      ") > live_docs_count(", live_docs_count, ")")};
  }

  if (flags & ~(SegmentMetaWriterImpl::kHasColumnStore |
                SegmentMetaWriterImpl::kSorted)) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: use of unsupported flags '", flags,
                                  "'")};
  }

  const auto sorted = bool(flags & SegmentMetaWriterImpl::kSorted);

  if ((!field_limits::valid(sort)) && sorted) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: incorrectly marked as sorted")};
  }

  if ((field_limits::valid(sort)) && !sorted) [[unlikely]] {
    throw IndexError{absl::StrCat("While reading segment meta '", name,
                                  "', error: incorrectly marked as unsorted")};
  }

  // ...........................................................................
  // all operations below are noexcept
  // ...........................................................................

  meta.name = std::move(name);
  meta.version = segment_version;
  meta.column_store = flags & SegmentMetaWriterImpl::kHasColumnStore;
  meta.docs_count = docs_count;
  meta.live_docs_count = live_docs_count;
  meta.docs_mask = std::move(docs_mask);
  meta.sort = sort;
  meta.docs_mask_size = docs_mask_size;
  meta.byte_size = size + docs_mask_size;
  meta.files = std::move(files);
}

class PostingsReaderBase : public PostingsReader {
 public:
  uint64_t CountMappedMemory() const final {
    uint64_t bytes = 0;
    if (_doc_in != nullptr) {
      bytes += _doc_in->CountMappedMemory();
    }
    if (_pos_in != nullptr) {
      bytes += _pos_in->CountMappedMemory();
    }
    if (_pay_in != nullptr) {
      bytes += _pay_in->CountMappedMemory();
    }
    return bytes;
  }

  void prepare(IndexInput& in, const ReaderState& state,
               IndexFeatures features) final;

  size_t decode(const byte_type* in, IndexFeatures field_features,
                TermMeta& state) final;

 protected:
  explicit PostingsReaderBase(size_t block_size) noexcept
    : _block_size{block_size} {}

  ScorersView _scorers;
  IndexInput::ptr _doc_in;
  IndexInput::ptr _pos_in;
  IndexInput::ptr _pay_in;
  size_t _block_size;
};

inline void PostingsReaderBase::prepare(IndexInput& in,
                                        const ReaderState& state,
                                        IndexFeatures features) {
  std::string buf;

  // prepare document input
  PrepareInput(buf, _doc_in, IOAdvice::RANDOM, state,
               PostingsWriterBase::kDocExt, PostingsWriterBase::kDocFormatName,
               static_cast<int32_t>(PostingsFormat::Min),
               static_cast<int32_t>(PostingsFormat::Max));

  // Since terms doc postings too large
  //  it is too costly to verify checksum of
  //  the entire file. Here we perform cheap
  //  error detection which could recognize
  //  some forms of corruption.
  format_utils::ReadChecksum(*_doc_in);

  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    /* prepare positions input */
    PrepareInput(buf, _pos_in, IOAdvice::RANDOM, state,
                 PostingsWriterBase::kPosExt,
                 PostingsWriterBase::kPosFormatName,
                 static_cast<int32_t>(PostingsFormat::Min),
                 static_cast<int32_t>(PostingsFormat::Max));

    // Since terms pos postings too large
    // it is too costly to verify checksum of
    // the entire file. Here we perform cheap
    // error detection which could recognize
    // some forms of corruption.
    format_utils::ReadChecksum(*_pos_in);

    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      // prepare positions input
      PrepareInput(buf, _pay_in, IOAdvice::RANDOM, state,
                   PostingsWriterBase::kPayExt,
                   PostingsWriterBase::kPayFormatName,
                   static_cast<int32_t>(PostingsFormat::Min),
                   static_cast<int32_t>(PostingsFormat::Max));

      // Since terms pos postings too large
      // it is too costly to verify checksum of
      // the entire file. Here we perform cheap
      // error detection which could recognize
      // some forms of corruption.
      format_utils::ReadChecksum(*_pay_in);
    }
  }

  // check postings format
  format_utils::CheckHeader(in, PostingsWriterBase::kTermsFormatName,
                            static_cast<int32_t>(TermsFormat::Min),
                            static_cast<int32_t>(TermsFormat::Max));

  const uint64_t block_size = in.ReadV32();

  if (block_size != _block_size) {
    throw IndexError{
      absl::StrCat("while preparing postings_reader, error: "
                   "invalid block size '",
                   block_size, "', expected '", _block_size, "'")};
  }

  _scorers =
    state.scorers.subspan(0, std::min(state.scorers.size(), kMaxScorers));
}

template<typename PostingImpl>
struct PostingAdapter {
  PostingAdapter(DocIterator::ptr it) noexcept : _it{std::move(it)} {
    SDB_ASSERT(_it);
    SDB_ASSERT(dynamic_cast<PostingImpl*>(_it.get()));
  }

  PostingAdapter(PostingAdapter&&) noexcept = default;
  PostingAdapter& operator=(PostingAdapter&&) noexcept = default;

  IRS_FORCE_INLINE operator DocIterator::ptr&&() && noexcept {
    return std::move(_it);
  }

  IRS_FORCE_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept {
    return self().GetMutable(type);
  }

  IRS_FORCE_INLINE doc_id_t value() const noexcept { return self().value(); }

  IRS_FORCE_INLINE doc_id_t advance() { return self().advance(); }

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) {
    return self().seek(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint16_t index) {
    return self().FetchScoreArgs(index);
  }

  IRS_FORCE_INLINE uint32_t Collect(const ScoreFunction& scorer,
                                    ColumnCollector& columns,
                                    std::span<doc_id_t, kScoreBlock> docs,
                                    std::span<score_t, kScoreBlock> scores) {
    return self().Collect(scorer, columns, docs, scores);
  }

  IRS_FORCE_INLINE ScoreFunction
  PrepareScore(const PrepareScoreContext& ctx) const noexcept {
    return self().PrepareScore(ctx);
  }

  IRS_FORCE_INLINE auto FillBlock(doc_id_t min, doc_id_t max, uint64_t* mask,
                                  CollectScoreContext score_ctx,
                                  CollectMatchContext match_ctx) {
    return self().FillBlock(min, max, mask, score_ctx, match_ctx);
  }

 private:
  IRS_FORCE_INLINE PostingImpl& self() const noexcept {
    return static_cast<PostingImpl&>(*_it);
  }

  DocIterator::ptr _it;
};

inline size_t PostingsReaderBase::decode(const byte_type* in,
                                         IndexFeatures features,
                                         TermMeta& state) {
  auto& term_meta = static_cast<TermMetaImpl&>(state);

  const bool has_freq = IndexFeatures::None != (features & IndexFeatures::Freq);
  const auto* p = in;

  term_meta.docs_count = vread<uint32_t>(p);
  if (has_freq) {
    term_meta.freq = term_meta.docs_count + vread<uint32_t>(p);
  }

  term_meta.doc_start += vread<uint64_t>(p);
  if (has_freq && term_meta.freq &&
      IndexFeatures::None != (features & IndexFeatures::Pos)) {
    term_meta.pos_start += vread<uint64_t>(p);

    term_meta.pos_end = term_meta.freq > _block_size
                          ? vread<uint64_t>(p)
                          : address_limits::invalid();

    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      term_meta.pay_start += vread<uint64_t>(p);
    }
  }

  if (1 == term_meta.docs_count) {
    term_meta.e_single_doc = vread<uint32_t>(p);
  } else if (_block_size < term_meta.docs_count) {
    term_meta.e_skip_start = vread<uint64_t>(p);
  }

  SDB_ASSERT(p >= in);
  return size_t(std::distance(in, p));
}

template<typename FormatTraits, bool Freq, bool Pos, bool Offs>
struct IteratorTraitsImpl : FormatTraits {
  static constexpr bool Frequency() noexcept { return Freq; }
  static constexpr bool Position() noexcept { return Freq && Pos; }
  static constexpr bool Offset() noexcept { return Position() && Offs; }
  static constexpr IndexFeatures Features() noexcept {
    auto r = IndexFeatures::None;
    if constexpr (Freq) {
      r |= IndexFeatures::Freq;
    }
    if constexpr (Pos) {
      r |= IndexFeatures::Pos;
    }
    if constexpr (Offs) {
      r |= IndexFeatures::Offs;
    }
    return r;
  }
};

template<typename FormatTraits>
class PostingsReaderImpl final : public PostingsReaderBase {
 public:
  template<bool Freq, bool Pos, bool Offs>
  using IteratorTraits = IteratorTraitsImpl<FormatTraits, Freq, Pos, Offs>;

  PostingsReaderImpl() noexcept
    : PostingsReaderBase{FormatTraits::kBlockSize} {}

  size_t BitUnion(IndexFeatures field, const term_provider_f& provider,
                  size_t* set, uint8_t wand_count) final;

  DocIterator::ptr Iterator(IndexFeatures field_features,
                            IndexFeatures required_features,
                            std::span<const PostingCookie> metas,
                            const IteratorFieldOptions& options,
                            size_t min_match, ScoreMergeType type) const final;

 private:
  template<typename FieldTraits, typename Factory>
  static DocIterator::ptr IteratorImpl(IndexFeatures enabled,
                                       Factory&& factory);

  template<typename Factory>
  static DocIterator::ptr IteratorImpl(IndexFeatures field_features,
                                       IndexFeatures required_features,
                                       Factory&& factory);
};

template<typename FieldTraits, size_t N>
void BitUnionImpl(IndexInput& doc_in, doc_id_t docs_count, uint32_t (&docs)[N],
                  uint32_t (&enc_buf)[N], size_t* set) {
  constexpr auto kBits{BitsRequired<std::remove_pointer_t<decltype(set)>>()};
  size_t num_blocks = docs_count / FieldTraits::kBlockSize;

  auto prev_doc = doc_limits::invalid();
  while (num_blocks--) {
    FieldTraits::read_block_delta(doc_in, enc_buf, docs, prev_doc);
    if constexpr (FieldTraits::Frequency()) {
      FieldTraits::skip_block(doc_in);
    }

    // FIXME optimize
    for (const auto doc : docs) {
      SetBit(set[doc / kBits], doc % kBits);
    }
    prev_doc = docs[N - 1];
  }

  doc_id_t docs_left = docs_count % FieldTraits::kBlockSize;

  while (docs_left--) {
    doc_id_t delta;
    if constexpr (FieldTraits::Frequency()) {
      if (!ShiftUnpack32(doc_in.ReadV32(), delta)) {
        doc_in.ReadV32();
      }
    } else {
      delta = doc_in.ReadV32();
    }

    prev_doc += delta;
    SetBit(set[prev_doc / kBits], prev_doc % kBits);
  }
}

template<typename FormatTraits>
size_t PostingsReaderImpl<FormatTraits>::BitUnion(
  const IndexFeatures field_features, const term_provider_f& provider,
  size_t* set, uint8_t wand_count) {
  constexpr auto kBits{BitsRequired<std::remove_pointer_t<decltype(set)>>()};
  uint32_t enc_buf[FormatTraits::kBlockSize];
  uint32_t docs[FormatTraits::kBlockSize];
  const bool has_freq =
    IndexFeatures::None != (field_features & IndexFeatures::Freq);

  SDB_ASSERT(_doc_in);
  auto doc_in = _doc_in->Reopen();  // reopen thread-safe stream

  if (!doc_in) {
    // implementation returned wrong pointer
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Failed to reopen document input");

    throw IoError("failed to reopen document input");
  }

  size_t count = 0;
  while (const TermMeta* meta = provider()) {
    auto& term_state = static_cast<const TermMetaImpl&>(*meta);

    if (term_state.docs_count > 1) {
      doc_in->Seek(term_state.doc_start);
      SDB_ASSERT(!doc_in->IsEOF());
      if (term_state.docs_count < FormatTraits::kBlockSize) {
        CommonSkipWandData(DynamicExtent{wand_count}, *doc_in);
      }
      SDB_ASSERT(!doc_in->IsEOF());

      if (has_freq) {
        using FieldTraits = IteratorTraits<true, false, false>;
        BitUnionImpl<FieldTraits>(*doc_in, term_state.docs_count, docs, enc_buf,
                                  set);
      } else {
        using FieldTraits = IteratorTraits<false, false, false>;
        BitUnionImpl<FieldTraits>(*doc_in, term_state.docs_count, docs, enc_buf,
                                  set);
      }

      count += term_state.docs_count;
    } else {
      const doc_id_t doc = doc_limits::min() + term_state.e_single_doc;
      SetBit(set[doc / kBits], doc % kBits);

      ++count;
    }
  }

  return count;
}

template<typename FormatTraits>
template<typename FieldTraits, typename Factory>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::IteratorImpl(
  IndexFeatures enabled, Factory&& factory) {
  switch (ToIndex(enabled)) {
    case kPosOffs: {
      using IteratorTraits = IteratorTraits<true, true, true>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case kPos: {
      using IteratorTraits = IteratorTraits<true, true, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case IndexFeatures::Freq: {
      using IteratorTraits = IteratorTraits<true, false, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    default:
      break;
  }
  using IteratorTraits = IteratorTraits<false, false, false>;
  return std::forward<Factory>(factory)
    .template operator()<IteratorTraits, FieldTraits>();
}

template<typename FormatTraits>
template<typename Factory>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::IteratorImpl(
  IndexFeatures field_features, IndexFeatures required_features,
  Factory&& factory) {
  // get enabled features as the intersection
  // between requested and available features
  const auto enabled = field_features & required_features;

  switch (ToIndex(field_features)) {
    case kPosOffs: {
      using FieldTraits = IteratorTraits<true, true, true>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case kPos: {
      using FieldTraits = IteratorTraits<true, true, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case IndexFeatures::Freq: {
      using FieldTraits = IteratorTraits<true, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    default: {
      using FieldTraits = IteratorTraits<false, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
  }
}

template<typename FormatTraits>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::Iterator(
  IndexFeatures field_features, IndexFeatures required_features,
  std::span<const PostingCookie> metas, const IteratorFieldOptions& options,
  size_t min_match, ScoreMergeType type) const {
  SDB_ASSERT(!metas.empty());
  SDB_ASSERT(1 <= min_match);
  SDB_ASSERT(min_match <= metas.size());

  auto make_postings_iterator = [&](uint32_t meta_idx,
                                    const PostingCookie& cookie) {
    return IteratorImpl(
      field_features, required_features,
      [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
        return ResolveExtent<0>(
          options.count,
          [&]<typename Extent>(Extent&& extent) -> DocIterator::ptr {
            auto it = memory::make_managed<
              PostingIteratorImpl<IteratorTraits, FieldTraits, Extent>>(
              std::forward<Extent>(extent));
            it->Prepare(cookie, _doc_in.get(), _pos_in.get(), _pay_in.get());
            return it;
          });
      });
  };

  if (metas.size() == 1) {
    return make_postings_iterator(0, metas[0]);
  }

  std::vector<DocIterator::ptr> iterators;
  iterators.reserve(metas.size());
  uint32_t meta_idx = 0;
  for (const auto& meta : metas) {
    if (auto it = make_postings_iterator(meta_idx, meta)) {
      iterators.emplace_back(std::move(it));
    } else if (min_match == metas.size()) {
      return {};
    }
    ++meta_idx;
  }

  if (iterators.size() < min_match) {
    return {};
  }
  if (iterators.size() == 1) {
    return std::move(iterators[0]);
  }

  return IteratorImpl(
    field_features, required_features,
    [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
      using Adapter = PostingAdapter<PostingIteratorBase<IteratorTraits>>;
      std::vector<Adapter> adapters;
      adapters.reserve(iterators.size());
      for (auto& it : iterators) {
        adapters.emplace_back(std::move(it));
      }
      return ResolveMergeType(type, [&]<ScoreMergeType MergeType> {
        using MinMatchIterator = MinMatchIterator<Adapter, MergeType>;
        return MakeWeakDisjunction<MinMatchIterator>(
          options, std::move(adapters), min_match);
      });
    });
}

class FormatBase : public Format {
 public:
  IndexMetaWriter::ptr get_index_meta_writer() const final {
    return std::make_unique<IndexMetaWriterImpl>(
      IndexMetaWriterImpl::kFormatMax);
  }
  IndexMetaReader::ptr get_index_meta_reader() const final {
    // can reuse stateless reader
    static IndexMetaReaderImpl gInstance;
    return memory::to_managed<IndexMetaReader>(gInstance);
  }

  SegmentMetaWriter::ptr get_segment_meta_writer() const final {
    // can reuse stateless writer
    static SegmentMetaWriterImpl gInstance{SegmentMetaWriterImpl::kFormatMax};
    return memory::to_managed<SegmentMetaWriter>(gInstance);
  }
  SegmentMetaReader::ptr get_segment_meta_reader() const final {
    // can reuse stateless writer
    static SegmentMetaReaderImpl gInstance;
    return memory::to_managed<SegmentMetaReader>(gInstance);
  }

  FieldWriter::ptr get_field_writer(
    bool consolidation, IResourceManager& resource_manager) const final {
    return burst_trie::MakeWriter(
      burst_trie::Version::Min,
      get_postings_writer(consolidation, resource_manager), consolidation,
      resource_manager);
  }
  FieldReader::ptr get_field_reader(
    IResourceManager& resource_manager) const final {
    return burst_trie::MakeReader(get_postings_reader(), resource_manager);
  }

  ColumnstoreWriter::ptr get_columnstore_writer(
    bool consolidation, IResourceManager& resource_manager) const final {
    return columnstore2::MakeWriter(columnstore2::Version::Min, consolidation,
                                    resource_manager);
  }
  ColumnstoreReader::ptr get_columnstore_reader() const final {
    return columnstore2::MakeReader();
  }
};

template<typename F>
class FormatImpl final : public FormatBase {
 public:
  using FormatTraits = F;

  static constexpr std::string_view type_name() noexcept {
    return FormatTraits::kName;
  }

  static ptr make() {
    static const FormatImpl kInstance;
    return {Format::ptr{}, &kInstance};
  }

  PostingsWriter::ptr get_postings_writer(
    bool consolidation, IResourceManager& resource_manager) const final {
    return std::make_unique<PostingsWriterImpl<FormatTraits>>(
      PostingsFormat::WandSimd, consolidation, resource_manager);
  }
  PostingsReader::ptr get_postings_reader() const final {
    return std::make_unique<PostingsReaderImpl<FormatTraits>>();
  }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<FormatImpl>::id();
  }
};

// use base irs::position type for ancestors
template<typename IteratorTraits>
struct Type<PositionImpl<IteratorTraits>> : Type<PosAttr> {};

struct FormatTraits128 {
  using AlignType = __m128i;

  // TODO(mbkkt) rename to "block_128"
  static constexpr std::string_view kName = "1_5simd";

  static constexpr uint32_t kBlockSize = SIMDBlockSize;
  static_assert(kBlockSize <= doc_limits::eof());

  IRS_FORCE_INLINE static void PackBlock(const uint32_t* IRS_RESTRICT decoded,
                                         uint32_t* IRS_RESTRICT encoded,
                                         uint32_t bits) noexcept {
    ::simdpackwithoutmask(decoded, reinterpret_cast<AlignType*>(encoded), bits);
  }

  IRS_FORCE_INLINE static void write_block_delta(IndexOutput& out, uint32_t* in,
                                                 uint32_t prev, uint32_t* buf) {
    DeltaEncode<kBlockSize>(in, prev);
    bitpack::write_block32(PackBlock, out, in, buf, kBlockSize);
  }

  IRS_FORCE_INLINE static void write_block(IndexOutput& out, const uint32_t* in,
                                           uint32_t* buf) {
    bitpack::write_block32(PackBlock, out, in, buf, kBlockSize);
  }

  IRS_FORCE_INLINE static void read_block_delta(IndexInput& in, uint32_t* buf,
                                                uint32_t* out, uint32_t prev) {
    bitpack::read_block_delta32(
      [](uint32_t prev, uint32_t* IRS_RESTRICT decoded,
         const uint32_t* IRS_RESTRICT encoded, uint32_t bits) IRS_FORCE_INLINE {
        ::simdunpackd1(prev, reinterpret_cast<const AlignType*>(encoded),
                       decoded, bits);
      },
      in, buf, out, kBlockSize, prev);
  }

  IRS_FORCE_INLINE static void read_block(IndexInput& in, uint32_t* buf,
                                          uint32_t* out) {
    bitpack::read_block32(
      [](uint32_t* IRS_RESTRICT decoded, const uint32_t* IRS_RESTRICT encoded,
         uint32_t bits) IRS_FORCE_INLINE {
        ::simdunpack(reinterpret_cast<const AlignType*>(encoded), decoded,
                     bits);
      },
      in, buf, out, kBlockSize);
  }

  IRS_FORCE_INLINE static void skip_block(IndexInput& in) {
    bitpack::skip_block32(in, kBlockSize);
  }
};

}  // namespace irs
