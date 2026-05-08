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

#include <iresearch/formats/posting/format_block_128.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "basics/assert.h"
#include "basics/containers/bitset.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/formats_attributes.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/skip_list.hpp"

namespace irs {

// Buffer for storing skip data
struct SkipBuffer {
  explicit SkipBuffer(uint64_t* skip_ptr) noexcept : skip_ptr{skip_ptr} {}

  uint64_t* skip_ptr;  // skip data
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
  static_assert(doc_limits::invalid() ==
                0);  // Delta with invalid document should be measure correctly
};

// Buffer for storing positions
struct PosBuffer : SkipBuffer {
  explicit PosBuffer(std::span<uint32_t> buf, uint64_t* skip_ptr) noexcept
    : SkipBuffer{skip_ptr}, buf{buf} {}

  bool Full() const noexcept { return buf.size() == size; }

  void Next(uint32_t pos) noexcept {
    SDB_ASSERT(last <= pos);

    buf[size] = pos - last;
    last = pos;

    ++size;
  }

  void Reset() noexcept {
    offset = 0;
    size = 0;
    last = pos_limits::invalid();
  }

  uint64_t offset{};
  std::span<uint32_t> buf;  // buffer to store position deltas
  uint32_t size{};          // number of buffered elements
  uint32_t last{};          // last buffered position
};

// Buffer for storing payload data
struct PayBuffer : SkipBuffer {
  PayBuffer(uint32_t* offs_start_buf, uint32_t* offs_len_buf,
            uint64_t* skip_ptr) noexcept
    : SkipBuffer{skip_ptr},
      offs_start_buf{offs_start_buf},
      offs_len_buf{offs_len_buf} {}

  void PushOffset(uint32_t start, uint32_t end) noexcept {
    SDB_ASSERT(last <= start);
    SDB_ASSERT(start <= end);

    offs_start_buf[size] = start - last;
    offs_len_buf[size] = end - start;
    last = start;

    ++size;
  }

  void Reset() noexcept {
    size = 0;
    last = 0;
  }

  uint32_t* offs_start_buf;  // buffer to store start offsets
  uint32_t* offs_len_buf;    // buffer to store offset lengths
  uint32_t size{};           // number of buffered elements
  uint32_t last{};           // last start offset
};

inline WandWriter::ptr PrepareWandWriter(ScorerPtr scorer, size_t max_levels) {
  WandWriter::ptr writer = nullptr;
  if (scorer) {
    writer = (*scorer).prepare_wand_writer(max_levels);
  }
  return writer;
}

// Assume that doc_count = 28, skip_n = skip_0 = 12
//
//  |       block#0       | |      block#1        | |vInts|
//  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
//                          ^                       ^       (level 0 skip point)
class PostingsWriterBase : public PostingsWriter {
 public:
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
    : _new_skip(rm),
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
  FieldStats EndField() noexcept final {
    const auto count = _docs.count();
    SDB_ASSERT(count < doc_limits::eof());
    return {.has_wand = _valid_writer != nullptr,
            .docs_count = static_cast<doc_id_t>(count)};
  }

  void BeginBlock() final {
    // clear state in order to write
    // absolute address of the first
    // entry in the block
    _last_state.clear();
  }

  void Prepare(IndexOutput& out, const FlushState& state) final;
  void Encode(BufferedOutput& out, const TermMeta& attrs) final;

 protected:
  struct Attributes final : AttributeProvider {
    ValueIndex doc;
    FreqAttr freq;

    FreqAttr* wand_freq{};
    PosAttr* pos{};
    const OffsAttr* offs{};

    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<ValueIndex>::id()) {
        return &doc;
      }

      if (type == irs::Type<FreqAttr>::id()) {
        return wand_freq;
      }

      return nullptr;
    }

    void Reset(AttributeProvider& attrs) noexcept {
      if (auto* p = irs::GetMutable<PosAttr>(&attrs)) {
        pos = p;
        offs = irs::get<OffsAttr>(*pos);
      } else {
        pos = &PosAttr::empty();
        offs = nullptr;
      }
    }
  };

  SkipEntry GetSkipEntryAndRecalcDelta();
  void BeginTerm(TermMetaImpl& meta);
  void EndDocument(size_t processed_docs);
  virtual void FlushTailDoc() = 0;
  void EndTerm(TermMetaImpl& meta);
  void PrepareWriters(const FieldProperties& meta);

  template<typename Func>
  void ApplyToWriter(Func&& func) {
    if (_valid_writer) {
      func(*_valid_writer);
    }
  }

  bool HasValidWandWriter() const { return _valid_writer; }

  NewSkipWriter _new_skip;
  // SkipWriter _skip;
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
  WandWriter::ptr _writer;    // Wand writers
  WandWriter* _valid_writer;  // Valid wand writer
  Features _features;         // Features supported by current field
  const PostingsFormat _postings_format_version;
  const TermsFormat _terms_format_version;
};

inline void PostingsWriterBase::PrepareWriters(const FieldProperties& meta) {
  _valid_writer = nullptr;

  if (!_columns) [[unlikely]] {
    return;
  }

  // Enable/Disable frequency for WandWriter::Prepare
  _attrs.wand_freq = _features.HasFrequency() ? &_attrs.freq : nullptr;

  if (_writer && _writer->Prepare(*_columns, meta, _attrs)) {
    _valid_writer = _writer.get();
  }
}

inline SkipEntry PostingsWriterBase::GetSkipEntryAndRecalcDelta() {
  SDB_ASSERT(_doc_out);
  const doc_id_t doc = _doc.last;
  const uint64_t doc_ptr = _doc_out->Position();

  SkipEntry result;

  result.max_doc_delta = doc - _doc.skip_doc[1];
  result.doc_ptr = doc_ptr - _doc.skip_ptr[1];

  _doc.skip_doc[1] = doc;
  _doc.skip_ptr[1] = doc_ptr;

  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    const uint64_t pos_ptr = _pos_out->Position();
    result.meta.pos_ptr = pos_ptr - _pos.skip_ptr[1];
    _pos.skip_ptr[1] = pos_ptr;
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      const uint64_t pay_ptr = _pay_out->Position();
      result.meta.pay_ptr = pay_ptr - _pay.skip_ptr[1];
      _pay.skip_ptr[1] = pay_ptr;
    }
    SDB_ASSERT(_pos.size <= std::numeric_limits<uint8_t>::max());
    result.meta.pos_block_idx = _pos.size;
  }

  return result;
}

inline void PostingsWriterBase::Prepare(IndexOutput& out,
                                        const FlushState& state) {
  SDB_ASSERT(state.dir);
  SDB_ASSERT(!IsNull(state.name));

  std::string name;

  // Prepare document stream
  format_utils::PrepareOutput(name, _doc_out, state, kDocExt, kDocFormatName,
                              static_cast<int32_t>(_postings_format_version));

  if (IndexFeatures::None != (state.index_features & IndexFeatures::Pos)) {
    // Prepare proximity stream
    _pos.Reset();
    format_utils::PrepareOutput(name, _pos_out, state, kPosExt, kPosFormatName,
                                static_cast<int32_t>(_postings_format_version));

    if (IndexFeatures::None != (state.index_features & IndexFeatures::Offs)) {
      // Prepare payload stream
      _pay.Reset();
      format_utils::PrepareOutput(
        name, _pay_out, state, kPayExt, kPayFormatName,
        static_cast<int32_t>(_postings_format_version));
    }
  }

  _new_skip.Prepare(state.doc_count);

  format_utils::WriteHeader(out, kTermsFormatName,
                            static_cast<int32_t>(_terms_format_version));
  out.WriteV32(_new_skip.Skip0());  // Write postings block size

  // Prepare wand writers
  _writer = PrepareWandWriter(state.scorer, doc_limits::kMaxSkipLevels);
  _columns = state.columns;

  // Prepare documents bitset
  _docs.reset(doc_limits::min() + state.doc_count);
}

inline void PostingsWriterBase::Encode(BufferedOutput& out,
                                       const TermMeta& state) {
  const auto& meta = static_cast<const TermMetaImpl&>(state);

  out.WriteV32(meta.docs_count);
  if (_features.HasFrequency()) {
    SDB_ASSERT(meta.freq >= meta.docs_count);
    out.WriteV32(meta.freq - meta.docs_count);
  }

  out.WriteV64(meta.doc_start - _last_state.doc_start);
  if (_features.HasPosition()) {
    out.WriteV64(meta.pos_start - _last_state.pos_start);
    if (_features.HasOffset()) {
      out.WriteV64(meta.pay_start - _last_state.pay_start);
    }
    SDB_ASSERT(meta.pos_offset <= std::numeric_limits<uint8_t>::max());
    out.WriteByte(meta.pos_offset);
  }

  if (meta.docs_count == 1) {
    out.WriteV32(meta.e_single_doc);
  } else if (meta.docs_count > _new_skip.Skip0()) {
    out.WriteV64(meta.e_skip_start);
  }

  _last_state = meta;
}

inline void PostingsWriterBase::BeginTerm(TermMetaImpl& meta) {
  meta.doc_start = _doc_out->Position();
  std::fill_n(_doc.skip_doc, doc_limits::kMaxSkipLevels, doc_limits::invalid());
  std::fill_n(_doc.skip_ptr, doc_limits::kMaxSkipLevels, meta.doc_start);
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    meta.pos_start = _pos_out->Position();
    std::fill_n(_pos.skip_ptr, doc_limits::kMaxSkipLevels, meta.pos_start);
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      meta.pay_start = _pay_out->Position();
      std::fill_n(_pay.skip_ptr, doc_limits::kMaxSkipLevels, meta.pay_start);
    }
    meta.pos_offset = _pos.size;
  }

  _doc.last = doc_limits::invalid();
  _doc.block_last = doc_limits::invalid();
  _new_skip.Reset();
}

inline void PostingsWriterBase::EndTerm(TermMetaImpl& meta) {
  if (meta.docs_count == 0) {
    return;  // no documents to write
  }

  const bool has_skip_list = NewSkipWriter::Skip0() < meta.docs_count;
  auto write_max_score = [&](size_t level) {
    if (HasValidWandWriter()) {
      auto wand_data = _valid_writer->CalculateAndGetWandDataRoot(level);
      NewSkipWriter::WriteWandData(wand_data, *_doc_out);
    }
  };

  if (1 == meta.docs_count) {
    meta.e_single_doc = _doc.docs[0] - doc_limits::min();
  } else {
    if (!has_skip_list) {
      write_max_score(0);
    }
    if ((meta.docs_count & (NewSkipWriter::Skip0() - 1)) != 0) {
      FlushTailDoc();
    }
  }

  // if we have flushed at least
  // one block there was buffered
  // skip data, so we need to flush it
  if (has_skip_list) {
    meta.e_skip_start = _doc_out->Position() - meta.doc_start;
    const auto num_levels = NewSkipWriter::CountLevels(meta.docs_count);
    write_max_score(num_levels);
    _new_skip.FlushLevel(num_levels, *_doc_out);
  }

  _doc.doc = _doc.docs.begin();
  _doc.freq = _doc.freqs.begin();
  _doc.last = doc_limits::invalid();

  _pos.last = pos_limits::invalid();

  _pay.last = 0;
}

template<typename FormatTraits>
class PostingsWriterImpl final : public PostingsWriterBase {
 public:
  explicit PostingsWriterImpl(PostingsFormat version, bool volatile_attributes,
                              IResourceManager& rm)
    : PostingsWriterBase{doc_limits::kBlockSize,
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

  void BeginField(const FieldProperties& meta) final;
  void Write(DocIterator& docs, TermMeta& base_meta) final;
  void End() final;

 private:
  void FlushTailDoc() final;
  void FlushTailPos();
  void FlushTailPay();
  void AddPosition(uint32_t pos);
  void BeginDocument();
  void EndDocument(size_t processed_docs);

  struct {
    // Buffer for document deltas
    doc_id_t docs[doc_limits::kBlockSize]{};
    // Buffer for frequencies
    uint32_t freqs[doc_limits::kBlockSize]{};
    // Buffer for skip documents
    doc_id_t skip_doc[doc_limits::kMaxSkipLevels]{};
    // Buffer for skip pointers
    uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  } _doc_buf;
  struct {
    // Buffer for position deltas
    uint32_t buf[pos_limits::kBlockSize]{};
    // Buffer for skip pointers
    uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  } _prox_buf;
  struct {
    // Buffer for start offsets
    uint32_t offs_start_buf[pos_limits::kBlockSize]{};
    // Buffer for offset lengths
    uint32_t offs_len_buf[pos_limits::kBlockSize]{};
    // Buffer for skip pointers
    uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  } _pay_buf;
  struct {
    // Buffer for encoding (worst case)
    uint32_t buf[std::max(doc_limits::kBlockSize, pos_limits::kBlockSize)];
  } _encbuf;
  bool _volatile_attributes;
};

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailDoc() {
  auto* doc = _doc.docs.data();
  const auto tail = std::distance(doc, _doc.doc.base());
  SDB_ASSERT(tail != 0);
  FormatTraits::WriteTailDelta(tail, *_doc_out, doc, _doc.block_last, _buf);
  if (_features.HasFrequency()) {
    FormatTraits::WriteTail(tail, *_doc_out, _doc.freqs.data(), _buf);
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailPos() {
  SDB_ASSERT(_pos_out);
  SDB_ASSERT(_pos.size != 0);
  const auto tail_size = doc_limits::kBlockSize - _pos.size;
  SDB_ASSERT(tail_size != 0);

  auto* pos_tail = _pos.buf.data() + _pos.size;
  std::fill_n(pos_tail, tail_size, pos_tail[-1]);
  FormatTraits::WriteBlock(*_pos_out, _pos.buf.data(), _buf);

  _pos.size = 0;
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailPay() {
  SDB_ASSERT(_pay_out);
  SDB_ASSERT(_pay.size != 0);
  const auto tail_size = doc_limits::kBlockSize - _pay.size;
  SDB_ASSERT(tail_size != 0);

  auto* offs_start_tail = _pay.offs_start_buf + _pay.size;
  std::fill_n(offs_start_tail, tail_size, offs_start_tail[-1]);
  FormatTraits::WriteBlock(*_pay_out, _pay.offs_start_buf, _buf);

  auto* offs_len_tail = _pay.offs_len_buf + _pay.size;
  std::fill_n(offs_len_tail, tail_size, offs_len_tail[-1]);
  FormatTraits::WriteBlock(*_pay_out, _pay.offs_len_buf, _buf);

  _pay.size = 0;
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::BeginField(const FieldProperties& meta) {
  _features.Reset(meta.index_features);
  PrepareWriters(meta);
  _docs.clear();
  _last_state.clear();

  // It's needed because offsets block should be aligned with positions block.
  // But it's possible that fields have different features set.
  // So if it was case when we didn't have offsets we need to flush positions.
  // And if we had positions and offsets and now we will write only positions
  // we need to flush positions and offsets.
  if (_features.HasOffset()) {
    if (_pos.size != _pay.size) [[unlikely]] {
      SDB_ASSERT(_pay.size == 0);
      FlushTailPos();
    }
  } else if (_pay.size != 0) [[unlikely]] {
    FlushTailPos();
    FlushTailPay();
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::BeginDocument() {
  if (const auto id = _attrs.doc.value; _doc.last < id) [[likely]] {
    _doc.Push(id, _attrs.freq.value);

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
void PostingsWriterImpl<FormatTraits>::EndDocument(size_t processed_docs) {
  if (_doc.Full()) {
    InlineSkipEntry skip_entry;
    skip_entry.max_doc_delta = _doc.last - _doc.block_last;
    if (HasValidWandWriter()) {
      skip_entry.wand_data = _valid_writer->CalculateAndGetWandData(0);
    }

    auto [best_encoding_doc, best_size_doc] =
      FormatTraits::CalculateBestEncodingAndSizeBlockDelta(
        _doc.docs.data(), _doc.block_last, _buf);
    auto best_encoding_freq_opt =
      _features.HasFrequency()
        ? std::optional(
            FormatTraits::CalculateBestEncodingBlock(_doc.freqs.data(), _buf))
        : std::nullopt;

    PosPayMetadata meta;
    if (_features.HasPosition()) {
      SDB_ASSERT(_pos_out);
      const uint64_t pos_ptr = _pos_out->Position();
      meta.pos_ptr = pos_ptr - _pos.skip_ptr[0];
      _pos.skip_ptr[0] = pos_ptr;
      if (_features.HasOffset()) {
        SDB_ASSERT(_pay_out);
        const uint64_t pay_ptr = _pay_out->Position();
        meta.pay_ptr = pay_ptr - _pay.skip_ptr[0];
        _pay.skip_ptr[0] = pay_ptr;
      }
      SDB_ASSERT(_pos.size <= std::numeric_limits<uint8_t>::max());
      meta.pos_block_idx = _pos.size;
    }
    auto pos_pay_meta_serialized_size =
      NewSkipWriter::CalculatePosPayMetaSize(meta, _features);

    skip_entry.rest_block_size =
      (1 + best_size_doc) +
      (_features.HasFrequency() ? 1 + best_encoding_freq_opt->best_size : 0) +
      pos_pay_meta_serialized_size;

    // Serializing Inline Block
    {
      NewSkipWriter::WriteInlineSkipEntry(skip_entry, *_doc_out);

      auto decode_zone_start = _doc_out->Position();
      FormatTraits::WriteBlockDelta(best_encoding_doc, best_size_doc, *_doc_out,
                                    _doc.docs.data(), _doc.block_last, _buf);
      SDB_ASSERT(best_size_doc + 1 != _doc_out->Position() - decode_zone_start);
      if (_features.HasFrequency()) {
        auto frequencies_zone_start = _doc_out->Position();
        FormatTraits::WriteBlock(*best_encoding_freq_opt, *_doc_out,
                                 _doc.freqs.data(), _buf);
        SDB_ASSERT(_doc_out->Position() - frequencies_zone_start ==
                   best_encoding_freq_opt.value().best_size + 1);
      }

      NewSkipWriter::WriteInlinePosPayMetadata(meta, _features, *_doc_out);
      SDB_ASSERT(skip_entry.rest_block_size !=
                 _doc_out->Position() - decode_zone_start);
    }

    _new_skip.AddLevel1IfNeed(processed_docs, [this](MemoryIndexOutput& out) {
      NewSkipWriter::WriteSkipEntry(GetSkipEntryAndRecalcDelta(), _features,
                                    out);

      ApplyToWriter([&](auto& writer) {
        const uint8_t size = writer.Size(1);
        SDB_ASSERT(size <= WandWriter::kMaxSize);
        out.WriteByte(size);
      });
      ApplyToWriter([&](auto& writer) { writer.Write(1, out); });
    });

    _doc.block_last = _doc.last;
    _doc.doc = _doc.docs.begin();
    _doc.freq = _doc.freqs.begin();
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::AddPosition(uint32_t pos) {
  // at least positions stream should be created
  SDB_ASSERT(_features.HasPosition());
  SDB_ASSERT(!_features.HasOffset() == !_attrs.offs);

  SDB_ASSERT(_pos.size == _pay.size || _pay.size == 0);
  _pos.Next(pos);
  if (_features.HasOffset()) {
    _pay.PushOffset(_attrs.offs->start, _attrs.offs->end);
  }
  SDB_ASSERT(_pos.size == _pay.size || _pay.size == 0);

  if (_pos.Full()) [[unlikely]] {
    SDB_ASSERT(_pos_out);
    FormatTraits::WriteBlock(*_pos_out, _pos.buf.data(), _buf);
    _pos.size = 0;

    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      SDB_ASSERT(_pay.size != 0);
      FormatTraits::WriteBlock(*_pay_out, _pay.offs_start_buf, _buf);
      FormatTraits::WriteBlock(*_pay_out, _pay.offs_len_buf, _buf);
      _pay.size = 0;
    }
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::End() {
  format_utils::WriteFooter(*_doc_out);
  _doc_out.reset();  // ensure stream is closed

  if (_pos_out) {
    if (_pos.size != 0) {
      FlushTailPos();
    }
    format_utils::WriteFooter(*_pos_out);
    _pos_out.reset();  // ensure stream is closed

    if (_pay_out) {
      if (_pay.size != 0) {
        FlushTailPay();
      }
      format_utils::WriteFooter(*_pay_out);
      _pay_out.reset();  // ensure stream is closed
    } else {
      SDB_ASSERT(_pay.size == 0);
    }
  } else {
    SDB_ASSERT(_pos.size == 0);
    SDB_ASSERT(!_pay_out);
    SDB_ASSERT(_pay.size == 0);
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::Write(DocIterator& docs,
                                             TermMeta& base_meta) {
  auto refresh = [&](auto& attrs) noexcept { _attrs.Reset(attrs); };

  if (!_volatile_attributes) {
    refresh(docs);
  } else {
    auto* subscription = irs::get<AttrProviderChangeAttr>(docs);
    SDB_ASSERT(subscription);
    subscription->Subscribe(refresh);
  }

  auto& meta = static_cast<TermMetaImpl&>(base_meta);

  BeginTerm(meta);
  ApplyToWriter([&](auto& writer) { writer.Reset(); });

  uint32_t docs_count = 0;
  uint32_t total_freq = 0;

  while (true) {
    const auto doc = docs.advance();
    SDB_ASSERT(doc_limits::valid(doc));
    if (doc_limits::eof(doc)) {
      break;
    }
    _attrs.doc.value = doc;
    _attrs.freq.value = docs.GetFreq();

    BeginDocument();
    ApplyToWriter([](auto& writer) { writer.Update(); });
    SDB_ASSERT(_attrs.pos);
    while (_attrs.pos->next()) {
      SDB_ASSERT(pos_limits::valid(_attrs.pos->value()));
      AddPosition(_attrs.pos->value());
    }
    ++docs_count;
    total_freq += _attrs.freq.value;
    EndDocument(docs_count);
  }

  // FIXME(gnusi): do we need to write terminal skip if present?

  meta.docs_count = docs_count;
  meta.freq = total_freq;
  EndTerm(meta);
}

}  // namespace irs
