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

#include <iresearch/formats/format_utils.hpp>

#include "basics/containers/bitset.hpp"
#include "iresearch/analysis/token_attributes.hpp"
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
    : _skip{block_size, doc_limits::kSkipSize, rm},
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
    return {.wand_mask = _writers_mask,
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

  void WriteSkip(size_t level, MemoryIndexOutput& out) const;
  void BeginTerm(TermMetaImpl& meta);
  void EndDocument();
  virtual void FlushTailDoc() = 0;
  void EndTerm(TermMetaImpl& meta);
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

  // Enable/Disable frequency for WandWriter::Prepare
  _attrs.wand_freq = _features.HasFrequency() ? &_attrs.freq : nullptr;

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
  SDB_ASSERT(_doc_out);
  const doc_id_t doc = _doc.block_last;
  const uint64_t doc_ptr = _doc_out->Position();

  out.WriteV32(doc);  // - doc_.skip_doc[level];
  out.WriteV64(doc_ptr - _doc.skip_ptr[level]);

  _doc.skip_doc[level] = doc;
  _doc.skip_ptr[level] = doc_ptr;

  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    const uint64_t pos_ptr = _pos_out->Position();
    out.WriteV64(pos_ptr - _pos.skip_ptr[level]);
    _pos.skip_ptr[level] = pos_ptr;
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      const uint64_t pay_ptr = _pay_out->Position();
      out.WriteV64(pay_ptr - _pay.skip_ptr[level]);
      _pay.skip_ptr[level] = pay_ptr;
    }
    SDB_ASSERT(_pos.size <= std::numeric_limits<uint8_t>::max());
    out.WriteByte(_pos.size);
  }
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

  _skip.Prepare(doc_limits::kMaxSkipLevels, state.doc_count);

  format_utils::WriteHeader(out, kTermsFormatName,
                            static_cast<int32_t>(_terms_format_version));
  out.WriteV32(_skip.Skip0());  // Write postings block size

  // Prepare wand writers
  _writers = PrepareWandWriters(state.scorers, doc_limits::kMaxSkipLevels);
  _valid_writers.reserve(_writers.size());
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
  } else if (meta.docs_count > _skip.Skip0()) {
    out.WriteV64(meta.e_skip_start);
  }

  _last_state = meta;
}

inline void PostingsWriterBase::BeginTerm(TermMetaImpl& meta) {
  meta.doc_start = _doc_out->Position();
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
  _skip.Reset();
}

inline void PostingsWriterBase::EndDocument() {
  if (_doc.Full()) {
    _doc.block_last = _doc.last;
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
    if (!has_skip_list) {
      write_max_score(0);
    }
    if ((meta.docs_count & (_skip.Skip0() - 1)) != 0) {
      FlushTailDoc();
    }
  }

  // if we have flushed at least
  // one block there was buffered
  // skip data, so we need to flush it
  if (has_skip_list) {
    meta.e_skip_start = _doc_out->Position() - meta.doc_start;
    const auto num_levels = _skip.CountLevels();
    write_max_score(num_levels);
    _skip.FlushLevels(num_levels, *_doc_out);
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

    if (_doc.Full()) {
      FormatTraits::WriteBlockDelta(*_doc_out, _doc.docs.data(),
                                    _doc.block_last, _buf);
      if (_features.HasFrequency()) {
        FormatTraits::WriteBlock(*_doc_out, _doc.freqs.data(), _buf);
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
  ApplyWriters([&](auto& writer) { writer.Reset(); });

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
    total_freq += _attrs.freq.value;
    EndDocument();
  }

  // FIXME(gnusi): do we need to write terminal skip if present?

  meta.docs_count = docs_count;
  meta.freq = total_freq;
  EndTerm(meta);
}

}  // namespace irs
