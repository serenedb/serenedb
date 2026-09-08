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

#include "basics/containers/bitset.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {

struct DocBuffer {
  bool Full() const noexcept { return size == doc_limits::kBlockSize; }

  bool Empty() const noexcept { return size == 0; }

  void Push(doc_id_t doc) noexcept {
    docs[size] = doc;
    ++size;
    last = doc;
  }

  void Push(doc_id_t doc, uint32_t freq) noexcept {
    freqs[size] = freq;
    Push(doc);
  }

  doc_id_t docs[doc_limits::kBlockSize]{};
  uint32_t freqs[doc_limits::kBlockSize]{};
  doc_id_t skip_doc[doc_limits::kMaxSkipLevels]{};
  uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  uint32_t size{};
  doc_id_t last{doc_limits::invalid()};        // last buffered document id
  doc_id_t block_last{doc_limits::invalid()};  // last document id in a block
};

struct PosBuffer {
  bool Full() const noexcept { return size == pos_limits::kBlockSize; }

  void Next(uint32_t pos) noexcept {
    SDB_ASSERT(last <= pos);

    buf[size] = pos - last;
    last = pos;

    ++size;
  }

  void Reset() noexcept {
    size = 0;
    last = pos_limits::invalid();
  }

  uint32_t buf[pos_limits::kBlockSize]{};
  uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  uint32_t size{};  // number of buffered position deltas
  uint32_t last{};  // last buffered position
};

struct PayBuffer {
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

  uint32_t offs_start_buf[pos_limits::kBlockSize]{};
  uint32_t offs_len_buf[pos_limits::kBlockSize]{};
  uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  uint32_t size{};  // number of buffered offsets
  uint32_t last{};  // last start offset
};

inline ScoreBoundWriter::ptr PrepareScoreBoundWriter(ScorerPtr scorer,
                                                     size_t max_levels) {
  ScoreBoundWriter::ptr writer = nullptr;
  if (scorer) {
    writer = (*scorer).PrepareScoreBoundWriter(max_levels);
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

  FieldStats EndField() final {
    if (_features.HasVector() && _term_pay != nullptr) {
      SDB_ASSERT(_pay_out);
      _term_pay->Finish(*_pay_out);
    }
    const auto count = _docs.count();
    SDB_ASSERT(count < doc_limits::eof());
    return {.has_score_bounds = _valid_writer != nullptr,
            .docs_count = static_cast<doc_id_t>(count)};
  }

  void SetTermPayloadWriter(TermPayloadWriter* writer) final {
    _term_pay = writer;
  }

  void BeginBlock() final {
    // clear state in order to write
    // absolute address of the first
    // entry in the block
    _last_state.clear();
  }

  void Prepare(IndexOutput& out, const FlushState& state) final;
  void Encode(BufferedOutput& out, const PostingMeta& state) final;

 protected:
  explicit PostingsWriterBase(IResourceManager& rm)
    : _skip{doc_limits::kBlockSize, doc_limits::kSkipSize, rm} {}

  class Features {
   public:
    void Reset(IndexFeatures features) noexcept {
      _has_freq = (IndexFeatures::None != (features & IndexFeatures::Freq));
      _has_pos = (IndexFeatures::None != (features & IndexFeatures::Pos));
      _has_offs = (IndexFeatures::None != (features & IndexFeatures::Offs));
      _has_vec = (IndexFeatures::None != (features & IndexFeatures::Vec));
    }

    bool HasFrequency() const noexcept { return _has_freq; }
    bool HasPosition() const noexcept { return _has_pos; }
    bool HasOffset() const noexcept { return _has_offs; }
    bool HasVector() const noexcept { return _has_vec; }

   private:
    bool _has_freq{};
    bool _has_pos{};
    bool _has_offs{};
    bool _has_vec{};
  };

  struct Attributes final : AttributeProvider {
    ValueIndex doc;
    FreqAttr freq;

    FreqAttr* score_bound_freq{};
    PosAttr* pos{};
    const OffsAttr* offs{};

    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<ValueIndex>::id()) {
        return &doc;
      }

      if (type == irs::Type<FreqAttr>::id()) {
        return score_bound_freq;
      }

      return nullptr;
    }

    void Reset(TermPostings& docs) noexcept {
      if (auto* p = docs.Positions()) {
        pos = p;
        offs = irs::get<OffsAttr>(*pos);
      } else {
        pos = &PosAttr::empty();
        offs = nullptr;
      }
    }
  };

  void WriteSkip(size_t level, MemoryIndexOutput& out);
  void BeginTerm(PostingMeta& meta);
  void ArmSkip(const PostingMeta& meta);
  virtual void FlushTailDoc() = 0;
  void EndTerm(PostingMeta& meta);
  void PrepareWriters(const FieldProperties& meta);

  template<typename Func>
  void ApplyToWriter(Func&& func) {
    if (_valid_writer) {
      func(*_valid_writer);
    }
  }

  SkipWriter _skip;
  PostingMeta _last_state;    // Last final term state
  bitset _docs;               // Set of all processed documents
  IndexOutput::ptr _doc_out;  // Postings (doc + freq)
  IndexOutput::ptr _pos_out;  // Positions
  IndexOutput::ptr _pay_out;  // Payload (pay + offs)
  DocBuffer _doc;             // Document stream
  PosBuffer _pos;             // Proximity stream
  PayBuffer _pay;             // Payloads and offsets stream
  Attributes _attrs;          // Set of attributes
  const NormProvider* _norms{};
  ScoreBoundWriter::ptr _writer;      // Score bound writer
  ScoreBoundWriter* _valid_writer{};  // Valid score bound writer
  Features _features;                 // Features supported by current field
  // Per-term payload writer for IndexFeatures::Vec fields (e.g. IVF codes).
  TermPayloadWriter* _term_pay{};
  // Scratch list of the current term's document ids (collected when
  // HasVector).
  std::vector<doc_id_t> _term_docs;
  // Whether the current term armed the skip writer (it does at its first
  // block fill).
  bool _skip_armed{false};
};

inline void PostingsWriterBase::PrepareWriters(const FieldProperties& meta) {
  _valid_writer = nullptr;

  if (!_norms) [[unlikely]] {
    return;
  }

  _attrs.score_bound_freq = _features.HasFrequency() ? &_attrs.freq : nullptr;

  if (_writer && _writer->Prepare(*_norms, meta, _attrs)) {
    _valid_writer = _writer.get();
  }
}

inline void PostingsWriterBase::WriteSkip(size_t level,
                                          MemoryIndexOutput& out) {
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
  format_utils::PrepareOutput(name, _doc_out, state, kDocExt, kDocFormatName);

  if (IndexFeatures::None != (state.index_features & IndexFeatures::Pos)) {
    // Prepare proximity stream
    _pos.Reset();
    format_utils::PrepareOutput(name, _pos_out, state, kPosExt, kPosFormatName);
  }

  // The ".pay" stream holds position-level offsets (IndexFeatures::Offs) and/or
  // fixed-width per-document payloads (IndexFeatures::Vec, e.g. IVF codes).
  const bool has_offs =
    IndexFeatures::None != (state.index_features & IndexFeatures::Offs);
  const bool has_vec =
    IndexFeatures::None != (state.index_features & IndexFeatures::Vec);
  if (has_offs) {
    _pay.Reset();
  }
  if (has_offs || has_vec) {
    format_utils::PrepareOutput(name, _pay_out, state, kPayExt, kPayFormatName);
  }

  _skip.Prepare(doc_limits::kMaxSkipLevels, state.doc_count);

  out.WriteV32(_skip.Skip0());  // Write postings block size

  _writer = PrepareScoreBoundWriter(state.scorer, doc_limits::kMaxSkipLevels);
  _norms = state.norms;

  // Prepare documents bitset
  _docs.reset(doc_limits::min() + state.doc_count);
}

inline void PostingsWriterBase::Encode(BufferedOutput& out,
                                       const PostingMeta& meta) {
  SDB_ASSERT(!_features.HasVector() ||
             (!_features.HasPosition() && !_features.HasOffset()));

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
  } else if (_features.HasVector()) {
    out.WriteV64(meta.pay_start - _last_state.pay_start);
    SDB_ASSERT(meta.pos_offset <= std::numeric_limits<uint8_t>::max());
    out.WriteByte(meta.pos_offset);
  }

  if (meta.docs_count == 1 || meta.docs_count > _skip.Skip0()) {
    out.WriteV32(meta.doc_delta);
  }

  _last_state = meta;
}

inline void PostingsWriterBase::BeginTerm(PostingMeta& meta) {
  meta.doc_start = _doc_out->Position();
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    meta.pos_start = _pos_out->Position();
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      meta.pay_start = _pay_out->Position();
    }
    SDB_ASSERT(_pos.size <= std::numeric_limits<uint8_t>::max());
    meta.pos_offset = static_cast<uint8_t>(_pos.size);
  }
}

inline void PostingsWriterBase::ArmSkip(const PostingMeta& meta) {
  std::fill_n(_doc.skip_ptr, doc_limits::kMaxSkipLevels, meta.doc_start);
  if (_features.HasPosition()) {
    std::fill_n(_pos.skip_ptr, doc_limits::kMaxSkipLevels, meta.pos_start);
    if (_features.HasOffset()) {
      std::fill_n(_pay.skip_ptr, doc_limits::kMaxSkipLevels, meta.pay_start);
    }
  }
  _skip.Reset();
  _skip_armed = true;
}

inline void PostingsWriterBase::EndTerm(PostingMeta& meta) {
  if (meta.docs_count == 0) {
    return;  // no documents to write
  }

  const bool has_skip_list = _skip.Skip0() < meta.docs_count;
  SDB_ASSERT(!has_skip_list || _skip_armed);
  auto write_max_score = [&](size_t level) {
    ApplyToWriter([&](auto& writer) {
      const uint8_t size = writer.SizeRoot(level);
      _doc_out->WriteByte(size);
    });
    ApplyToWriter([&](auto& writer) { writer.WriteRoot(level, *_doc_out); });
  };

  if (1 == meta.docs_count) {
    meta.doc_delta = _doc.docs[0] - doc_limits::min();
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
    const uint64_t skip_start = _doc_out->Position() - meta.doc_start;
    SDB_ENSURE(skip_start <= std::numeric_limits<uint32_t>::max(),
               "postings writer: a single term's `.doc` footprint of ",
               skip_start, " bytes exceeds the ",
               std::numeric_limits<uint32_t>::max(), " byte limit");
    meta.doc_delta = static_cast<uint32_t>(skip_start);
    const auto num_levels = _skip.CountLevels();
    write_max_score(num_levels);
    _skip.FlushLevels(num_levels, *_doc_out);
  }

  _doc.size = 0;
  _doc.last = doc_limits::invalid();
  _doc.block_last = doc_limits::invalid();
  _skip_armed = false;

  _pos.last = pos_limits::invalid();

  _pay.last = 0;
}

template<typename FormatTraits>
class PostingsWriterImpl final : public PostingsWriterBase {
 public:
  explicit PostingsWriterImpl(bool volatile_attributes, IResourceManager& rm)
    : PostingsWriterBase{rm}, _volatile_attributes{volatile_attributes} {}

  void BeginField(const FieldProperties& meta) final;
  void Write(TermPostings& docs, PostingMeta& meta) final;
  void End() final;

 private:
  void FlushTailDoc() final;
  void FlushTailPos();
  void FlushTailPay();
  void AddPosition(uint32_t pos);

  // Buffer for block encoding (worst case)
  uint32_t _enc_buf[std::max(doc_limits::kBlockSize, pos_limits::kBlockSize)];
  bool _volatile_attributes;
};

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailDoc() {
  const auto tail = _doc.size;
  SDB_ASSERT(tail != 0);
  FormatTraits::WriteTailDelta(tail, *_doc_out, _doc.docs, _doc.block_last,
                               _enc_buf);
  if (_features.HasFrequency()) {
    FormatTraits::WriteTail(tail, *_doc_out, _doc.freqs, _enc_buf);
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailPos() {
  SDB_ASSERT(_pos_out);
  SDB_ASSERT(_pos.size != 0);
  const auto tail_size = doc_limits::kBlockSize - _pos.size;
  SDB_ASSERT(tail_size != 0);

  auto* pos_tail = _pos.buf + _pos.size;
  std::fill_n(pos_tail, tail_size, pos_tail[-1]);
  FormatTraits::WriteBlock(*_pos_out, _pos.buf, _enc_buf);

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
  FormatTraits::WriteBlock(*_pay_out, _pay.offs_start_buf, _enc_buf);

  auto* offs_len_tail = _pay.offs_len_buf + _pay.size;
  std::fill_n(offs_len_tail, tail_size, offs_len_tail[-1]);
  FormatTraits::WriteBlock(*_pay_out, _pay.offs_len_buf, _enc_buf);

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
    FormatTraits::WriteBlock(*_pos_out, _pos.buf, _enc_buf);
    _pos.size = 0;

    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      SDB_ASSERT(_pay.size != 0);
      FormatTraits::WriteBlock(*_pay_out, _pay.offs_start_buf, _enc_buf);
      FormatTraits::WriteBlock(*_pay_out, _pay.offs_len_buf, _enc_buf);
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
  } else {
    SDB_ASSERT(_pos.size == 0);
  }

  // ".pay" may be open for offsets (with positions) and/or for fixed-width
  // per-document payloads (IndexFeatures::Vec, no positions).
  if (_pay_out) {
    if (_pay.size != 0) {
      FlushTailPay();
    }
    format_utils::WriteFooter(*_pay_out);
    _pay_out.reset();  // ensure stream is closed
  } else {
    SDB_ASSERT(_pay.size == 0);
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::Write(TermPostings& docs,
                                             PostingMeta& meta) {
  auto refresh = [&](TermPostings& attrs) noexcept { _attrs.Reset(attrs); };

  if (!_volatile_attributes) {
    refresh(docs);
  } else {
    docs.Subscribe(refresh);
  }

  BeginTerm(meta);
  ApplyToWriter([&](auto& writer) { writer.Reset(); });

  const bool has_vec = _features.HasVector();
  if (has_vec) {
    _term_docs.clear();
  }
  const bool has_freq = _features.HasFrequency();
  const bool has_pos = _features.HasPosition();

  uint32_t docs_count = 0;
  uint32_t total_freq = 0;

  while (true) {
    const auto doc = docs.Advance();
    SDB_ASSERT(doc_limits::valid(doc));
    if (doc_limits::eof(doc)) {
      break;
    }
    const uint32_t freq = has_freq ? docs.GetFreq() : 0;
    if (has_vec) {
      _term_docs.push_back(doc);
    }

    if (_doc.last >= doc) [[unlikely]] {
      throw IndexError{
        absl::StrCat("While beginning document in postings_writer, error: "
                     "docs out of order '",
                     doc, "' < '", _doc.last, "'")};
    }

    if (doc_limits::valid(_doc.last) && _doc.Empty()) {
      if (!_skip_armed) {
        ArmSkip(meta);
      }
      _skip.Skip(docs_count, [this](size_t level, MemoryIndexOutput& out) {
        WriteSkip(level, out);

        ApplyToWriter([&](auto& writer) {
          const uint8_t size = writer.Size(level);
          SDB_ASSERT(size <= ScoreBoundWriter::kMaxSize);
          out.WriteByte(size);
        });
        ApplyToWriter([&](auto& writer) { writer.Write(level, out); });
      });
    }

    if (has_freq) {
      _doc.Push(doc, freq);
    } else {
      _doc.Push(doc);
    }
    if (_doc.Full()) {
      FormatTraits::WriteBlockDelta(*_doc_out, _doc.docs, _doc.block_last,
                                    _enc_buf);
      if (has_freq) {
        FormatTraits::WriteBlock(*_doc_out, _doc.freqs, _enc_buf);
      }
      _doc.block_last = _doc.last;
      _doc.size = 0;
    }

    _docs.set(doc);

    // First position offsets now is format dependent
    _pos.last = pos_limits::invalid();
    _pay.last = 0;

    if (_valid_writer) {
      _attrs.doc.value = doc;
      _attrs.freq.value = freq;
      _valid_writer->Update();
    }
    if (has_pos) {
      SDB_ASSERT(_attrs.pos);
      while (_attrs.pos->next()) {
        SDB_ASSERT(pos_limits::valid(_attrs.pos->value()));
        AddPosition(_attrs.pos->value());
      }
    }
    ++docs_count;
    total_freq += freq;
  }

  meta.docs_count = docs_count;
  meta.freq = total_freq;
  EndTerm(meta);

  // Stream this term's fixed-width per-document payload (e.g. IVF quantized
  // codes) into ".pay", contiguous per term.
  if (has_vec) {
    SDB_ASSERT(_pay_out && _term_pay);
    meta.pay_start = _pay_out->Position();
    meta.pos_offset = _term_pay->PendingLanes();
    _term_pay->WriteTermPayload(*_pay_out, _term_docs);
  }
}

}  // namespace irs
