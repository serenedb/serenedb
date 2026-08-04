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
#include "iresearch/formats/seek_cookie.hpp"

namespace irs {

// Buffer for one block of documents and their frequencies
struct DocBuffer {
  bool Full() const noexcept { return size == doc_limits::kBlockSize; }

  bool Empty() const noexcept { return size == 0; }

  void Push(doc_id_t doc, uint32_t freq) noexcept {
    docs[size] = doc;
    freqs[size] = freq;
    ++size;
    last = doc;
  }

  doc_id_t docs[doc_limits::kBlockSize]{};
  uint32_t freqs[doc_limits::kBlockSize]{};
  doc_id_t skip_doc[doc_limits::kMaxSkipLevels]{};
  uint64_t skip_ptr[doc_limits::kMaxSkipLevels]{};
  uint32_t size{};
  doc_id_t last{doc_limits::invalid()};        // last buffered document id
  doc_id_t block_last{doc_limits::invalid()};  // last document id in a block
};

// Buffer for one block of position deltas
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
  uint32_t size{};  // number of buffered elements
  uint32_t last{};  // last buffered position
};

// Buffer for one block of position offsets
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
  uint32_t size{};  // number of buffered elements
  uint32_t last{};  // last start offset
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

  FieldStats EndField() final {
    if (_features.HasPayload() && _term_pay != nullptr) {
      SDB_ASSERT(_pay_out);
      // One partial pack per stream: the field's last one is flushed here.
      _term_pay->Finish(*_pay_out);
    }
    // A transplanted run's documents never pass through the bitset, so a
    // field with any of them always arrives with the count already known.
    const auto count = _known_docs != 0 ? size_t{_known_docs} : _docs.count();
    _known_docs = 0;
    SDB_ASSERT(count < doc_limits::eof());
    return {.has_wand = _valid_writer != nullptr,
            .docs_count = static_cast<doc_id_t>(count),
            .pay_base = _pay_base};
  }

  void SetTermPayloadWriter(TermPayloadWriter* writer) final {
    _term_pay = writer;
  }

  void SetKnownDocsCount(doc_id_t count) final { _known_docs = count; }

  void Prepare(IndexOutput& out, const FlushState& state) final;

 protected:
  explicit PostingsWriterBase(IResourceManager& rm)
    : _skip{doc_limits::kBlockSize, doc_limits::kSkipSize, rm} {}

  class Features {
   public:
    void Reset(IndexFeatures features) noexcept {
      _has_freq = (IndexFeatures::None != (features & IndexFeatures::Freq));
      _has_pos = (IndexFeatures::None != (features & IndexFeatures::Pos));
      _has_offs = (IndexFeatures::None != (features & IndexFeatures::Offs));
      _has_pay = (IndexFeatures::None != (features & IndexFeatures::Pay));
    }

    bool HasFrequency() const noexcept { return _has_freq; }
    bool HasPosition() const noexcept { return _has_pos; }
    bool HasOffset() const noexcept { return _has_offs; }
    bool HasPayload() const noexcept { return _has_pay; }

   private:
    bool _has_freq{};
    bool _has_pos{};
    bool _has_offs{};
    bool _has_pay{};
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

  // The streams' positions at one instant. A run starts at one mark and ends
  // at the next; everything the dictionary record spells about the run is the
  // distance between the two.
  struct StreamMark {
    uint64_t doc;
    uint64_t pos;
    uint64_t pay;
  };

  StreamMark Mark() const noexcept {
    return {.doc = _doc_out->Position(),
            .pos = _features.HasPosition() ? _pos_out->Position() : 0,
            .pay = _features.HasOffset() ? _pay_out->Position() : 0};
  }

  void WriteSkip(size_t level, MemoryIndexOutput& out);
  // Arms the skip writer for the run that just filled its first block: skip
  // pointers base at the run's stream starts. Nothing is armed for the runs
  // that never fill one.
  void ArmSkip();
  // The wand score bound of a whole run: its size byte, then its bytes.
  void WriteRoot(size_t level) {
    ApplyToWriter([&](auto& writer) {
      const uint8_t size = writer.SizeRoot(level);
      _doc_out->WriteByte(size);
    });
    ApplyToWriter([&](auto& writer) { writer.WriteRoot(level, *_doc_out); });
  }
  void PrepareWriters(const FieldProperties& meta);

  template<typename Func>
  void ApplyToWriter(Func&& func) {
    if (_valid_writer) {
      func(*_valid_writer);
    }
  }

  SkipWriter _skip;
  bitset _docs;               // Set of all processed documents
  IndexOutput::ptr _doc_out;  // Postings (doc + freq)
  IndexOutput::ptr _pos_out;  // Positions
  IndexOutput::ptr _pay_out;  // Payload (pay + offs)
  DocBuffer _doc;             // Document stream
  PosBuffer _pos;             // Proximity stream
  PayBuffer _pay;             // Payloads and offsets stream
  Attributes _attrs;          // Set of attributes
  const NormProvider* _norms{};
  WandWriter::ptr _writer;    // Wand writers
  WandWriter* _valid_writer;  // Valid wand writer
  Features _features;         // Features supported by current field
  // Per-term payload writer for IndexFeatures::Pay fields (e.g. IVF codes).
  TermPayloadWriter* _term_pay{};
  // Scratch list of the current run's document ids (collected when
  // HasPayload).
  std::vector<doc_id_t> _term_docs;
  // The payload stream of a `IndexFeatures::Pay` field: where it starts in
  // `.pay` and how many documents' codes it already holds. A term anchors on
  // the lane its first document took, whatever pack that lane falls in.
  uint64_t _pay_base{0};
  uint64_t _pay_lane{0};
  // Where the run being written started in each stream.
  StreamMark _run_base{};
  // Field docs count handed in ahead of the terms; 0 means count via `_docs`.
  doc_id_t _known_docs{0};
  // Whether the current run armed the skip writer (it does at its first block
  // fill).
  bool _skip_armed{false};
};

inline void PostingsWriterBase::PrepareWriters(const FieldProperties& meta) {
  _valid_writer = nullptr;

  if (!_norms) [[unlikely]] {
    return;
  }

  // Enable/Disable frequency for WandWriter::Prepare
  _attrs.wand_freq = _features.HasFrequency() ? &_attrs.freq : nullptr;

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
  // fixed-width per-document payloads (IndexFeatures::Pay, e.g. IVF codes).
  if (IndexFeatures::None !=
      (state.index_features & (IndexFeatures::Offs | IndexFeatures::Pay))) {
    if (IndexFeatures::None != (state.index_features & IndexFeatures::Offs)) {
      _pay.Reset();
    }
    format_utils::PrepareOutput(name, _pay_out, state, kPayExt, kPayFormatName);
  }

  _skip.Prepare(doc_limits::kMaxSkipLevels, state.doc_count);

  out.WriteV32(_skip.Skip0());  // Write postings block size

  // Prepare wand writers
  _writer = PrepareWandWriter(state.scorer, doc_limits::kMaxSkipLevels);
  _norms = state.norms;

  // Prepare documents bitset
  _docs.reset(doc_limits::min() + state.doc_count);
}

inline void PostingsWriterBase::ArmSkip() {
  std::fill_n(_doc.skip_ptr, doc_limits::kMaxSkipLevels, _run_base.doc);
  if (_features.HasPosition()) {
    std::fill_n(_pos.skip_ptr, doc_limits::kMaxSkipLevels, _run_base.pos);
    if (_features.HasOffset()) {
      std::fill_n(_pay.skip_ptr, doc_limits::kMaxSkipLevels, _run_base.pay);
    }
  }
  _skip.Reset();
  _skip_armed = true;
}

template<typename FormatTraits>
class PostingsWriterImpl final : public PostingsWriterBase {
 public:
  explicit PostingsWriterImpl(bool volatile_attributes, IResourceManager& rm)
    : PostingsWriterBase{rm}, _volatile_attributes{volatile_attributes} {}

  void BeginField(const FieldProperties& meta) final;
  void WriteTerm(DocIterator& docs, uint32_t row_group_size,
                 TermRuns& out) final;
  void End() final;

 private:
  void Subscribe(DocIterator& docs);
  void BeginRun(uint32_t rg, doc_id_t rg_base);
  // Writes one document of the current run: the skip entry its block boundary
  // owes, the id delta and frequency, the wand update, and its positions.
  // `local` is the row-group-local id, `df` how many documents the run holds
  // before this one.
  void WriteDocument(doc_id_t local, uint32_t freq, uint32_t df);
  // Emits everything a run leaves at its end -- root, tail, skip data --
  // exactly as one whole-term posting list would, and returns the run
  // measured between the stream marks it started and ended at.
  TermRuns::Run EndRun(uint32_t rg, uint32_t df, uint32_t tf);
  // Appends one transplanted run: its `.doc` bytes verbatim -- deltas, tail,
  // wand roots and skip data are all run-relative, so the copy is the
  // re-encode -- and its positions re-emitted at this writer's block phase.
  // Byte-identical to what the per-document path would produce for the same
  // run, per document work included at neither stream.
  void Transplant(const BlitRun& run, TermRuns& out);
  // Re-emits `count` position deltas that start at `slot` of the source block
  // at `block`: per-document delta restarts are encoded in the values, so a
  // phase shift repacks whole blocks and never touches a document boundary.
  void AppendBlitPositions(IndexInput& in, uint64_t block, uint32_t slot,
                           uint32_t count);
  void FlushTailDoc(bool freqs_all_one);
  void FlushTailPos();
  void FlushTailPay();
  void AddPosition(uint32_t pos);

  // The one decoded source position block transplants read through:
  // consecutive runs of one source are contiguous in `.pos`, so the block a
  // run ends inside is the block the next one starts in.
  struct PosBlockCache {
    const IndexInput* src{};
    uint64_t off{};
    uint64_t next{};
    uint32_t buf[pos_limits::kBlockSize];
  };

  // Buffer for block encoding (worst case)
  uint32_t _enc_buf[std::max(doc_limits::kBlockSize, pos_limits::kBlockSize)];
  PosBlockCache _blit_pos;
  bool _volatile_attributes;
};

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailDoc(bool freqs_all_one) {
  const auto tail = _doc.size;
  SDB_ASSERT(tail != 0);
  FormatTraits::WriteTailDelta(tail, *_doc_out, _doc.docs, _doc.block_last,
                               _enc_buf);
  // A run whose tf equals its df has every frequency at one, and the record
  // spells that already (the tfΔ column / flag), so such a run stores no
  // frequency tail; readers refill ones off the run's own df == tf.
  if (_features.HasFrequency() && !freqs_all_one) {
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

  // Lane 0 sits where the stream starts, which is only known once the previous
  // field's leftover offset block has gone out.
  if (_features.HasPayload()) {
    SDB_ASSERT(_pay_out);
    _pay_base = _pay_out->Position();
    _pay_lane = 0;
  }

  // Transplant inputs are reopened per field, so a recycled allocation could
  // otherwise masquerade as the cached block's stream.
  _blit_pos.src = nullptr;
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
  // per-document payloads (IndexFeatures::Pay, no positions).
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
void PostingsWriterImpl<FormatTraits>::Subscribe(DocIterator& docs) {
  auto refresh = [this](auto& attrs) noexcept { _attrs.Reset(attrs); };

  if (!_volatile_attributes) {
    refresh(docs);
  } else {
    auto* subscription = irs::get<AttrProviderChangeAttr>(docs);
    SDB_ASSERT(subscription);
    subscription->Subscribe(refresh);
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::BeginRun(uint32_t rg, doc_id_t rg_base) {
  if (_term_pay) {
    _term_pay->SetRowGroupBase(rg_base);
  }
  if (_valid_writer) {
    _valid_writer->SetRowGroup(rg);
    _valid_writer->Reset();
  }
  if (_features.HasPayload()) {
    _term_docs.clear();
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::WriteDocument(doc_id_t local,
                                                     uint32_t freq,
                                                     uint32_t df) {
  if (_doc.last >= local) [[unlikely]] {
    throw IndexError{
      absl::StrCat("While beginning document in postings_writer, error: "
                   "docs out of order '",
                   local, "' < '", _doc.last, "'")};
  }

  if (doc_limits::valid(_doc.last) && _doc.Empty()) {
    // The block boundary: record a skip entry before the document that starts
    // the next block.
    if (!_skip_armed) {
      ArmSkip();
    }
    _skip.Skip(df, [this](size_t level, MemoryIndexOutput& out) {
      WriteSkip(level, out);

      // FIXME(gnusi): optimize for 1 writer case? compile? maybe just 1
      // composite wand writer?
      ApplyToWriter([&](auto& writer) {
        const uint8_t size = writer.Size(level);
        SDB_ASSERT(size <= WandWriter::kMaxSize);
        out.WriteByte(size);
      });
      ApplyToWriter([&](auto& writer) { writer.Write(level, out); });
    });
  }

  _doc.Push(local, freq);
  if (_doc.Full()) {
    FormatTraits::WriteBlockDelta(*_doc_out, _doc.docs, _doc.block_last,
                                  _enc_buf);
    if (_features.HasFrequency()) {
      FormatTraits::WriteBlock(*_doc_out, _doc.freqs, _enc_buf);
    }
    _doc.block_last = _doc.last;
    _doc.size = 0;
  }

  // First position offsets now is format dependent
  _pos.last = pos_limits::invalid();
  _pay.last = 0;

  if (_valid_writer) {
    _attrs.doc.value = local;
    _attrs.freq.value = freq;
    _valid_writer->Update();
  }
  if (_features.HasPayload()) {
    _term_docs.push_back(local);
  }
  if (_features.HasPosition()) {
    SDB_ASSERT(_attrs.pos);
    while (_attrs.pos->next()) {
      SDB_ASSERT(pos_limits::valid(_attrs.pos->value()));
      AddPosition(_attrs.pos->value());
    }
  }
}

template<typename FormatTraits>
TermRuns::Run PostingsWriterImpl<FormatTraits>::EndRun(uint32_t rg, uint32_t df,
                                                       uint32_t tf) {
  SDB_ASSERT(df != 0);

  const bool has_skip_list = _skip.Skip0() < df;
  SDB_ASSERT(!has_skip_list || _skip_armed);

  doc_id_t single_doc = 0;
  uint64_t e_skip = 0;
  // A run of one document writes no `.doc` bytes and spells its id; a run past
  // one block spells where its skip data starts; anything between spells
  // neither, so the record's remaining slot stays zero.
  if (df == 1) {
    single_doc = _doc.docs[0] - doc_limits::min();
  } else {
    if (!has_skip_list) {
      WriteRoot(0);
    }
    if ((df & (_skip.Skip0() - 1)) != 0) {
      FlushTailDoc(tf == df);
    }
  }

  // if we have flushed at least
  // one block there was buffered
  // skip data, so we need to flush it
  if (has_skip_list) {
    e_skip = _doc_out->Position() - _run_base.doc;
    const auto num_levels = _skip.CountLevels();
    WriteRoot(num_levels);
    _skip.FlushLevels(num_levels, *_doc_out);
  }

  _doc.size = 0;
  _doc.last = doc_limits::invalid();
  _doc.block_last = doc_limits::invalid();
  _skip_armed = false;

  _pos.last = pos_limits::invalid();

  _pay.last = 0;

  // Stream this run's fixed-width per-document payload (e.g. IVF quantized
  // codes) into ".pay", contiguous per (term, row group).
  if (_features.HasPayload()) {
    SDB_ASSERT(_pay_out && _term_pay);
    _term_pay->WriteTermPayload(*_pay_out, _term_docs);
  }

  const StreamMark end = Mark();
  const TermRuns::Run run{.rg = rg,
                          .df = df,
                          .tf = tf,
                          .single_doc = single_doc,
                          .run_len = end.doc - _run_base.doc,
                          .e_skip = e_skip,
                          .pos_delta = end.pos - _run_base.pos,
                          .pay_delta = end.pay - _run_base.pay};
  SDB_ASSERT(run.df != 1 || run.run_len == 0);
  _run_base = end;
  return run;
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::Transplant(const BlitRun& run,
                                                  TermRuns& out) {
  SDB_ASSERT(_doc.Empty());
  SDB_ASSERT(!_skip_armed);
  SDB_ASSERT(!_features.HasOffset() && !_features.HasPayload() && !_term_pay);
  SDB_ASSERT(run.df != 1 || run.doc_len == 0);
  if (run.doc_len != 0) {
    auto& in = *run.doc_in;
    if (const auto* data = in.ReadStable(run.doc_start, run.doc_len))
      [[likely]] {
      _doc_out->WriteData(data, run.doc_len);
    } else {
      in.Seek(run.doc_start);
      auto* buf = reinterpret_cast<byte_type*>(_enc_buf);
      for (uint64_t left = run.doc_len; left != 0;) {
        const auto n = std::min<uint64_t>(left, sizeof(_enc_buf));
        in.ReadData(buf, n);
        _doc_out->WriteData(buf, n);
        left -= n;
      }
    }
  }
  if (_features.HasPosition() && run.tf != 0) {
    AppendBlitPositions(*run.pos_in, run.pos_start, run.pos_slot, run.tf);
  }
  const StreamMark end = Mark();
  SDB_ASSERT(end.doc - _run_base.doc == run.doc_len);
  out.runs.push_back(
    {.rg = run.rg,
     .df = run.df,
     .tf = run.tf,
     .single_doc = run.df == 1 ? run.inlined : 0,
     .run_len = run.doc_len,
     .e_skip = run.df > doc_limits::kBlockSize ? run.inlined : 0,
     .pos_delta = end.pos - _run_base.pos,
     .pay_delta = 0});
  out.docs_count += run.df;
  out.freq += run.tf;
  _run_base = end;
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::AppendBlitPositions(IndexInput& in,
                                                           uint64_t block,
                                                           uint32_t slot,
                                                           uint32_t count) {
  SDB_ASSERT(_pos_out);
  SDB_ASSERT(slot < pos_limits::kBlockSize);
  SDB_ASSERT(count != 0);
  auto& cache = _blit_pos;
  while (true) {
    if (cache.src != &in || cache.off != block) {
      in.Seek(block);
      FormatTraits::ReadBlock(in, _enc_buf, cache.buf);
      cache.src = &in;
      cache.off = block;
      cache.next = in.Position();
    }
    uint32_t take = std::min(count, pos_limits::kBlockSize - slot);
    count -= take;
    while (take != 0) {
      const uint32_t n = std::min(take, pos_limits::kBlockSize - _pos.size);
      std::memcpy(_pos.buf + _pos.size, cache.buf + slot, n * sizeof(uint32_t));
      _pos.size += n;
      slot += n;
      take -= n;
      if (_pos.size == pos_limits::kBlockSize) {
        FormatTraits::WriteBlock(*_pos_out, _pos.buf, _enc_buf);
        _pos.size = 0;
      }
    }
    if (count == 0) {
      return;
    }
    block = cache.next;
    slot = 0;
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::WriteTerm(DocIterator& docs,
                                                 uint32_t row_group_size,
                                                 TermRuns& out) {
  Subscribe(docs);

  out.runs.clear();
  out.docs_count = 0;
  out.freq = 0;
  _run_base = Mark();
  out.doc_start = _run_base.doc;
  out.pos_ptr = _run_base.pos;
  out.pos_anchor = _features.HasPosition() ? _pos.size : 0;
  // A payload term anchors on a lane, not a byte: its documents' codes are
  // the next `df` lanes of the field's one stream, so every run of it is
  // placed by the record's own df sums.
  out.pay_ptr = _features.HasPayload() ? _pay_lane : _run_base.pay;

  // Runs a merge hands over as bytes: theirs are output row groups the doc
  // iterator never yields, so the two interleave by row group alone. The
  // transplanted `.doc` bytes claim wand roots exactly when the source wrote
  // them, so disagreement with this writer would corrupt the plane.
  std::span<const BlitRun> blits;
  size_t blitted = 0;
  if (_volatile_attributes) {
    if (const auto* plan = irs::get<BlitPlanAttr>(docs)) [[likely]] {
      SDB_ENSURE(
        plan->runs.empty() || plan->has_wand == (_valid_writer != nullptr),
        "postings_writer: transplant plan wand state diverges from "
        "the writer's");
      blits = plan->runs;
    }
  }

  // A run of one document is inlined in the dictionary record and nothing is
  // written to any stream for it. Reading the run's length costs a one
  // document lookahead, and that lookahead is legal exactly when the streams
  // hold nothing per position -- positions, offsets and a term payload all
  // have to be drained before `advance()` clobbers them. A positional
  // df == 1 run flows through the run loop, which emits no `.doc` bytes for
  // it either.
  const bool inline_singletons = !_term_pay && !_features.HasPosition() &&
                                 !_features.HasOffset() &&
                                 !_features.HasPayload();
  const bool has_freq = _features.HasFrequency();

  auto doc = docs.advance();
  while (true) {
    if (blitted != blits.size()) [[unlikely]] {
      const uint32_t next_rg = doc_limits::eof(doc)
                                 ? std::numeric_limits<uint32_t>::max()
                                 : (doc - doc_limits::min()) / row_group_size;
      while (blitted != blits.size() && blits[blitted].rg < next_rg) {
        SDB_ASSERT(blitted == 0 || blits[blitted - 1].rg < blits[blitted].rg);
        Transplant(blits[blitted], out);
        ++blitted;
      }
    }
    if (doc_limits::eof(doc)) {
      break;
    }
    SDB_ASSERT(doc_limits::valid(doc));
    const uint32_t rg = (doc - doc_limits::min()) / row_group_size;
    const doc_id_t rg_base = rg * row_group_size;
    const doc_id_t limit = doc_limits::min() + rg_base + row_group_size;
    uint32_t freq = has_freq ? docs.GetFreq() : 0;
    uint32_t df = 0;
    uint32_t tf = 0;

    if (inline_singletons) {
      const auto first = doc;
      const auto first_freq = freq;
      doc = docs.advance();
      if (doc >= limit) {
        _docs.set(first);
        out.runs.push_back({.rg = rg,
                            .df = 1,
                            .tf = first_freq,
                            .single_doc = first - rg_base - doc_limits::min()});
        out.docs_count += 1;
        out.freq += first_freq;
        continue;
      }
      BeginRun(rg, rg_base);
      _docs.set(first);
      WriteDocument(first - rg_base, first_freq, 0);
      df = 1;
      tf = first_freq;
      freq = has_freq ? docs.GetFreq() : 0;
    } else {
      BeginRun(rg, rg_base);
    }

    while (true) {
      SDB_ASSERT(doc_limits::valid(doc));
      _docs.set(doc);
      WriteDocument(doc - rg_base, freq, df);
      ++df;
      tf += freq;
      doc = docs.advance();
      if (doc >= limit) {
        break;
      }
      if (has_freq) {
        freq = docs.GetFreq();
      }
    }

    out.runs.push_back(EndRun(rg, df, tf));
    out.docs_count += df;
    out.freq += tf;
  }

  if (_features.HasPayload()) {
    _pay_lane += out.docs_count;
  }
}

}  // namespace irs
