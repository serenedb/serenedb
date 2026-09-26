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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/utils/containers/bitset.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

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
  uint32_t size{};  // number of buffered offsets
  uint32_t last{};  // last start offset
};

class BlockGroup {
 public:
  bool Empty() const noexcept { return _blocks == 0; }

  uint32_t Blocks() const noexcept { return _blocks; }

  void Reset() noexcept {
    _data.clear();
    _blocks = 0;
  }

  void Append(const uint32_t* data, uint32_t size) {
    const auto* bytes = reinterpret_cast<const byte_type*>(data);
    _data.insert(_data.end(), bytes, bytes + size);
  }

  void Close(IndexOutput& out) {
    SDB_ASSERT(_blocks < PosGroup::kBlocks);
    SDB_ASSERT(_data.size() <= std::numeric_limits<uint16_t>::max());
    _ends[_blocks] = static_cast<uint16_t>(_data.size());
    if (++_blocks == PosGroup::kBlocks) {
      Flush(out);
    }
  }

  void Flush(IndexOutput& out) {
    SDB_ASSERT(_blocks != 0);
    byte_type header[PosGroup::kHeaderBytes];
    for (uint32_t i = 0; i != PosGroup::kBlocks; ++i) {
      absl::little_endian::Store16(header + i * sizeof(uint16_t),
                                   _ends[std::min(i, _blocks - 1)]);
    }
    out.WriteData(header, sizeof(header));
    out.WriteData(_data.data(), _data.size());
    _data.clear();
    _blocks = 0;
  }

 private:
  std::vector<byte_type> _data;
  uint16_t _ends[PosGroup::kBlocks]{};
  uint32_t _blocks = 0;
};

inline ScoreBoundWriter::ptr PrepareScoreBoundWriter(ScorerPtr scorer) {
  ScoreBoundWriter::ptr writer = nullptr;
  if (scorer) {
    writer = (*scorer).PrepareScoreBoundWriter();
  }
  return writer;
}

class PostingsWriterBase : public PostingsWriter {
 public:
  static constexpr std::string_view kDocFormatName =
    "iresearch_11_postings_documents";
  static constexpr std::string_view kDocExt = "doc";
  static constexpr std::string_view kPosFormatName =
    "iresearch_11_postings_positions";
  static constexpr std::string_view kPosExt = "pos";
  static constexpr std::string_view kPayFormatName =
    "iresearch_11_postings_payloads";
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
  explicit PostingsWriterBase(IResourceManager&) noexcept {}

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

  void AddBlock(const PostingMeta& meta, doc_id_t last);
  void BeginTerm(PostingMeta& meta);
  virtual void FlushTailDoc() = 0;
  void EndTerm(PostingMeta& meta);
  void PrepareWriters(const FieldProperties& meta);

  uint32_t PosIndex() const noexcept {
    const auto index = _pos_group.Blocks() * pos_limits::kBlockSize + _pos.size;
    SDB_ASSERT(index < PosGroup::kPositions);
    return index;
  }

  void FlushGroups() {
    if (!_pos_group.Empty()) {
      _pos_group.Flush(*_pos_out);
    }
    if (!_pay_group.Empty()) {
      _pay_group.Flush(*_pay_out);
    }
  }

  template<typename Func>
  void ApplyToWriter(Func&& func) {
    if (_valid_writer) {
      func(*_valid_writer);
    }
  }

  BlockIndexWriter _index;
  PostingMeta _last_state;    // Last final term state
  bitset _docs;               // Set of all processed documents
  IndexOutput::ptr _doc_out;  // Postings (doc + freq)
  IndexOutput::ptr _pos_out;  // Positions
  IndexOutput::ptr _pay_out;  // Payload (pay + offs)
  DocBuffer _doc;             // Document stream
  PosBuffer _pos;             // Proximity stream
  PayBuffer _pay;             // Payloads and offsets stream
  BlockGroup _pos_group;
  BlockGroup _pay_group;
  Attributes _attrs;  // Set of attributes
  const NormProvider* _norms{};
  ScoreBoundWriter::ptr _writer;      // Score bound writer
  ScoreBoundWriter* _valid_writer{};  // Valid score bound writer
  Features _features;                 // Features supported by current field
  // Per-term payload writer for IndexFeatures::Vec fields (e.g. IVF codes).
  TermPayloadWriter* _term_pay{};
  // Scratch list of the current term's document ids (collected when
  // HasVector).
  std::vector<doc_id_t> _term_docs;
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

inline void PostingsWriterBase::AddBlock(const PostingMeta& meta,
                                         doc_id_t last) {
  SDB_ASSERT(_doc_out);
  uint64_t pos_group = 0;
  uint32_t pos_index = 0;
  uint64_t pay_group = 0;
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    pos_group = _pos_out->Position() - meta.pos_start;
    pos_index = PosIndex();
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      pay_group = _pay_out->Position() - meta.pay_start;
    }
  }
  _index.Add(last, _doc_out->Position() - meta.doc_start, pos_group, pos_index,
             pay_group);
  ApplyToWriter([&](auto& writer) {
    writer.Take(0, _index.AddBound());
    if (_index.Size() % BlockIndex::kRun == 0) {
      writer.Take(1, _index.AddRunBound());
    }
  });
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
    _pos_group.Reset();
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
    _pay_group.Reset();
  }
  if (has_offs || has_vec) {
    format_utils::PrepareOutput(name, _pay_out, state, kPayExt, kPayFormatName);
  }

  out.WriteV32(doc_limits::kBlockSize);  // Write postings block size

  _writer = PrepareScoreBoundWriter(state.scorer);
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
    const uint64_t pos_delta = meta.pos_start - _last_state.pos_start;
    out.WriteV64(pos_delta);
    if (_features.HasOffset()) {
      out.WriteV64(meta.pay_start - _last_state.pay_start);
    }
    SDB_ASSERT(pos_delta != 0 || _last_state.pos_offset <= meta.pos_offset);
    out.WriteV32(pos_delta == 0 ? meta.pos_offset - _last_state.pos_offset
                                : meta.pos_offset);
  } else if (_features.HasVector()) {
    out.WriteV64(meta.pay_start - _last_state.pay_start);
    SDB_ASSERT(meta.pos_offset <= std::numeric_limits<uint8_t>::max());
    out.WriteByte(meta.pos_offset);
  }

  if (meta.docs_count == 1 || meta.docs_count > doc_limits::kBlockSize) {
    out.WriteV32(meta.doc_delta);
  }

  _last_state = meta;
}

inline void PostingsWriterBase::BeginTerm(PostingMeta& meta) {
  _index.Reset();
  meta.doc_start = _doc_out->Position();
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    meta.pos_start = _pos_out->Position();
    if (_features.HasOffset()) {
      SDB_ASSERT(_pay_out);
      meta.pay_start = _pay_out->Position();
    }
    meta.pos_offset = PosIndex();
  }
}

inline void PostingsWriterBase::EndTerm(PostingMeta& meta) {
  if (meta.docs_count == 0) {
    return;  // no documents to write
  }

  const bool has_skip_list = doc_limits::kBlockSize < meta.docs_count;
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
    if (meta.docs_count < doc_limits::kBlockSize) {
      write_max_score(0);
    }
    if ((meta.docs_count & (doc_limits::kBlockSize - 1)) != 0) {
      FlushTailDoc();
    }
  }

  if (has_skip_list) {
    AddBlock(meta, _doc.last);
    if (_index.Size() % BlockIndex::kRun != 0) {
      ApplyToWriter(
        [&](auto& writer) { writer.Take(1, _index.AddRunBound()); });
    }
    const uint64_t skip_start = _doc_out->Position() - meta.doc_start;
    SDB_ENSURE(skip_start <= std::numeric_limits<uint32_t>::max(),
               "postings writer: a single term's `.doc` footprint of ",
               skip_start, " bytes exceeds the ",
               std::numeric_limits<uint32_t>::max(), " byte limit");
    meta.doc_delta = static_cast<uint32_t>(skip_start);
    ApplyToWriter([&](auto& writer) { writer.Take(2, _index.Root()); });
    _index.Write(*_doc_out, {.pos = _features.HasPosition(),
                             .offs = _features.HasOffset(),
                             .bounds = _valid_writer != nullptr});
  }

  _doc.size = 0;
  _doc.last = doc_limits::invalid();
  _doc.block_last = doc_limits::invalid();

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
  bool WritePostings(const PostingRows& postings, PostingMeta& meta) final;
  void End() final;

 private:
  void FlushTailDoc() final;
  void FlushTailPos();
  void FlushTailPay();
  void WritePosBlock();
  void WritePayBlock();
  template<bool HasOffs>
  void PushPosition(uint32_t pos, uint32_t offs_start = 0,
                    uint32_t offs_end = 0);
  template<bool HasOffs>
  void PushPositions(const uint32_t* pos, const uint32_t* offs_start,
                     const uint32_t* offs_end, size_t n);
  void AddPosition(uint32_t pos);
  void BeginDocInTerm(doc_id_t doc, uint32_t freq, PostingMeta& meta,
                      bool has_freq);

  uint32_t _enc_buf[FormatTraits::kEncWords];
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
  WritePosBlock();
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::FlushTailPay() {
  SDB_ASSERT(_pay_out);
  SDB_ASSERT(_pay.size != 0);
  const auto tail_size = doc_limits::kBlockSize - _pay.size;
  SDB_ASSERT(tail_size != 0);

  auto* offs_start_tail = _pay.offs_start_buf + _pay.size;
  std::fill_n(offs_start_tail, tail_size, offs_start_tail[-1]);
  auto* offs_len_tail = _pay.offs_len_buf + _pay.size;
  std::fill_n(offs_len_tail, tail_size, offs_len_tail[-1]);
  WritePayBlock();
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::WritePosBlock() {
  SDB_ASSERT(_pos_out);
  _pos_group.Append(_enc_buf, FormatTraits::EncodeBlock(_pos.buf, _enc_buf));
  _pos_group.Close(*_pos_out);
  _pos.size = 0;
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::WritePayBlock() {
  SDB_ASSERT(_pay_out);
  _pay_group.Append(_enc_buf,
                    FormatTraits::EncodeBlock(_pay.offs_start_buf, _enc_buf));
  _pay_group.Append(_enc_buf,
                    FormatTraits::EncodeBlock(_pay.offs_len_buf, _enc_buf));
  _pay_group.Close(*_pay_out);
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
    if (_pos.size != _pay.size || _pos_group.Blocks() != _pay_group.Blocks())
      [[unlikely]] {
      SDB_ASSERT(_pay.size == 0);
      if (_pos.size != 0) {
        FlushTailPos();
      }
      FlushGroups();
    }
  } else if (_pay.size != 0 || !_pay_group.Empty()) [[unlikely]] {
    if (_pay.size != 0) {
      FlushTailPos();
      FlushTailPay();
    }
    FlushGroups();
  }
}

template<typename FormatTraits>
template<bool HasOffs>
void PostingsWriterImpl<FormatTraits>::PushPosition(uint32_t pos,
                                                    uint32_t offs_start,
                                                    uint32_t offs_end) {
  // at least positions stream should be created
  SDB_ASSERT(_features.HasPosition());
  SDB_ASSERT(_features.HasOffset() == HasOffs);

  SDB_ASSERT(_pos.size == _pay.size || _pay.size == 0);
  _pos.Next(pos);
  if constexpr (HasOffs) {
    _pay.PushOffset(offs_start, offs_end);
  }
  SDB_ASSERT(_pos.size == _pay.size || _pay.size == 0);

  if (_pos.Full()) [[unlikely]] {
    WritePosBlock();
    if constexpr (HasOffs) {
      SDB_ASSERT(_pay.size != 0);
      WritePayBlock();
    }
  }
}

// Bulk form of PushPosition for one doc's position run handed over as
// columns: deltas land block-chunk-at-a-time (the inner subtract loops
// vectorize), the block-full check runs once per chunk instead of once per
// position.
template<typename FormatTraits>
template<bool HasOffs>
void PostingsWriterImpl<FormatTraits>::PushPositions(const uint32_t* pos,
                                                     const uint32_t* offs_start,
                                                     const uint32_t* offs_end,
                                                     size_t n) {
  SDB_ASSERT(_features.HasPosition());
  SDB_ASSERT(_features.HasOffset() == HasOffs);

  size_t k = 0;
  while (k < n) {
    SDB_ASSERT(_pos.size < pos_limits::kBlockSize);
    const size_t take =
      std::min<size_t>(pos_limits::kBlockSize - _pos.size, n - k);

    uint32_t* out = _pos.buf + _pos.size;
    uint32_t last = _pos.last;
    for (size_t m = 0; m < take; ++m) {
      SDB_ASSERT(last <= pos[k + m]);
      out[m] = pos[k + m] - last;
      last = pos[k + m];
    }
    _pos.last = last;
    _pos.size += static_cast<uint32_t>(take);

    if constexpr (HasOffs) {
      uint32_t* sb = _pay.offs_start_buf + _pay.size;
      uint32_t* lb = _pay.offs_len_buf + _pay.size;
      uint32_t last_start = _pay.last;
      for (size_t m = 0; m < take; ++m) {
        SDB_ASSERT(last_start <= offs_start[k + m]);
        SDB_ASSERT(offs_start[k + m] <= offs_end[k + m]);
        sb[m] = offs_start[k + m] - last_start;
        lb[m] = offs_end[k + m] - offs_start[k + m];
        last_start = offs_start[k + m];
      }
      _pay.last = last_start;
      _pay.size += static_cast<uint32_t>(take);
    }
    SDB_ASSERT(_pos.size == _pay.size || _pay.size == 0);

    if (_pos.Full()) [[unlikely]] {
      WritePosBlock();
      if constexpr (HasOffs) {
        SDB_ASSERT(_pay.size != 0);
        WritePayBlock();
      }
    }
    k += take;
  }
}

template<typename FormatTraits>
void PostingsWriterImpl<FormatTraits>::AddPosition(uint32_t pos) {
  SDB_ASSERT(!_features.HasOffset() == !_attrs.offs);
  if (_features.HasOffset()) {
    PushPosition<true>(pos, _attrs.offs->start, _attrs.offs->end);
  } else {
    PushPosition<false>(pos);
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
    if (!_pos_group.Empty()) {
      _pos_group.Flush(*_pos_out);
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
    if (!_pay_group.Empty()) {
      _pay_group.Flush(*_pay_out);
    }
    format_utils::WriteFooter(*_pay_out);
    _pay_out.reset();  // ensure stream is closed
  } else {
    SDB_ASSERT(_pay.size == 0);
  }
}

template<typename FormatTraits>
IRS_FORCE_INLINE void PostingsWriterImpl<FormatTraits>::BeginDocInTerm(
  doc_id_t doc, uint32_t freq, PostingMeta& meta, bool has_freq) {
  if (_doc.last >= doc) [[unlikely]] {
    throw IndexError{
      absl::StrCat("While beginning document in postings_writer, error: "
                   "docs out of order '",
                   doc, "' < '", _doc.last, "'")};
  }

  if (doc_limits::valid(_doc.last) && _doc.Empty()) {
    AddBlock(meta, _doc.block_last);
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
    const auto doc = docs.Next();
    SDB_ASSERT(doc_limits::valid(doc));
    if (doc_limits::eof(doc)) {
      break;
    }
    const uint32_t freq = has_freq ? docs.GetFreq() : 0;
    if (has_vec) {
      _term_docs.push_back(doc);
    }

    BeginDocInTerm(doc, freq, meta, has_freq);

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

// Span fast path: the same per-doc protocol as Write, fed straight from
// the term's row columns instead of per-doc iterator dispatch; the row
// walk itself lives in TermPostings::VisitRuns.
template<typename FormatTraits>
bool PostingsWriterImpl<FormatTraits>::WritePostings(
  const PostingRows& postings, PostingMeta& meta) {
  BeginTerm(meta);
  ApplyToWriter([&](auto& writer) { writer.Reset(); });

  const bool has_vec = _features.HasVector();
  if (has_vec) {
    _term_docs.clear();
  }
  const bool has_freq = _features.HasFrequency();
  const bool has_pos = _features.HasPosition();
  const bool has_offs = _features.HasOffset();

  _attrs.pos = nullptr;
  _attrs.offs = nullptr;

  uint32_t docs_count = 0;
  uint32_t total_freq = 0;

  const auto begin_doc = [&](doc_id_t doc, uint32_t occurrences)
                           IRS_FORCE_INLINE {
                             const uint32_t freq = has_freq ? occurrences : 0;
                             if (has_vec) {
                               _term_docs.push_back(doc);
                             }
                             BeginDocInTerm(doc, freq, meta, has_freq);
                             ++docs_count;
                             total_freq += freq;
                           };

  SDB_ASSERT(!has_pos || postings.span.pos != nullptr ||
             postings.pos_blocks != nullptr);
  SDB_ASSERT(!has_pos || !has_offs || postings.span.offs_start != nullptr ||
             postings.offs_start_blocks != nullptr);
  if (has_pos) {
    ResolveBool(has_offs, [&]<bool HasOffs> {
      const auto emit_positions =
        [&](const uint32_t* pos, const uint32_t* offs_start,
            const uint32_t* offs_end, size_t n) IRS_FORCE_INLINE {
          if (n == 1) [[likely]] {
            if constexpr (HasOffs) {
              PushPosition<HasOffs>(pos[0], offs_start[0], offs_end[0]);
            } else {
              PushPosition<HasOffs>(pos[0]);
            }
          } else {
            PushPositions<HasOffs>(pos, offs_start, offs_end, n);
          }
        };

      postings.VisitRuns(begin_doc, emit_positions);
    });
  } else {
    postings.VisitRuns(begin_doc);
  }

  meta.docs_count = docs_count;
  meta.freq = total_freq;
  EndTerm(meta);

  if (has_vec) {
    SDB_ASSERT(_pay_out && _term_pay);
    meta.pay_start = _pay_out->Position();
    meta.pos_offset = _term_pay->PendingLanes();
    _term_pay->WriteTermPayload(*_pay_out, _term_docs);
  }
  return true;
}

}  // namespace irs
