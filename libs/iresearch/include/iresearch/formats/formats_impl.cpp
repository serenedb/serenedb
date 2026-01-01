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

#include <absl/algorithm/container.h>

#include "basics/assert.h"
#include "basics/resource_manager.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"
extern "C" {
#include <simdbitpacking.h>
}

#include <limits>

#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "columnstore2.hpp"
#include "format_utils.hpp"
#include "formats.hpp"
#include "formats_attributes.hpp"
#include "formats_burst_trie.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/bitpack.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "skip_list.hpp"

namespace irs {
namespace {

constexpr IndexFeatures kPos = IndexFeatures::Freq | IndexFeatures::Pos;
constexpr IndexFeatures kPosOffs = kPos | IndexFeatures::Offs;
constexpr IndexFeatures kPosPay = kPos | IndexFeatures::Pay;

void WriteStrings(IndexOutput& out, const auto& strings) {
  SDB_ASSERT(strings.size() < std::numeric_limits<uint32_t>::max());

  out.WriteV32(static_cast<uint32_t>(strings.size()));
  for (const auto& s : strings) {
    WriteStr(out, s);
  }
}

std::vector<std::string> ReadStrings(IndexInput& in) {
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

struct FormatTraits {
  using AlignType = uint32_t;

  static constexpr uint32_t kBlockSize = 128;
  static_assert(kBlockSize <= doc_limits::eof());

  IRS_FORCE_INLINE static void PackBlock(const uint32_t* IRS_RESTRICT decoded,
                                         uint32_t* IRS_RESTRICT encoded,
                                         uint32_t bits) noexcept {
    packed::PackBlock(decoded, encoded, bits);
    packed::PackBlock(decoded + packed::kBlockSize32, encoded + bits, bits);
    packed::PackBlock(decoded + 2 * packed::kBlockSize32, encoded + 2 * bits,
                      bits);
    packed::PackBlock(decoded + 3 * packed::kBlockSize32, encoded + 3 * bits,
                      bits);
  }

  IRS_FORCE_INLINE static void UnpackBlock(uint32_t* IRS_RESTRICT decoded,
                                           const uint32_t* IRS_RESTRICT encoded,
                                           uint32_t bits) noexcept {
    packed::UnpackBlock(encoded, decoded, bits);
    packed::UnpackBlock(encoded + bits, decoded + packed::kBlockSize32, bits);
    packed::UnpackBlock(encoded + 2 * bits, decoded + 2 * packed::kBlockSize32,
                        bits);
    packed::UnpackBlock(encoded + 3 * bits, decoded + 3 * packed::kBlockSize32,
                        bits);
  }

  IRS_FORCE_INLINE static void write_block(IndexOutput& out, const uint32_t* in,
                                           uint32_t* buf) {
    bitpack::write_block32<kBlockSize>(PackBlock, out, in, buf);
  }

  IRS_FORCE_INLINE static void read_block(IndexInput& in, uint32_t* buf,
                                          uint32_t* out) {
    bitpack::read_block32<kBlockSize>(UnpackBlock, in, buf, out);
  }

  IRS_FORCE_INLINE static void skip_block(IndexInput& in) {
    bitpack::skip_block32(in, kBlockSize);
  }
};

template<typename T, typename M>
std::string FileName(const M& meta);

void PrepareOutput(std::string& str, IndexOutput::ptr& out,
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

void PrepareInput(std::string& str, IndexInput::ptr& in, IOAdvice advice,
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
  doc_id_t last{doc_limits::invalid()};    // last buffered document id
  doc_id_t block_last{doc_limits::min()};  // last document id in a block
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
  PayBuffer(uint32_t* pay_sizes, uint32_t* offs_start_buf,
            uint32_t* offs_len_buf, uint64_t* skip_ptr) noexcept
    : SkipBuffer{skip_ptr},
      pay_sizes{pay_sizes},
      offs_start_buf{offs_start_buf},
      offs_len_buf{offs_len_buf} {}

  void PushPayload(uint32_t i, bytes_view pay) {
    if (!pay.empty()) {
      pay_buf.append(pay.data(), pay.size());
    }
    pay_sizes[i] = static_cast<uint32_t>(pay.size());
  }

  void PushOffset(uint32_t i, uint32_t start, uint32_t end) noexcept {
    SDB_ASSERT(start >= last && start <= end);

    offs_start_buf[i] = start - last;
    offs_len_buf[i] = end - start;
    last = start;
  }

  void Reset() noexcept {
    SkipBuffer::Reset();
    pay_buf.clear();
    block_last = 0;
    last = 0;
  }

  uint32_t* pay_sizes;       // buffer to store payloads sizes
  uint32_t* offs_start_buf;  // buffer to store start offsets
  uint32_t* offs_len_buf;    // buffer to store offset lengths
  bstring pay_buf;           // buffer for payload
  size_t block_last{};       // last payload buffer length in a block
  uint32_t last{};           // last start offset
};

std::vector<irs::WandWriter::ptr> PrepareWandWriters(ScorersView scorers,
                                                     size_t max_levels) {
  std::vector<irs::WandWriter::ptr> writers(
    std::min(scorers.size(), kMaxScorers));
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
class PostingsWriterBase : public irs::PostingsWriter {
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
                     uint64_t* prox_skip_ptr, uint32_t* pay_sizes,
                     uint32_t* offs_start_buf, uint32_t* offs_len_buf,
                     uint64_t* pay_skip_ptr, uint32_t* enc_buf,
                     PostingsFormat postings_format_version,
                     TermsFormat terms_format_version, IResourceManager& rm)
    : _skip{block_size, kSkipN, rm},
      _doc{docs, freqs, skip_doc, doc_skip_ptr},
      _pos{prox_buf, prox_skip_ptr},
      _pay{pay_sizes, offs_start_buf, offs_len_buf, pay_skip_ptr},
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
      _has_pay = (IndexFeatures::None != (features & IndexFeatures::Pay));
    }

    bool HasFrequency() const noexcept { return _has_freq; }
    bool HasPosition() const noexcept { return _has_pos; }
    bool HasOffset() const noexcept { return _has_offs; }
    bool HasPayload() const noexcept { return _has_pay; }
    bool HasOffsetOrPayload() const noexcept { return _has_offs | _has_pay; }

   private:
    bool _has_freq{};
    bool _has_pos{};
    bool _has_offs{};
    bool _has_pay{};
  };

  struct Attributes final : AttributeProvider {
    irs::DocAttr doc;
    irs::FreqAttr freq_value;

    const FreqAttr* freq{};
    PosAttr* pos{};
    const OffsAttr* offs{};
    const PayAttr* pay{};

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
      pos = &irs::PosAttr::empty();
      offs = nullptr;
      pay = nullptr;

      freq = irs::get<FreqAttr>(attrs);
      if (freq) {
        if (auto* p = irs::GetMutable<irs::PosAttr>(&attrs)) {
          pos = p;
          offs = irs::get<irs::OffsAttr>(*pos);
          pay = irs::get<irs::PayAttr>(*pos);
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
  std::vector<irs::WandWriter::ptr> _writers;    // List of wand writers
  std::vector<irs::WandWriter*> _valid_writers;  // Valid wand writers
  uint64_t _writers_mask{};
  Features _features;  // Features supported by current field
  const PostingsFormat _postings_format_version;
  const TermsFormat _terms_format_version;
};

void PostingsWriterBase::PrepareWriters(const FieldProperties& meta) {
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
      irs::SetBit(_writers_mask, i);
      _valid_writers.emplace_back(writer.get());
    }
    ++i;
  }
}

void PostingsWriterBase::WriteSkip(size_t level, MemoryIndexOutput& out) const {
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

    if (_features.HasOffsetOrPayload()) {
      SDB_ASSERT(_pay_out);

      if (_features.HasPayload()) {
        out.WriteV32(static_cast<uint32_t>(_pay.block_last));
      }

      const uint64_t pay_ptr = _pay_out->Position();

      out.WriteV64(pay_ptr - _pay.skip_ptr[level]);
      _pay.skip_ptr[level] = pay_ptr;
    }
  }
}

void PostingsWriterBase::prepare(IndexOutput& out, const FlushState& state) {
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

    if (IndexFeatures::None !=
        (state.index_features & (IndexFeatures::Pay | IndexFeatures::Offs))) {
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

void PostingsWriterBase::encode(BufferedOutput& out, const TermMeta& state) {
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
    if (_features.HasOffsetOrPayload()) {
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

void PostingsWriterBase::end() {
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

void PostingsWriterBase::BeginTerm() {
  _doc.start = _doc_out->Position();
  std::fill_n(_doc.skip_ptr, kMaxSkipLevels, _doc.start);
  if (_features.HasPosition()) {
    SDB_ASSERT(_pos_out);
    _pos.start = _pos_out->Position();
    std::fill_n(_pos.skip_ptr, kMaxSkipLevels, _pos.start);
    if (_features.HasOffsetOrPayload()) {
      SDB_ASSERT(_pay_out);
      _pay.start = _pay_out->Position();
      std::fill_n(_pay.skip_ptr, kMaxSkipLevels, _pay.start);
    }
  }

  _doc.last = doc_limits::invalid();
  _doc.block_last = doc_limits::min();
  _skip.Reset();
}

void PostingsWriterBase::EndDocument() {
  if (_doc.Full()) {
    _doc.block_last = _doc.last;
    _doc.end = _doc_out->Position();
    if (_features.HasPosition()) {
      SDB_ASSERT(_pos_out);
      _pos.end = _pos_out->Position();
      // documents stream is full, but positions stream is not
      // save number of positions to skip before the next block
      _pos.block_last = _pos.size;
      if (_features.HasOffsetOrPayload()) {
        SDB_ASSERT(_pay_out);
        _pay.end = _pay_out->Position();
        _pay.block_last = _pay.pay_buf.size();
      }
    }

    _doc.doc = _doc.docs.begin();
    _doc.freq = _doc.freqs.begin();
  }
}

void PostingsWriterBase::EndTerm(TermMetaImpl& meta) {
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
      uint32_t last_pay_size = std::numeric_limits<uint32_t>::max();
      uint32_t last_offs_len = std::numeric_limits<uint32_t>::max();
      uint32_t pay_buf_start = 0;
      for (uint32_t i = 0; i < _pos.size; ++i) {
        const uint32_t pos_delta = _pos.buf[i];
        if (_features.HasPayload()) {
          SDB_ASSERT(_pay_out);

          const uint32_t size = _pay.pay_sizes[i];
          if (last_pay_size != size) {
            last_pay_size = size;
            out.WriteV32(ShiftPack32(pos_delta, true));
            out.WriteV32(size);
          } else {
            out.WriteV32(ShiftPack32(pos_delta, false));
          }

          if (size != 0) {
            out.WriteBytes(_pay.pay_buf.c_str() + pay_buf_start, size);
            pay_buf_start += size;
          }
        } else {
          out.WriteV32(pos_delta);
        }

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

      if (_features.HasPayload()) {
        SDB_ASSERT(_pay_out);
        _pay.pay_buf.clear();
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
    _pay.pay_buf.clear();
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
                         _pay_buf.pay_sizes,
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
    // Buffer for payloads sizes
    uint32_t pay_sizes[FormatTraits::kBlockSize]{};
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
      // FIXME do aligned?
      DeltaEncode<FormatTraits::kBlockSize>(_doc.docs.data(), _doc.block_last);
      FormatTraits::write_block(*_doc_out, _doc.docs.data(), _buf);
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

  if (_attrs.pay) {
    _pay.PushPayload(_pos.size, _attrs.pay->value);
  }

  if (_attrs.offs) {
    _pay.PushOffset(_pos.size, _attrs.offs->start, _attrs.offs->end);
  }

  _pos.Next(pos);

  if (_pos.Full()) {
    FormatTraits::write_block(*_pos_out, _pos.buf.data(), _buf);
    _pos.size = 0;

    if (_features.HasPayload()) {
      SDB_ASSERT(_pay_out);
      auto& pay_buf = _pay.pay_buf;

      _pay_out->WriteV32(static_cast<uint32_t>(pay_buf.size()));
      if (!pay_buf.empty()) {
        FormatTraits::write_block(*_pay_out, _pay.pay_sizes, _buf);
        _pay_out->WriteBytes(pay_buf.c_str(), pay_buf.size());
        pay_buf.clear();
      }
    }

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
  const auto* doc = irs::get<DocAttr>(docs);

  if (!doc) [[unlikely]] {
    SDB_ASSERT(false);
    throw IllegalArgument{"'document' attribute is missing"};
  }

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

  while (docs.next()) {
    SDB_ASSERT(doc_limits::valid(doc->value));
    SDB_ASSERT(_attrs.freq);
    _attrs.doc.value = doc->value;
    _attrs.freq_value.value = _attrs.freq->value;

    if (doc_limits::valid(_doc.last) && _doc.Empty()) {
      _skip.Skip(docs_count, [this](size_t level, MemoryIndexOutput& out) {
        WriteSkip(level, out);

        // FIXME(gnusi): optimize for 1 writer case? compile? maybe just 1
        // composite wand writer?
        ApplyWriters([&](auto& writer) {
          const uint8_t size = writer.Size(level);
          SDB_ASSERT(size <= irs::WandWriter::kMaxSize);
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
  // payload size to skip before in new document block
  uint32_t pay_pos{};
};

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to, const SkipState& from) noexcept {
  if constexpr (IteratorTraits::Position() &&
                (IteratorTraits::Payload() || IteratorTraits::Offset())) {
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

    if constexpr (FieldTraits::Payload() || FieldTraits::Offset()) {
      if constexpr (FieldTraits::Payload()) {
        state.pay_pos = in.ReadV32();
      }

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
    if constexpr (IteratorTraits::Payload() || IteratorTraits::Offset()) {
      to.pay_ptr = from.pay_start;
    }
  }
}

template<typename IteratorTraits, typename FieldTraits,
         bool Offs = IteratorTraits::Offset(),
         bool Pay = IteratorTraits::Payload()>
struct PositionBase;

// Implementation of iterator over positions, payloads and offsets
template<typename IteratorTraits, typename FieldTraits>
struct PositionBase<IteratorTraits, FieldTraits, true, true>
  : public PositionBase<IteratorTraits, FieldTraits, false, false> {
  using Base = PositionBase<IteratorTraits, FieldTraits, false, false>;

  irs::Attribute* Attribute(TypeInfo::type_id type) noexcept {
    if (irs::Type<PayAttr>::id() == type) {
      return &pay;
    }

    return irs::Type<OffsAttr>::id() == type ? &offs : nullptr;
  }

  void Prepare(const DocState& state) {
    Base::Prepare(state);

    pay_in = state.pay_in->Reopen();  // reopen thread-safe stream

    if (!pay_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen payload input in");

      throw IoError{"failed to reopen payload input"};
    }

    pay_in->Seek(state.term_state->pay_start);
  }

  void Prepare(const SkipState& state) {
    Base::Prepare(state);

    pay_in->Seek(state.pay_ptr);
    pay_data_pos = state.pay_pos;
  }

  void ReadAttributes() noexcept {
    offs.start += offs_start_deltas[this->buf_pos];
    offs.end = offs.start + offs_lengts[this->buf_pos];

    pay.value =
      bytes_view(pay_data.c_str() + pay_data_pos, pay_lengths[this->buf_pos]);
    pay_data_pos += pay_lengths[this->buf_pos];
  }

  void ClearAttributes() noexcept {
    offs.clear();
    pay.value = {};
  }

  void ReadBlock() {
    Base::ReadBlock();

    // read payload
    const uint32_t size = pay_in->ReadV32();
    if (size) {
      IteratorTraits::read_block(*pay_in, this->enc_buf, pay_lengths);
      pay_data.resize(size);

      [[maybe_unused]] const auto read =
        pay_in->ReadBytes(pay_data.data(), size);
      SDB_ASSERT(read == size);
    }

    // read offsets
    IteratorTraits::read_block(*pay_in, this->enc_buf, offs_start_deltas);
    IteratorTraits::read_block(*pay_in, this->enc_buf, offs_lengts);

    pay_data_pos = 0;
  }

  void ReadTailBlock() {
    size_t pos = 0;

    for (size_t i = 0; i < this->tail_length; ++i) {
      // read payloads
      if (ShiftUnpack32(this->pos_in->ReadV32(), Base::pos_deltas[i])) {
        pay_lengths[i] = this->pos_in->ReadV32();
      } else {
        SDB_ASSERT(i);
        pay_lengths[i] = pay_lengths[i - 1];
      }

      if (pay_lengths[i]) {
        const auto size = pay_lengths[i];  // length of current payload
        pay_data.resize(pos + size);  // FIXME(gnusi): use oversize from absl

        [[maybe_unused]] const auto read =
          this->pos_in->ReadBytes(pay_data.data() + pos, size);
        SDB_ASSERT(read == size);

        pos += size;
      }

      if (ShiftUnpack32(this->pos_in->ReadV32(), offs_start_deltas[i])) {
        offs_lengts[i] = this->pos_in->ReadV32();
      } else {
        SDB_ASSERT(i);
        offs_lengts[i] = offs_lengts[i - 1];
      }
    }

    pay_data_pos = 0;
  }

  void SkipBlock() {
    Base::SkipBlock();
    Base::SkipPayload(*pay_in);
    Base::SkipOffsets(*pay_in);
  }

  void Skip(size_t count) noexcept {
    // current payload start
    const auto begin = this->pay_lengths + this->buf_pos;
    const auto end = begin + count;
    this->pay_data_pos = std::accumulate(begin, end, this->pay_data_pos);

    Base::Skip(count);
  }

  uint32_t offs_start_deltas[IteratorTraits::kBlockSize]{};  // buffer to store
                                                             // offset starts
  uint32_t offs_lengts[IteratorTraits::kBlockSize]{};        // buffer to store
                                                             // offset lengths
  uint32_t pay_lengths[IteratorTraits::kBlockSize]{};        // buffer to store
                                                             // payload lengths
  IndexInput::ptr pay_in;
  OffsAttr offs;
  PayAttr pay;
  size_t pay_data_pos{};  // current position in a payload buffer
  bstring pay_data;       // buffer to store payload data
};

// Implementation of iterator over positions and payloads
template<typename IteratorTraits, typename FieldTraits>
struct PositionBase<IteratorTraits, FieldTraits, false, true>
  : public PositionBase<IteratorTraits, FieldTraits, false, false> {
  using Base = PositionBase<IteratorTraits, FieldTraits, false, false>;

  irs::Attribute* Attribute(TypeInfo::type_id type) noexcept {
    return irs::Type<PayAttr>::id() == type ? &pay : nullptr;
  }

  void Prepare(const DocState& state) {
    Base::Prepare(state);

    pay_in = state.pay_in->Reopen();  // reopen thread-safe stream

    if (!pay_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen payload input");

      throw IoError("failed to reopen payload input");
    }

    pay_in->Seek(state.term_state->pay_start);
  }

  void Prepare(const SkipState& state) {
    Base::Prepare(state);

    pay_in->Seek(state.pay_ptr);
    pay_data_pos = state.pay_pos;
  }

  void ReadAttributes() noexcept {
    pay.value =
      bytes_view(pay_data.c_str() + pay_data_pos, pay_lengths[this->buf_pos]);
    pay_data_pos += pay_lengths[this->buf_pos];
  }

  void ClearAttributes() noexcept { pay.value = {}; }

  void ReadBlock() {
    Base::ReadBlock();

    // read payload
    const uint32_t size = pay_in->ReadV32();
    if (size) {
      IteratorTraits::read_block(*pay_in, this->enc_buf, pay_lengths);
      pay_data.resize(size);

      [[maybe_unused]] const auto read =
        pay_in->ReadBytes(pay_data.data(), size);
      SDB_ASSERT(read == size);
    }

    if constexpr (FieldTraits::Offset()) {
      Base::SkipOffsets(*pay_in);
    }

    pay_data_pos = 0;
  }

  void ReadTailBlock() {
    size_t pos = 0;

    for (size_t i = 0; i < this->tail_length; ++i) {
      // read payloads
      if (ShiftUnpack32(this->pos_in->ReadV32(), this->pos_deltas[i])) {
        pay_lengths[i] = this->pos_in->ReadV32();
      } else {
        SDB_ASSERT(i);
        pay_lengths[i] = pay_lengths[i - 1];
      }

      if (pay_lengths[i]) {
        const auto size = pay_lengths[i];  // current payload length
        pay_data.resize(pos + size);  // FIXME(gnusi): use oversize from absl

        [[maybe_unused]] const auto read =
          this->pos_in->ReadBytes(pay_data.data() + pos, size);
        SDB_ASSERT(read == size);

        pos += size;
      }

      // skip offsets
      if constexpr (FieldTraits::Offset()) {
        uint32_t code;
        if (ShiftUnpack32(this->pos_in->ReadV32(), code)) {
          this->pos_in->ReadV32();
        }
      }
    }

    pay_data_pos = 0;
  }

  void SkipBlock() {
    Base::SkipBlock();
    Base::SkipPayload(*pay_in);
    if constexpr (FieldTraits::Offset()) {
      Base::SkipOffsets(*pay_in);
    }
  }

  void Skip(size_t count) noexcept {
    // current payload start
    const auto begin = this->pay_lengths + this->buf_pos;
    const auto end = begin + count;
    this->pay_data_pos = std::accumulate(begin, end, this->pay_data_pos);

    Base::Skip(count);
  }

  uint32_t pay_lengths[IteratorTraits::kBlockSize]{};  // buffer to store
                                                       // payload lengths
  IndexInput::ptr pay_in;
  PayAttr pay;
  size_t pay_data_pos{};  // current position in a payload buffer
  bstring pay_data;       // buffer to store payload data
};

// Implementation of iterator over positions and offsets
template<typename IteratorTraits, typename FieldTraits>
struct PositionBase<IteratorTraits, FieldTraits, true, false>
  : public PositionBase<IteratorTraits, FieldTraits, false, false> {
  using Base = PositionBase<IteratorTraits, FieldTraits, false, false>;

  irs::Attribute* Attribute(TypeInfo::type_id type) noexcept {
    return irs::Type<OffsAttr>::id() == type ? &offs : nullptr;
  }

  void Prepare(const DocState& state) {
    Base::Prepare(state);

    pay_in = state.pay_in->Reopen();  // reopen thread-safe stream

    if (!pay_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen payload input");

      throw IoError("failed to reopen payload input");
    }

    pay_in->Seek(state.term_state->pay_start);
  }

  void Prepare(const SkipState& state) {
    Base::Prepare(state);

    pay_in->Seek(state.pay_ptr);
  }

  void ReadAttributes() noexcept {
    offs.start += offs_start_deltas[this->buf_pos];
    offs.end = offs.start + offs_lengts[this->buf_pos];
  }

  void ClearAttributes() noexcept { offs.clear(); }

  void ReadBlock() {
    Base::ReadBlock();

    if constexpr (FieldTraits::Payload()) {
      Base::SkipPayload(*pay_in);
    }

    // read offsets
    IteratorTraits::read_block(*pay_in, this->enc_buf, offs_start_deltas);
    IteratorTraits::read_block(*pay_in, this->enc_buf, offs_lengts);
  }

  void ReadTailBlock() {
    uint32_t pay_size = 0;
    for (size_t i = 0; i < this->tail_length; ++i) {
      // skip payloads
      if constexpr (FieldTraits::Payload()) {
        if (ShiftUnpack32(this->pos_in->ReadV32(), this->pos_deltas[i])) {
          pay_size = this->pos_in->ReadV32();
        }
        if (pay_size) {
          this->pos_in->Seek(this->pos_in->Position() + pay_size);
        }
      } else {
        this->pos_deltas[i] = this->pos_in->ReadV32();
      }

      // read offsets
      if (ShiftUnpack32(this->pos_in->ReadV32(), offs_start_deltas[i])) {
        offs_lengts[i] = this->pos_in->ReadV32();
      } else {
        SDB_ASSERT(i);
        offs_lengts[i] = offs_lengts[i - 1];
      }
    }
  }

  void SkipBlock() {
    Base::SkipBlock();
    if constexpr (FieldTraits::Payload()) {
      Base::SkipPayload(*pay_in);
    }
    Base::SkipOffsets(*pay_in);
  }

  uint32_t offs_start_deltas[IteratorTraits::kBlockSize]{};  // buffer to store
                                                             // offset starts
  uint32_t offs_lengts[IteratorTraits::kBlockSize]{};        // buffer to store
                                                             // offset lengths
  IndexInput::ptr pay_in;
  OffsAttr offs;
};

// Implementation of iterator over positions
template<typename IteratorTraits, typename FieldTraits>
struct PositionBase<IteratorTraits, FieldTraits, false, false> {
  static void SkipPayload(IndexInput& in) {
    const size_t size = in.ReadV32();
    if (size) {
      IteratorTraits::skip_block(in);
      in.Seek(in.Position() + size);
    }
  }

  static void SkipOffsets(IndexInput& in) {
    IteratorTraits::skip_block(in);
    IteratorTraits::skip_block(in);
  }

  irs::Attribute* Attribute(TypeInfo::type_id) noexcept {
    // implementation has no additional attributes
    return nullptr;
  }

  void Prepare(const DocState& state) {
    pos_in = state.pos_in->Reopen();  // reopen thread-safe stream

    if (!pos_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen positions input");

      throw IoError("failed to reopen positions input");
    }

    cookie.file_pointer = state.term_state->pos_start;
    pos_in->Seek(state.term_state->pos_start);
    freq = state.freq;
    enc_buf = state.enc_buf;
    tail_start = state.tail_start;
    tail_length = state.tail_length;
  }

  void Prepare(const SkipState& state) {
    pos_in->Seek(state.pos_ptr);
    pend_pos = state.pend_pos;
    buf_pos = IteratorTraits::kBlockSize;
    cookie.file_pointer = state.pos_ptr;
    cookie.pend_pos = pend_pos;
  }

  void Reset() {
    if (std::numeric_limits<size_t>::max() != cookie.file_pointer) {
      buf_pos = IteratorTraits::kBlockSize;
      pend_pos = cookie.pend_pos;
      pos_in->Seek(cookie.file_pointer);
    }
  }

  void ReadAttributes() {}

  void ClearAttributes() {}

  void ReadTailBlock() {
    uint32_t pay_size = 0;
    for (size_t i = 0; i < tail_length; ++i) {
      if constexpr (FieldTraits::Payload()) {
        if (ShiftUnpack32(pos_in->ReadV32(), pos_deltas[i])) {
          pay_size = pos_in->ReadV32();
        }
        if (pay_size) {
          pos_in->Seek(pos_in->Position() + pay_size);
        }
      } else {
        pos_deltas[i] = pos_in->ReadV32();
      }

      if constexpr (FieldTraits::Offset()) {
        uint32_t delta;
        if (ShiftUnpack32(pos_in->ReadV32(), delta)) {
          pos_in->ReadV32();
        }
      }
    }
  }

  void ReadBlock() { IteratorTraits::read_block(*pos_in, enc_buf, pos_deltas); }

  void SkipBlock() { IteratorTraits::skip_block(*pos_in); }

  // skip within a block
  void Skip(size_t count) noexcept { buf_pos += count; }

  struct Cookie {
    size_t pend_pos{};
    size_t file_pointer = std::numeric_limits<size_t>::max();
  };

  uint32_t pos_deltas[IteratorTraits::kBlockSize];  // buffer to store
                                                    // position deltas
  const uint32_t* freq;  // lenght of the posting list for a document
  uint32_t* enc_buf;     // auxillary buffer to decode data
  size_t pend_pos{};     // how many positions "behind" we are
  uint64_t tail_start;   // file pointer where the last (vInt encoded) pos delta
                         // block is
  size_t tail_length;    // number of positions in the last (vInt encoded) pos
                         // delta block
  uint32_t buf_pos{
    IteratorTraits::kBlockSize};  // current position in pos_deltas_ buffer
  Cookie cookie;
  IndexInput::ptr pos_in;
};

template<typename IteratorTraits, typename FieldTraits,
         bool Pos = IteratorTraits::Position()>
class PositionImpl final : public irs::PosAttr,
                           private PositionBase<IteratorTraits, FieldTraits> {
 public:
  using Impl = PositionBase<IteratorTraits, FieldTraits>;

  irs::Attribute* GetMutable(TypeInfo::type_id type) final {
    return Impl::Attribute(type);
  }

  value_t seek(value_t target) final {
    const uint32_t freq = *this->freq;
    if (this->pend_pos > freq) {
      Skip(static_cast<uint32_t>(this->pend_pos - freq));
      this->pend_pos = freq;
    }
    while (_value < target && this->pend_pos) {
      if (this->buf_pos == IteratorTraits::kBlockSize) {
        Refill();
        this->buf_pos = 0;
      }
      _value += this->pos_deltas[this->buf_pos];
      SDB_ASSERT(irs::pos_limits::valid(_value));
      this->ReadAttributes();

      ++this->buf_pos;
      --this->pend_pos;
    }
    if (0 == this->pend_pos && _value < target) {
      _value = pos_limits::eof();
    }
    return _value;
  }

  bool next() final {
    if (0 == this->pend_pos) {
      _value = pos_limits::eof();

      return false;
    }

    const uint32_t freq = *this->freq;

    if (this->pend_pos > freq) {
      Skip(static_cast<uint32_t>(this->pend_pos - freq));
      this->pend_pos = freq;
    }

    if (this->buf_pos == IteratorTraits::kBlockSize) {
      Refill();
      this->buf_pos = 0;
    }
    _value += this->pos_deltas[this->buf_pos];
    SDB_ASSERT(irs::pos_limits::valid(_value));
    this->ReadAttributes();

    ++this->buf_pos;
    --this->pend_pos;
    return true;
  }

  void reset() final {
    _value = pos_limits::invalid();
    Impl::Reset();
  }

  // prepares iterator to work
  // or notifies iterator that doc iterator has skipped to a new block
  using Impl::Prepare;

  // notify iterator that corresponding DocIterator has moved forward
  void Notify(uint32_t n) {
    this->pend_pos += n;
    this->cookie.pend_pos += n;
  }

  void Clear() noexcept {
    _value = pos_limits::invalid();
    Impl::ClearAttributes();
  }

 private:
  void Refill() {
    if (this->pos_in->Position() == this->tail_start) {
      this->ReadTailBlock();
    } else {
      this->ReadBlock();
    }
  }

  void Skip(uint32_t count) {
    auto left = IteratorTraits::kBlockSize - this->buf_pos;
    if (count >= left) {
      count -= left;
      while (count >= IteratorTraits::kBlockSize) {
        this->SkipBlock();
        count -= IteratorTraits::kBlockSize;
      }
      Refill();
      this->buf_pos = 0;
      left = IteratorTraits::kBlockSize;
    }

    if (count < left) {
      Impl::Skip(count);
    }
    Clear();
  }
};

// Empty iterator over positions
template<typename IteratorTraits, typename FieldTraits>
struct PositionImpl<IteratorTraits, FieldTraits, false> : Attribute {
  static constexpr std::string_view type_name() noexcept {
    return irs::PosAttr::type_name();
  }

  void Prepare(const DocState&) noexcept {}
  void Prepare(const SkipState&) noexcept {}
  void Notify(uint32_t) noexcept {}
  void Clear() noexcept {}
};

struct Empty {};

// Buffer type containing only document buffer
template<typename IteratorTraits>
struct DataBuffer {
  doc_id_t docs[IteratorTraits::kBlockSize]{};
};

// Buffer type containing both document and fequency buffers
template<typename IteratorTraits>
struct FreqBuffer : DataBuffer<IteratorTraits> {
  uint32_t freqs[IteratorTraits::kBlockSize];
};

template<typename IteratorTraits>
using BufferType =
  std::conditional_t<IteratorTraits::Frequency(), FreqBuffer<IteratorTraits>,
                     DataBuffer<IteratorTraits>>;

template<typename IteratorTraits, typename FieldTraits>
class DocIteratorBase : public DocIterator {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());
  void ReadTailBlock();

 protected:
  // returns current position in the document block 'docs_'
  doc_id_t RelativePos() noexcept {
    SDB_ASSERT(_begin >= _buf.docs);
    return static_cast<doc_id_t>(_begin - _buf.docs);
  }

  void Refill();

  BufferType<IteratorTraits> _buf;
  uint32_t _enc_buf[IteratorTraits::kBlockSize];  // buffer for encoding
  const doc_id_t* _begin{std::end(_buf.docs)};
  uint32_t* _freq{};  // pointer into docs_ to the frequency attribute value for
                      // the current doc
  IndexInput::ptr _doc_in;
  doc_id_t _left{};
};

template<typename IteratorTraits, typename FieldTraits>
void DocIteratorBase<IteratorTraits, FieldTraits>::Refill() {
  if (_left >= IteratorTraits::kBlockSize) [[likely]] {
    // read doc deltas
    IteratorTraits::read_block(*_doc_in, _enc_buf, _buf.docs);

    if constexpr (IteratorTraits::Frequency()) {
      IteratorTraits::read_block(*_doc_in, _enc_buf, _buf.freqs);
    } else if constexpr (FieldTraits::Frequency()) {
      IteratorTraits::skip_block(*_doc_in);
    }

    static_assert(std::size(decltype(_buf.docs){}) ==
                  IteratorTraits::kBlockSize);
    _begin = std::begin(_buf.docs);
    if constexpr (IteratorTraits::Frequency()) {
      _freq = _buf.freqs;
    }
    _left -= IteratorTraits::kBlockSize;
  } else {
    ReadTailBlock();
  }
}

template<typename IteratorTraits, typename FieldTraits>
void DocIteratorBase<IteratorTraits, FieldTraits>::ReadTailBlock() {
  auto* doc = std::end(_buf.docs) - _left;
  _begin = doc;

  [[maybe_unused]] uint32_t* freq;
  if constexpr (IteratorTraits::Frequency()) {
    _freq = freq = std::end(_buf.freqs) - _left;
  }

  while (doc < std::end(_buf.docs)) {
    if constexpr (FieldTraits::Frequency()) {
      if constexpr (IteratorTraits::Frequency()) {
        if (ShiftUnpack32(_doc_in->ReadV32(), *doc++)) {
          *freq++ = 1;
        } else {
          *freq++ = _doc_in->ReadV32();
        }
      } else {
        if (!ShiftUnpack32(_doc_in->ReadV32(), *doc++)) {
          _doc_in->ReadV32();
        }
      }
    } else {
      *doc++ = _doc_in->ReadV32();
    }
  }
  _left = 0;
}

template<typename IteratorTraits, typename FieldTraits>
using AttributesT = std::conditional_t<
  IteratorTraits::Position(),
  std::tuple<DocAttr, FreqAttr, ScoreAttr, CostAttr,
             PositionImpl<IteratorTraits, FieldTraits>>,
  std::conditional_t<IteratorTraits::Frequency(),
                     std::tuple<DocAttr, FreqAttr, ScoreAttr, CostAttr>,
                     std::tuple<DocAttr, ScoreAttr, CostAttr>>>;

template<typename IteratorTraits, typename FieldTraits>
class SingleDocIterator
  : public DocIterator,
    private std::conditional_t<IteratorTraits::Position(),
                               DataBuffer<IteratorTraits>, Empty> {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());

  using Attributes = AttributesT<IteratorTraits, FieldTraits>;
  using Position = PositionImpl<IteratorTraits, FieldTraits>;

 public:
  SingleDocIterator() = default;

  void WandPrepare(const TermMeta& meta, const IndexInput* pos_in,
                   const IndexInput* pay_in,
                   const ScoreFunctionFactory& factory) {
    Prepare(meta, pos_in, pay_in);
    auto func = factory(*this);

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    doc_value = _next;

    auto& score = std::get<ScoreAttr>(_attrs);
    func(&score.max.leaf);
    score.max.tail = score.max.leaf;
    score = ScoreFunction::Constant(score.max.tail);

    doc_value = doc_limits::invalid();
  }

  void Prepare(const TermMeta& meta, [[maybe_unused]] const IndexInput* pos_in,
               [[maybe_unused]] const IndexInput* pay_in);

 private:
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    doc_value = _next;
    _next = doc_limits::eof();

    if constexpr (IteratorTraits::Position()) {
      if (!doc_limits::eof(doc_value)) {
        auto& pos = std::get<Position>(_attrs);
        pos.Notify(std::get<FreqAttr>(_attrs).value);
        pos.Clear();
      }
    }

    return !doc_limits::eof(doc_value);
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    while (doc_value < target) {
      next();
    }

    return doc_value;
  }

  doc_id_t _next{doc_limits::eof()};
  Attributes _attrs;
};

template<typename IteratorTraits, typename FieldTraits>
void SingleDocIterator<IteratorTraits, FieldTraits>::Prepare(
  const TermMeta& meta, [[maybe_unused]] const IndexInput* pos_in,
  [[maybe_unused]] const IndexInput* pay_in) {
  // use single_doc_iterator for singleton docs only,
  // must be ensured by the caller
  SDB_ASSERT(meta.docs_count == 1);

  const auto& term_state = static_cast<const TermMetaImpl&>(meta);
  _next = doc_limits::min() + term_state.e_single_doc;
  std::get<CostAttr>(_attrs).reset(1);  // estimate iterator

  if constexpr (IteratorTraits::Frequency()) {
    const auto term_freq = meta.freq;

    SDB_ASSERT(term_freq);
    std::get<FreqAttr>(_attrs).value = term_freq;

    if constexpr (IteratorTraits::Position()) {
      auto get_tail_start = [&]() noexcept {
        if (term_freq < IteratorTraits::kBlockSize) {
          return term_state.pos_start;
        } else if (term_freq == IteratorTraits::kBlockSize) {
          return address_limits::invalid();
        } else {
          return term_state.pos_start + term_state.pos_end;
        }
      };

      const DocState state{
        .pos_in = pos_in,
        .pay_in = pay_in,
        .term_state = &term_state,
        .freq = &std::get<FreqAttr>(_attrs).value,
        .enc_buf = this->docs,
        .tail_start = get_tail_start(),
        .tail_length = term_freq % IteratorTraits::kBlockSize};

      std::get<Position>(_attrs).Prepare(state);
    }
  }
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
    func(&score);
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
  func(&score);
  if (block_offset) {
    in.Skip(block_offset);
  }
}

// Iterator over posting list.
// IteratorTraits defines requested features.
// FieldTraits defines requested features.
template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
class DocIteratorImpl : public DocIteratorBase<IteratorTraits, FieldTraits> {
  using Attributes = AttributesT<IteratorTraits, FieldTraits>;
  using Position = PositionImpl<IteratorTraits, FieldTraits>;

 public:
  DocIteratorImpl(WandExtent extent)
    : _skip{IteratorTraits::kBlockSize, PostingsWriterBase::kSkipN,
            ReadSkip{extent}} {
    SDB_ASSERT(absl::c_all_of(
      this->_buf.docs, [](doc_id_t doc) { return !doc_limits::valid(doc); }));
  }

  void WandPrepare(const TermMeta& meta, const IndexInput* doc_in,
                   const IndexInput* pos_in, const IndexInput* pay_in,
                   const ScoreFunctionFactory& factory, const Scorer& scorer,
                   uint8_t wand_index) {
    Prepare(meta, doc_in, pos_in, pay_in, wand_index);
    if (meta.docs_count > FieldTraits::kBlockSize) {
      return;
    }
    auto ctx = scorer.prepare_wand_source();
    auto func = factory(*ctx);

    auto old_offset = std::numeric_limits<size_t>::max();
    if (meta.docs_count == FieldTraits::kBlockSize) {
      old_offset = this->_doc_in->Position();
      FieldTraits::skip_block(*this->_doc_in);
      if constexpr (FieldTraits::Frequency()) {
        FieldTraits::skip_block(*this->_doc_in);
      }
    }

    auto& score = std::get<irs::ScoreAttr>(_attrs);
    _skip.Reader().ReadMaxScore(wand_index, func, *ctx, *this->_doc_in,
                                score.max.tail);
    score.max.leaf = score.max.tail;

    if (old_offset != std::numeric_limits<size_t>::max()) {
      this->_doc_in->Seek(old_offset);
    }
  }

  void Prepare(const TermMeta& meta, const IndexInput* doc_in,
               [[maybe_unused]] const IndexInput* pos_in,
               [[maybe_unused]] const IndexInput* pay_in,
               uint8_t wand_index = WandContext::kDisable);

 private:
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t seek(doc_id_t target) final;

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (this->_begin == std::end(this->_buf.docs)) {
      if (!this->_left) [[unlikely]] {
        doc_value = doc_limits::eof();
        return false;
      }

      this->Refill();

      // If this is the initial doc_id then
      // set it to min() for proper delta value
      doc_value += doc_id_t{!doc_limits::valid(doc_value)};
    }

    doc_value += *this->_begin++;  // update document attribute

    if constexpr (IteratorTraits::Frequency()) {
      auto& freq = std::get<FreqAttr>(_attrs);
      freq.value = *this->_freq++;  // update frequency attribute

      if constexpr (IteratorTraits::Position()) {
        auto& pos = std::get<Position>(_attrs);
        pos.Notify(freq.value);
        pos.Clear();
      }
    }

    return true;
  }

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

  void SeekToBlock(doc_id_t target);

  uint64_t _skip_offs{};
  SkipReader<ReadSkip> _skip;
  Attributes _attrs;
  uint32_t _docs_count{};
};

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void DocIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::ReadSkip::Read(
  size_t level, IndexInput& in) {
  auto& next = _skip_levels[level];

  // Store previous step on the same level
  CopyState<IteratorTraits>(*_prev, next);

  ReadState<FieldTraits>(next, in);

  SkipWandData(in);
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void DocIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::ReadSkip::Seal(
  size_t level) {
  auto& next = _skip_levels[level];

  // Store previous step on the same level
  CopyState<IteratorTraits>(*_prev, next);

  // Stream exhausted
  next.doc = doc_limits::eof();
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void DocIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::Prepare(
  const TermMeta& meta, const IndexInput* doc_in,
  [[maybe_unused]] const IndexInput* pos_in,
  [[maybe_unused]] const IndexInput* pay_in, uint8_t wand_index) {
  // Don't use DocIterator for singleton docs, must be ensured by the caller
  SDB_ASSERT(meta.docs_count > 1);
  SDB_ASSERT(this->_begin == std::end(this->_buf.docs));

  auto& term_state = static_cast<const TermMetaImpl&>(meta);
  this->_left = term_state.docs_count;

  // Init document stream
  if (!this->_doc_in) {
    this->_doc_in = doc_in->Reopen();  // Reopen thread-safe stream

    if (!this->_doc_in) {
      // Implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen document input");

      throw IoError("failed to reopen document input");
    }
  }

  this->_doc_in->Seek(term_state.doc_start);
  SDB_ASSERT(!this->_doc_in->IsEOF());

  std::get<CostAttr>(_attrs).reset(term_state.docs_count);  // Estimate iterator

  SDB_ASSERT(!IteratorTraits::Frequency() || meta.freq);
  if constexpr (IteratorTraits::Frequency() && IteratorTraits::Position()) {
    const auto term_freq = meta.freq;

    auto get_tail_start = [&]() noexcept {
      if (term_freq < IteratorTraits::kBlockSize) {
        return term_state.pos_start;
      } else if (term_freq == IteratorTraits::kBlockSize) {
        return address_limits::invalid();
      } else {
        return term_state.pos_start + term_state.pos_end;
      }
    };

    const DocState state{.pos_in = pos_in,
                         .pay_in = pay_in,
                         .term_state = &term_state,
                         .freq = &std::get<FreqAttr>(_attrs).value,
                         .enc_buf = this->_enc_buf,
                         .tail_start = get_tail_start(),
                         .tail_length = term_freq % IteratorTraits::kBlockSize};

    std::get<Position>(_attrs).Prepare(state);
  }

  if (term_state.docs_count > IteratorTraits::kBlockSize) {
    // Allow using skip-list for long enough postings
    _skip.Reader().Enable(term_state);
    _skip_offs = term_state.doc_start + term_state.e_skip_start;
    _docs_count = term_state.docs_count;
  } else if (term_state.docs_count != IteratorTraits::kBlockSize &&
             wand_index == WandContext::kDisable) {
    _skip.Reader().SkipWandData(*this->_doc_in);
  }
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
doc_id_t DocIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::seek(
  doc_id_t target) {
  auto& doc_value = std::get<DocAttr>(_attrs).value;

  if (target <= doc_value) [[unlikely]] {
    return doc_value;
  }

  // Check whether it makes sense to use skip-list
  if (_skip.Reader().UpperBound() < target) {
    SeekToBlock(target);

    if (!this->_left) [[unlikely]] {
      return doc_value = doc_limits::eof();
    }

    this->Refill();

    // If this is the initial doc_id then
    // set it to min() for proper delta value
    doc_value += doc_id_t{!doc_limits::valid(doc_value)};
  }

  [[maybe_unused]] uint32_t notify{0};
  while (this->_begin != std::end(this->_buf.docs)) {
    doc_value += *this->_begin++;

    if constexpr (!IteratorTraits::Position()) {
      if (doc_value >= target) {
        if constexpr (IteratorTraits::Frequency()) {
          this->_freq = this->_buf.freqs + this->RelativePos();
          SDB_ASSERT((this->_freq - 1) >= this->_buf.freqs);
          SDB_ASSERT((this->_freq - 1) < std::end(this->_buf.freqs));
          std::get<FreqAttr>(_attrs).value = this->_freq[-1];
        }
        return doc_value;
      }
    } else {
      SDB_ASSERT(IteratorTraits::Frequency());
      auto& freq = std::get<FreqAttr>(_attrs);
      auto& pos = std::get<Position>(_attrs);
      freq.value = *this->_freq++;
      notify += freq.value;

      if (doc_value >= target) {
        pos.Notify(notify);
        pos.Clear();
        return doc_value;
      }
    }
  }

  if constexpr (IteratorTraits::Position()) {
    std::get<Position>(_attrs).Notify(notify);
  }
  while (doc_value < target) {
    next();
  }

  return doc_value;
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent>
void DocIteratorImpl<IteratorTraits, FieldTraits, WandExtent>::SeekToBlock(
  doc_id_t target) {
  // Ensured by caller
  SDB_ASSERT(_skip.Reader().UpperBound() < target);

  SkipState last;  // Where block starts
  _skip.Reader().Reset(last);

  // Init skip writer in lazy fashion
  if (!_docs_count) [[likely]] {
  seek_after_initialization:
    SDB_ASSERT(_skip.NumLevels());

    this->_left = _skip.Seek(target);
    this->_doc_in->Seek(last.doc_ptr);
    std::get<DocAttr>(_attrs).value = last.doc;
    if constexpr (IteratorTraits::Position()) {
      auto& pos = std::get<Position>(_attrs);
      pos.Prepare(last);  // Notify positions
    }

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

// WAND iterator over posting list.
// IteratorTraits defines requested features.
// FieldTraits defines requested features.
template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
class Wanderator : public DocIteratorBase<IteratorTraits, FieldTraits>,
                   private ScoreCtx {
  static_assert(IteratorTraits::Frequency());

  using Attributes = AttributesT<IteratorTraits, FieldTraits>;
  using Position = PositionImpl<IteratorTraits, FieldTraits>;

  static void MinStrict(ScoreCtx* ctx, score_t arg) noexcept {
    auto& self = static_cast<Wanderator&>(*ctx);
    self._skip.Reader().threshold = arg;
  }

  static void MinWeak(ScoreCtx* ctx, score_t arg) noexcept {
    MinStrict(ctx, std::nextafter(arg, 0.f));
  }

 public:
  Wanderator(const ScoreFunctionFactory& factory, const Scorer& scorer,
             WandExtent extent, uint8_t index, bool strict)
    : _skip{IteratorTraits::kBlockSize, PostingsWriterBase::kSkipN,
            ReadSkip{factory, scorer, index, extent}},
      _scorer{factory(*this)} {
    SDB_ASSERT(absl::c_all_of(this->_buf.docs, [](doc_id_t doc) {
      return doc == doc_limits::invalid();
    }));
    std::get<irs::ScoreAttr>(_attrs).Reset(
      *this,
      [](ScoreCtx* ctx, score_t* res) noexcept {
        auto& self = static_cast<Wanderator&>(*ctx);
        if constexpr (Root) {
          *res = self._score;
        } else {
          self._scorer(res);
        }
      },
      strict ? MinStrict : MinWeak);
  }

  void WandPrepare(const TermMeta& meta, const IndexInput* doc_in,
                   [[maybe_unused]] const IndexInput* pos_in,
                   [[maybe_unused]] const IndexInput* pay_in);

 private:
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t seek(doc_id_t target) final;

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() final { return !doc_limits::eof(seek(value() + 1)); }

  struct SkipScoreContext final : AttributeProvider {
    FreqAttr freq;

    Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
      if (id == irs::Type<FreqAttr>::id()) {
        return &freq;
      }
      return nullptr;
    }
  };

  class ReadSkip {
   public:
    ReadSkip(const ScoreFunctionFactory& factory, const Scorer& scorer,
             uint8_t index, WandExtent extent)
      : ctx{scorer.prepare_wand_source()},
        func{factory(*ctx)},
        index{index},
        extent{extent} {
      SDB_ASSERT(extent.GetExtent() > 0);
    }

    void EnsureSorted() const noexcept {
      SDB_ASSERT(absl::c_is_sorted(
        skip_levels,
        [](const auto& lhs, const auto& rhs) { return lhs.doc > rhs.doc; }));
      SDB_ASSERT(absl::c_is_sorted(skip_scores, std::greater<>{}));
    }

    void ReadMaxScore(irs::ScoreAttr::UpperBounds& max, IndexInput& input) {
      CommonReadWandData(extent, index, func, *ctx, input, max.tail);
      max.leaf = max.tail;
    }
    void Init(const TermMetaImpl& state, size_t num_levels,
              irs::ScoreAttr::UpperBounds& max);
    bool IsLess(size_t level, doc_id_t target) const noexcept {
      return skip_levels[level].doc < target || skip_scores[level] <= threshold;
    }
    bool IsLessThanUpperBound(doc_id_t target) const noexcept {
      return skip_levels.back().doc < target || skip_scores.back() <= threshold;
    }
    void MoveDown(size_t level) noexcept {
      auto& next = skip_levels[level];

      // Move to the more granular level
      CopyState<IteratorTraits>(next, prev_skip);
    }
    void Read(size_t level, IndexInput& in);
    void Seal(size_t level);
    size_t AdjustLevel(size_t level) const noexcept;
    SkipState& State() noexcept { return prev_skip; }
    SkipState& Next() noexcept { return skip_levels.back(); }

    WandSource::ptr ctx;
    ScoreFunction func;
    std::vector<SkipState> skip_levels;
    std::vector<score_t> skip_scores;
    SkipState prev_skip;  // skip context used by skip reader
    score_t threshold{};
    uint8_t index;
    [[no_unique_address]] WandExtent extent;
  };

  doc_id_t shallow_seek(doc_id_t target) final {
    SeekToBlock(target);
    return _skip.Reader().Next().doc;
  }

  void SeekToBlock(doc_id_t target) {
    _skip.Reader().EnsureSorted();

    // Check whether we have to use skip-list
    if (_skip.Reader().IsLessThanUpperBound(target)) {
      // Ensured by Prepare(...)
      SDB_ASSERT(_skip.NumLevels());
      // We could decide to make new skip before actual read block
      // SDB_ASSERT(0 == skip_.Reader().State().doc_ptr);

      this->_left = _skip.Seek(target);
      auto& doc_value = std::get<DocAttr>(_attrs).value;
      doc_value = _skip.Reader().State().doc;
      std::get<ScoreAttr>(_attrs).max.leaf = _skip.Reader().skip_scores.back();
      // Will trigger Refill in "next"
      this->_begin = std::end(this->_buf.docs);
    }
  }

  SkipReader<ReadSkip> _skip;
  Attributes _attrs;
  ScoreFunction _scorer;  // FIXME(gnusi): can we use only one ScoreFunction?
  score_t _score{};
};

template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
void Wanderator<IteratorTraits, FieldTraits, WandExtent, Root>::ReadSkip::Init(
  const TermMetaImpl& term_state, size_t num_levels,
  irs::ScoreAttr::UpperBounds& max) {
  // Don't use wanderator for short posting lists, must be ensured by the caller
  SDB_ASSERT(term_state.docs_count > IteratorTraits::kBlockSize);

  skip_levels.resize(num_levels);
  skip_scores.resize(num_levels);
  max.leaf = skip_scores.back();
#ifdef SDB_GTEST
  max.levels = std::span{skip_scores};
#endif

  // Since we store pointer deltas, add postings offset
  auto& top = skip_levels.front();
  CopyState<IteratorTraits>(top, term_state);
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
void Wanderator<IteratorTraits, FieldTraits, WandExtent, Root>::ReadSkip::Read(
  size_t level, IndexInput& in) {
  auto& last = prev_skip;
  auto& next = skip_levels[level];
  auto& score = skip_scores[level];

  // Store previous step on the same level
  CopyState<IteratorTraits>(last, next);

  ReadState<FieldTraits>(next, in);
  // TODO(mbkkt) We could don't read actual wand data for not just term query
  //  It looks almost no difference now, but if we will have bigger wand data
  //  it could be useful
  // if constexpr (Root) {
  CommonReadWandData(extent, index, func, *ctx, in, score);
  // } else {
  //   auto& back = skip_scores_.back();
  //   if (&score == &back || threshold_ > back) {
  //     CommonReadWandData(extent_, index_, func_, *ctx_, in, score);
  //   } else {
  //     CommonSkipWandData(extent_, in);
  //     score = std::numeric_limits<score_t>::max();
  //   }
  // }
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
size_t Wanderator<IteratorTraits, FieldTraits, WandExtent,
                  Root>::ReadSkip::AdjustLevel(size_t level) const noexcept {
  while (level && skip_levels[level].doc >= skip_levels[level - 1].doc) {
    SDB_ASSERT(skip_levels[level - 1].doc != doc_limits::eof());
    --level;
  }
  return level;
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
void Wanderator<IteratorTraits, FieldTraits, WandExtent, Root>::ReadSkip::Seal(
  size_t level) {
  auto& last = prev_skip;
  auto& next = skip_levels[level];

  // Store previous step on the same level
  CopyState<IteratorTraits>(last, next);

  // Stream exhausted
  next.doc = doc_limits::eof();
  skip_scores[level] = std::numeric_limits<score_t>::max();
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
void Wanderator<IteratorTraits, FieldTraits, WandExtent, Root>::WandPrepare(
  const TermMeta& meta, const IndexInput* doc_in,
  [[maybe_unused]] const IndexInput* pos_in,
  [[maybe_unused]] const IndexInput* pay_in) {
  // Don't use wanderator for short posting lists, must be ensured by the caller
  SDB_ASSERT(meta.docs_count > IteratorTraits::kBlockSize);
  SDB_ASSERT(this->_begin == std::end(this->_buf.docs));

  auto& term_state = static_cast<const TermMetaImpl&>(meta);
  this->_left = term_state.docs_count;

  // Init document stream
  if (!this->_doc_in) {
    this->_doc_in = doc_in->Reopen();  // reopen thread-safe stream

    if (!this->_doc_in) {
      // Implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Failed to reopen document input");

      throw IoError("failed to reopen document input");
    }
  }

  this->_doc_in->Seek(term_state.doc_start);
  SDB_ASSERT(!this->_doc_in->IsEOF());

  std::get<CostAttr>(_attrs).reset(term_state.docs_count);  // Estimate iterator

  SDB_ASSERT(!IteratorTraits::Frequency() || meta.freq);

  if constexpr (IteratorTraits::Frequency() && IteratorTraits::Position()) {
    const auto term_freq = meta.freq;

    auto get_tail_start = [&]() noexcept {
      if (term_freq < IteratorTraits::kBlockSize) {
        return term_state.pos_start;
      } else if (term_freq == IteratorTraits::kBlockSize) {
        return address_limits::invalid();
      } else {
        return term_state.pos_start + term_state.pos_end;
      }
    };

    const DocState state{.pos_in = pos_in,
                         .pay_in = pay_in,
                         .term_state = &term_state,
                         .freq = &std::get<FreqAttr>(_attrs).value,
                         .enc_buf = this->_enc_buf,
                         .tail_start = get_tail_start(),
                         .tail_length = term_freq % IteratorTraits::kBlockSize};

    std::get<Position>(_attrs).Prepare(state);
  }

  auto skip_in = this->_doc_in->Dup();

  if (!skip_in) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Failed to duplicate document input");

    throw IoError("Failed to duplicate document input");
  }

  skip_in->Seek(term_state.doc_start + term_state.e_skip_start);

  auto& max = std::get<irs::ScoreAttr>(_attrs).max;
  _skip.Reader().ReadMaxScore(max, *skip_in);

  _skip.Prepare(std::move(skip_in), term_state.docs_count);

  // Initialize skip levels
  if (const auto num_levels = _skip.NumLevels();
      num_levels > 0 && num_levels <= PostingsWriterBase::kMaxSkipLevels)
    [[likely]] {
    _skip.Reader().Init(term_state, num_levels, max);
  } else {
    SDB_ASSERT(false);
    throw IndexError{absl::StrCat("Invalid number of skip levels ", num_levels,
                                  ", must be in range of [1, ",
                                  PostingsWriterBase::kMaxSkipLevels, "].")};
  }
}

template<typename IteratorTraits, typename FieldTraits, typename WandExtent,
         bool Root>
doc_id_t Wanderator<IteratorTraits, FieldTraits, WandExtent, Root>::seek(
  doc_id_t target) {
  auto& doc_value = std::get<DocAttr>(_attrs).value;

  if (target <= doc_value) [[unlikely]] {
    return doc_value;
  }

  while (true) {
    SeekToBlock(target);

    if (this->_begin == std::end(this->_buf.docs)) {
      if (!this->_left) [[unlikely]] {
        return doc_value = doc_limits::eof();
      }

      if (auto& state = _skip.Reader().State(); state.doc_ptr) {
        this->_doc_in->Seek(state.doc_ptr);
        if constexpr (IteratorTraits::Position()) {
          auto& pos = std::get<Position>(_attrs);
          pos.Prepare(state);  // Notify positions
        }
        state.doc_ptr = 0;
      }

      doc_value += !doc_limits::valid(doc_value);
      this->Refill();
    }

    [[maybe_unused]] uint32_t notify{0};
    [[maybe_unused]] const auto threshold = _skip.Reader().threshold;

    while (this->_begin != std::end(this->_buf.docs)) {
      doc_value += *this->_begin++;

      if constexpr (!IteratorTraits::Position()) {
        if (doc_value >= target) {
          if constexpr (IteratorTraits::Frequency()) {
            this->_freq = this->_buf.freqs + this->RelativePos();
            SDB_ASSERT((this->_freq - 1) >= this->_buf.freqs);
            SDB_ASSERT((this->_freq - 1) < std::end(this->_buf.freqs));

            auto& freq = std::get<FreqAttr>(_attrs);
            freq.value = this->_freq[-1];
            if constexpr (Root) {
              // We can use approximation before actual score for bm11, bm15,
              // tfidf(true/false) but only for term query, so I don't think
              // it's really good idea. And I don't except big difference
              // because in such case, compution is pretty cheap
              _scorer(&_score);
              if (_score <= threshold) {
                continue;
              }
            }
          }
          return doc_value;
        }
      } else {
        static_assert(IteratorTraits::Frequency());
        auto& freq = std::get<FreqAttr>(_attrs);
        freq.value = *this->_freq++;
        notify += freq.value;
        if (doc_value >= target) {
          if constexpr (Root) {
            _scorer(&_score);
            if (_score <= threshold) {
              continue;
            }
          }
          auto& pos = std::get<Position>(_attrs);
          pos.Notify(notify);
          pos.Clear();
          return doc_value;
        }
      }
    }

    if constexpr (IteratorTraits::Position()) {
      std::get<Position>(_attrs).Notify(notify);
    }

    target = doc_value + 1;
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

bool IndexMetaWriterImpl::prepare(Directory& dir, IndexMeta& meta,
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
        irs::WriteStr(*out, payload);
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

bool IndexMetaWriterImpl::commit() {
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

void IndexMetaWriterImpl::rollback() noexcept {
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

uint64_t ParseGeneration(std::string_view file) noexcept {
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

bool IndexMetaReaderImpl::last_segments_file(const Directory& dir,
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

void IndexMetaReaderImpl::read(const Directory& dir, IndexMeta& meta,
                               std::string_view filename) {
  std::string meta_file;
  if (IsNull(filename)) {
    meta_file = IndexMetaWriterImpl::FileName(meta.gen);
    filename = meta_file;
  }

  auto in =
    dir.open(filename, irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE);

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
      payload = irs::ReadString<bstring>(*in);
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

uint64_t WriteDocumentMask(IndexOutput& out, const auto& docs_mask) {
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

std::pair<const std::shared_ptr<DocumentMask>, uint64_t> ReadDocumentMask(
  IndexInput& in, IResourceManager& rm) {
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
std::string FileName<SegmentMetaWriter, SegmentMeta>(const SegmentMeta& meta) {
  return irs::FileName(meta.name, meta.version,
                       SegmentMetaWriterImpl::kFormatExt);
}

void SegmentMetaWriterImpl::write(Directory& dir, std::string& meta_file,
                                  SegmentMeta& meta) {
  if (meta.docs_count < meta.live_docs_count ||
      meta.docs_count - meta.live_docs_count != RemovalCount(meta))
    [[unlikely]] {
    throw IndexError{absl::StrCat("Invalid segment meta '", meta.name,
                                  "' detected : docs_count=", meta.docs_count,
                                  ", live_docs_count=", meta.live_docs_count)};
  }

  meta_file = FileName<irs::SegmentMetaWriter>(meta);
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

void SegmentMetaReaderImpl::read(const Directory& dir, SegmentMeta& meta,
                                 std::string_view filename) {
  const std::string meta_file = IsNull(filename)
                                  ? FileName<SegmentMetaWriter>(meta)
                                  : std::string{filename};

  auto in =
    dir.open(meta_file, irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE);

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

void PostingsReaderBase::prepare(IndexInput& in, const ReaderState& state,
                                 IndexFeatures features) {
  std::string buf;

  // prepare document input
  PrepareInput(buf, _doc_in, irs::IOAdvice::RANDOM, state,
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
    PrepareInput(buf, _pos_in, irs::IOAdvice::RANDOM, state,
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

    if (IndexFeatures::None !=
        (features & (IndexFeatures::Pay | IndexFeatures::Offs))) {
      // prepare positions input
      PrepareInput(buf, _pay_in, irs::IOAdvice::RANDOM, state,
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

size_t PostingsReaderBase::decode(const byte_type* in, IndexFeatures features,
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

    if (IndexFeatures::None !=
        (features & (IndexFeatures::Pay | IndexFeatures::Offs))) {
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

template<typename FormatTraits>
class PostingsReaderImpl final : public PostingsReaderBase {
 public:
  template<bool Freq, bool Pos, bool Offs, bool Pay>
  struct IteratorTraits : FormatTraits {
    static constexpr bool Frequency() noexcept { return Freq; }
    static constexpr bool Position() noexcept { return Freq && Pos; }
    static constexpr bool Offset() noexcept { return Position() && Offs; }
    static constexpr bool Payload() noexcept { return Position() && Pay; }
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
      if constexpr (Pay) {
        r |= IndexFeatures::Pay;
      }
      return r;
    }
  };

  PostingsReaderImpl() noexcept
    : PostingsReaderBase{FormatTraits::kBlockSize} {}

  DocIterator::ptr iterator(IndexFeatures field_features,
                            IndexFeatures required_features,
                            const TermMeta& meta, uint8_t wand_count) final {
    if (meta.docs_count == 0) {
      SDB_ASSERT(false);
      return DocIterator::empty();
    }

    return IteratorImpl(
      field_features, required_features,
      [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
        if (meta.docs_count == 1) {
          auto it = memory::make_managed<
            SingleDocIterator<IteratorTraits, FieldTraits>>();
          it->Prepare(meta, _pos_in.get(), _pay_in.get());
          return it;
        }
        return ResolveExtent<0>(
          wand_count,
          [&]<typename Extent>(Extent&& extent) -> DocIterator::ptr {
            auto it = memory::make_managed<
              DocIteratorImpl<IteratorTraits, FieldTraits, Extent>>(
              std::forward<Extent>(extent));
            it->Prepare(meta, _doc_in.get(), _pos_in.get(), _pay_in.get());
            return it;
          });
      });
  }

  DocIterator::ptr wanderator(IndexFeatures field_features,
                              IndexFeatures required_features,
                              const TermMeta& meta,
                              const WanderatorOptions& options, WandContext ctx,
                              WandInfo info) final {
    auto it = MakeWanderator(field_features, required_features, meta, options,
                             ctx, info);
    if (it) {
      return it;
    }
    return iterator(field_features, required_features, meta, info.count);
  }

  size_t BitUnion(IndexFeatures field, const term_provider_f& provider,
                  size_t* set, uint8_t wand_count) final;

 private:
  DocIterator::ptr MakeWanderator(IndexFeatures field_features,
                                  IndexFeatures required_features,
                                  const TermMeta& meta,
                                  const WanderatorOptions& options,
                                  WandContext ctx, WandInfo info) {
    if (meta.docs_count == 0 ||
        info.mapped_index == irs::WandContext::kDisable ||
        _scorers.size() <= ctx.index) {
      return {};
    }
    SDB_ASSERT(info.count != 0);
    const auto& scorer = *_scorers[ctx.index];
    const auto scorer_features = scorer.GetIndexFeatures();
    // TODO(mbkkt) Should we also check against required_features?
    //  Or we should to do required_features |= scorer_features;
    if (scorer_features != (field_features & scorer_features)) {
      return {};
    }
    return IteratorImpl(
      field_features, required_features,
      [&]<typename IteratorTraits, typename FieldTraits> -> DocIterator::ptr {
        // No need to use wanderator for short lists
        if (meta.docs_count == 1) {
          auto it = memory::make_managed<
            SingleDocIterator<IteratorTraits, FieldTraits>>();
          it->WandPrepare(meta, _pos_in.get(), _pay_in.get(), options.factory);
          return it;
        }
        return ResolveExtent<1>(
          info.count,
          [&]<typename WandExtent>(WandExtent extent) -> DocIterator::ptr {
            // TODO(mbkkt) Now we don't support wanderator without frequency
            if constexpr (IteratorTraits::Frequency()) {
              // No need to use wanderator for short lists
              if (meta.docs_count > FormatTraits::kBlockSize) {
                return ResolveBool(
                  ctx.root, [&]<bool Root> -> DocIterator::ptr {
                    auto it = memory::make_managed<Wanderator<
                      IteratorTraits, FieldTraits, WandExtent, Root>>(
                      options.factory, scorer, extent, info.mapped_index,
                      ctx.strict);

                    it->WandPrepare(meta, _doc_in.get(), _pos_in.get(),
                                    _pay_in.get());

                    return it;
                  });
              }
            }
            auto it = memory::make_managed<
              DocIteratorImpl<IteratorTraits, FieldTraits, WandExtent>>(extent);
            it->WandPrepare(meta, _doc_in.get(), _pos_in.get(), _pay_in.get(),
                            options.factory, scorer, info.mapped_index);
            return it;
          });
      });
  }

  template<typename FieldTraits, typename Factory>
  DocIterator::ptr IteratorImpl(IndexFeatures enabled, Factory&& factory);

  template<typename Factory>
  DocIterator::ptr IteratorImpl(IndexFeatures field_features,
                                IndexFeatures required_features,
                                Factory&& factory);
};

template<typename FormatTraits>
template<typename FieldTraits, typename Factory>
DocIterator::ptr PostingsReaderImpl<FormatTraits>::IteratorImpl(
  IndexFeatures enabled, Factory&& factory) {
  switch (ToIndex(enabled)) {
    case kPosOffsPay: {
      using IteratorTraits = IteratorTraits<true, true, true, true>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case kPosOffs: {
      using IteratorTraits = IteratorTraits<true, true, true, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case kPosPay: {
      using IteratorTraits = IteratorTraits<true, true, false, true>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case kPos: {
      using IteratorTraits = IteratorTraits<true, true, false, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    case IndexFeatures::Freq: {
      using IteratorTraits = IteratorTraits<true, false, false, false>;
      if constexpr ((FieldTraits::Features() & IteratorTraits::Features()) ==
                    IteratorTraits::Features()) {
        return std::forward<Factory>(factory)
          .template operator()<IteratorTraits, FieldTraits>();
      }
    } break;
    default:
      break;
  }
  using IteratorTraitsT = IteratorTraits<false, false, false, false>;
  return std::forward<Factory>(factory)
    .template operator()<IteratorTraitsT, FieldTraits>();
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
    case kPosOffsPay: {
      using FieldTraits = IteratorTraits<true, true, true, true>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case kPosOffs: {
      using FieldTraits = IteratorTraits<true, true, true, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case kPosPay: {
      using FieldTraits = IteratorTraits<true, true, false, true>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case kPos: {
      using FieldTraits = IteratorTraits<true, true, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    case IndexFeatures::Freq: {
      using FieldTraits = IteratorTraits<true, false, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
    default: {
      using FieldTraits = IteratorTraits<false, false, false, false>;
      return IteratorImpl<FieldTraits>(enabled, std::forward<Factory>(factory));
    }
  }
}

template<typename FieldTraits, size_t N>
void BitUnionImpl(IndexInput& doc_in, doc_id_t docs_count, uint32_t (&docs)[N],
                  uint32_t (&enc_buf)[N], size_t* set) {
  constexpr auto kBits{BitsRequired<std::remove_pointer_t<decltype(set)>>()};
  size_t num_blocks = docs_count / FieldTraits::kBlockSize;

  doc_id_t doc = doc_limits::min();
  while (num_blocks--) {
    FieldTraits::read_block(doc_in, enc_buf, docs);
    if constexpr (FieldTraits::Frequency()) {
      FieldTraits::skip_block(doc_in);
    }

    // FIXME optimize
    for (const auto delta : docs) {
      doc += delta;
      irs::SetBit(set[doc / kBits], doc % kBits);
    }
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

    doc += delta;
    irs::SetBit(set[doc / kBits], doc % kBits);
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
        using FieldTraitsT = IteratorTraits<true, false, false, false>;
        BitUnionImpl<FieldTraitsT>(*doc_in, term_state.docs_count, docs,
                                   enc_buf, set);
      } else {
        using FieldTraitsT = IteratorTraits<false, false, false, false>;
        BitUnionImpl<FieldTraitsT>(*doc_in, term_state.docs_count, docs,
                                   enc_buf, set);
      }

      count += term_state.docs_count;
    } else {
      const doc_id_t doc = doc_limits::min() + term_state.e_single_doc;
      irs::SetBit(set[doc / kBits], doc % kBits);

      ++count;
    }
  }

  return count;
}

class Format15 : public Format {
 public:
  using FormatTraits = FormatTraits;

  static constexpr std::string_view type_name() noexcept { return "1_5"; }

  static ptr make();

  IndexMetaWriter::ptr get_index_meta_writer() const final;
  IndexMetaReader::ptr get_index_meta_reader() const final;

  SegmentMetaWriter::ptr get_segment_meta_writer() const final;
  SegmentMetaReader::ptr get_segment_meta_reader() const final;

  FieldWriter::ptr get_field_writer(
    bool consolidation, IResourceManager& resource_manager) const final;
  FieldReader::ptr get_field_reader(
    IResourceManager& resource_manager) const final;

  ColumnstoreWriter::ptr get_columnstore_writer(
    bool consolidation, IResourceManager& resource_manager) const final;
  ColumnstoreReader::ptr get_columnstore_reader() const final;

  irs::PostingsWriter::ptr get_postings_writer(
    bool consolidation, IResourceManager& resource_manager) const override;
  irs::PostingsReader::ptr get_postings_reader() const override;

  TypeInfo::type_id type() const noexcept override {
    return irs::Type<Format15>::id();
  }
};

static const Format15 kFormaT15Instance;

Format::ptr Format15::make() {
  return {irs::Format::ptr(), &kFormaT15Instance};
}

IndexMetaWriter::ptr Format15::get_index_meta_writer() const {
  return std::make_unique<IndexMetaWriterImpl>(IndexMetaWriterImpl::kFormatMax);
}

IndexMetaReader::ptr Format15::get_index_meta_reader() const {
  // can reuse stateless reader
  static IndexMetaReaderImpl gInstance;
  return memory::to_managed<IndexMetaReader>(gInstance);
}

SegmentMetaWriter::ptr Format15::get_segment_meta_writer() const {
  // can reuse stateless writer
  static SegmentMetaWriterImpl gInstance{SegmentMetaWriterImpl::kFormatMax};
  return memory::to_managed<SegmentMetaWriter>(gInstance);
}

SegmentMetaReader::ptr Format15::get_segment_meta_reader() const {
  // can reuse stateless writer
  static SegmentMetaReaderImpl gInstance;
  return memory::to_managed<SegmentMetaReader>(gInstance);
}

FieldWriter::ptr Format15::get_field_writer(
  bool consolidation, IResourceManager& resource_manager) const {
  return burst_trie::MakeWriter(
    burst_trie::Version::Min,
    get_postings_writer(consolidation, resource_manager), consolidation,
    resource_manager);
}

FieldReader::ptr Format15::get_field_reader(
  IResourceManager& resource_manager) const {
  return burst_trie::MakeReader(get_postings_reader(), resource_manager);
}

ColumnstoreWriter::ptr Format15::get_columnstore_writer(
  bool consolidation, IResourceManager& resource_manager) const {
  return columnstore2::MakeWriter(columnstore2::Version::Min, consolidation,
                                  resource_manager);
}

ColumnstoreReader::ptr Format15::get_columnstore_reader() const {
  return columnstore2::MakeReader();
}

irs::PostingsWriter::ptr Format15::get_postings_writer(
  bool consolidation, IResourceManager& resource_manager) const {
  return std::make_unique<PostingsWriterImpl<FormatTraits>>(
    PostingsFormat::Wand, consolidation, resource_manager);
}

irs::PostingsReader::ptr Format15::get_postings_reader() const {
  return std::make_unique<PostingsReaderImpl<FormatTraits>>();
}

#ifdef IRESEARCH_SSE2

struct FormatTraitsSse4 {
  using AlignType = __m128i;

  static constexpr uint32_t kBlockSize = SIMDBlockSize;
  static_assert(kBlockSize <= doc_limits::eof());

  IRS_FORCE_INLINE static void PackBlock(const uint32_t* IRS_RESTRICT decoded,
                                         uint32_t* IRS_RESTRICT encoded,
                                         uint32_t bits) noexcept {
    ::simdpackwithoutmask(decoded, reinterpret_cast<AlignType*>(encoded), bits);
  }

  IRS_FORCE_INLINE static void UnpackBlock(uint32_t* IRS_RESTRICT decoded,
                                           const uint32_t* IRS_RESTRICT encoded,
                                           uint32_t bits) noexcept {
    ::simdunpack(reinterpret_cast<const AlignType*>(encoded), decoded, bits);
  }

  IRS_FORCE_INLINE static void write_block(IndexOutput& out, const uint32_t* in,
                                           uint32_t* buf) {
    bitpack::write_block32<kBlockSize>(PackBlock, out, in, buf);
  }

  IRS_FORCE_INLINE static void read_block(IndexInput& in, uint32_t* buf,
                                          uint32_t* out) {
    bitpack::read_block32<kBlockSize>(UnpackBlock, in, buf, out);
  }

  IRS_FORCE_INLINE static void skip_block(IndexInput& in) {
    bitpack::skip_block32(in, kBlockSize);
  }
};

class Format15simd final : public Format15 {
 public:
  using FormatTraits = FormatTraitsSse4;

  static constexpr std::string_view type_name() noexcept { return "1_5simd"; }

  static ptr make();

  irs::PostingsWriter::ptr get_postings_writer(
    bool consolidation, IResourceManager& resource_manager) const final;
  irs::PostingsReader::ptr get_postings_reader() const final;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<Format15simd>::id();
  }
};

static const Format15simd kFormaT15SimdInstance;

Format::ptr Format15simd::make() {
  return {irs::Format::ptr{}, &kFormaT15SimdInstance};
}

irs::PostingsWriter::ptr Format15simd::get_postings_writer(
  bool consolidation, IResourceManager& resource_manager) const {
  return std::make_unique<PostingsWriterImpl<FormatTraits>>(
    PostingsFormat::WandSimd, consolidation, resource_manager);
}

irs::PostingsReader::ptr Format15simd::get_postings_reader() const {
  return std::make_unique<PostingsReaderImpl<FormatTraits>>();
}

#endif  // IRESEARCH_SSE2

}  // namespace

void formats::Init() {
  REGISTER_FORMAT(Format15);
#ifdef IRESEARCH_SSE2
  REGISTER_FORMAT(Format15simd);
#endif
}

// use base irs::position type for ancestors
template<typename IteratorTraits, typename FieldTraits, bool Pos>
struct Type<PositionImpl<IteratorTraits, FieldTraits, Pos>> : Type<PosAttr> {};

}  // namespace irs
