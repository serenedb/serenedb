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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/field_data.hpp"

#include <algorithm>
#include <set>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "basics/object_pool.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/buffered_column_iterator.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/io_utils.hpp"
#include "iresearch/utils/lz4compression.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

const byte_block_pool kEmptyPool;

template<typename Stream>
void WriteOffset(Posting& p, Stream& out, IndexFeatures& features,
                 const uint32_t base, const OffsAttr& offs) {
  const uint32_t start_offset = base + offs.start;
  const uint32_t end_offset = base + offs.end;

  SDB_ASSERT(start_offset >= p.offs);

  WriteVarint<uint32_t>(start_offset - p.offs, out);
  WriteVarint<uint32_t>(end_offset - start_offset, out);

  p.offs = start_offset;
  features |= IndexFeatures::Offs;
}

template<typename Stream>
void WriteProx(Stream& out, uint32_t prox, const irs::PayAttr* pay,
               IndexFeatures& features) {
  if (!pay || pay->value.empty()) {
    WriteVarint<uint32_t>(ShiftPack32(prox, false), out);
  } else {
    WriteVarint<uint32_t>(ShiftPack32(prox, true), out);
    WriteVarint<size_t>(pay->value.size(), out);
    out.write(pay->value.data(), pay->value.size());

    // saw payloads
    features |= IndexFeatures::Pay;
  }
}

template<typename Inserter>
IRS_FORCE_INLINE void WriteCookie(Inserter& out, uint64_t cookie) {
  *out++ = static_cast<byte_type>(cookie);  // offset
  WriteVarint<uint32_t>(cookie >> 8, out);  // slice offset
}

IRS_FORCE_INLINE uint64_t Cookie(size_t slice_offset, size_t offset) noexcept {
  SDB_ASSERT(offset <= std::numeric_limits<uint8_t>::max());
  return static_cast<uint64_t>(slice_offset) << 8 |
         static_cast<byte_type>(offset);
}

template<typename Reader>
IRS_FORCE_INLINE uint64_t ReadCookie(Reader& in) {
  const size_t offset = *in;
  ++in;
  const size_t slice_offset = irs::vread<uint32_t>(in);
  return Cookie(slice_offset, offset);
}

IRS_FORCE_INLINE uint64_t
Cookie(const byte_block_pool::sliced_greedy_inserter& stream) noexcept {
  // we don't span slices over the buffers
  const auto slice_offset = stream.slice_offset();
  return Cookie(slice_offset, stream.pool_offset() - slice_offset);
}

IRS_FORCE_INLINE byte_block_pool::sliced_greedy_reader GreedyReader(
  const byte_block_pool& pool, uint64_t cookie) noexcept {
  return byte_block_pool::sliced_greedy_reader(
    pool, static_cast<size_t>((cookie >> 8) & 0xFFFFFFFF),
    static_cast<size_t>(cookie & 0xFF));
}

IRS_FORCE_INLINE byte_block_pool::sliced_greedy_inserter GreedyWriter(
  byte_block_pool::inserter& writer, uint64_t cookie) noexcept {
  return byte_block_pool::sliced_greedy_inserter(
    writer, static_cast<size_t>((cookie >> 8) & 0xFFFFFFFF),
    static_cast<size_t>(cookie & 0xFF));
}

template<typename Reader>
class PosIteratorImpl final : public irs::PosAttr {
 public:
  PosIteratorImpl() : _prox_in(kEmptyPool) {}

  void Clear() noexcept {
    _pos = 0;
    _value = pos_limits::invalid();
    _offs.clear();
    _pay.value = {};
  }

  // reset field
  void Reset(IndexFeatures features, const FreqAttr& freq) {
    SDB_ASSERT(IndexFeatures::None != (features & IndexFeatures::Freq));

    _freq = &freq;

    std::get<AttributePtr<OffsAttr>>(_attrs) =
      IndexFeatures::None != (features & IndexFeatures::Offs) ? &_offs
                                                              : nullptr;

    std::get<AttributePtr<PayAttr>>(_attrs) =
      IndexFeatures::None != (features & IndexFeatures::Pay) ? &_pay : nullptr;
  }

  // reset value
  void Reset(const Reader& prox) {
    Clear();
    _prox_in = prox;
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  bool next() final {
    SDB_ASSERT(_freq);

    if (_pos == _freq->value) {
      _value = irs::pos_limits::eof();

      return false;
    }

    uint32_t pos;

    if (ShiftUnpack32(irs::vread<uint32_t>(_prox_in), pos)) {
      const size_t size = irs::vread<size_t>(_prox_in);
      _payload_value.resize(size);
      _prox_in.read(const_cast<byte_type*>(_payload_value.data()), size);
      _pay.value = _payload_value;
    }

    _value += pos;
    SDB_ASSERT(pos_limits::valid(_value));

    if (std::get<AttributePtr<OffsAttr>>(_attrs).ptr) {
      _offs.start += irs::vread<uint32_t>(_prox_in);
      _offs.end = _offs.start + irs::vread<uint32_t>(_prox_in);
    }

    ++_pos;

    return true;
  }

 private:
  using Attributes = std::tuple<AttributePtr<OffsAttr>, AttributePtr<PayAttr>>;

  Reader _prox_in;
  bstring _payload_value;
  const FreqAttr* _freq{};  // number of term positions in a document
  PayAttr _pay;
  OffsAttr _offs;
  Attributes _attrs;
  uint32_t _pos{};  // current position
};

}  // namespace

class DocIteratorImpl : public DocIterator {
 public:
  DocIteratorImpl() noexcept : _freq_in(kEmptyPool) {}

  // reset field
  void Reset(const FieldData& field) {
    _field = &field;
    auto& freq = std::get<AttributePtr<FreqAttr>>(_attrs);
    auto& pos = std::get<AttributePtr<PosAttr>>(_attrs);
    freq = nullptr;
    pos = nullptr;
    _has_cookie = false;

    const auto features = field.requested_features();
    if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
      freq = &_freq;

      if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
        _pos.Reset(features, _freq);
        pos = &_pos;
        _has_cookie = field.prox_random_access();
      }
    }
  }

  // reset term
  void Reset(const irs::Posting& posting,
             const byte_block_pool::sliced_reader& freq,
             const byte_block_pool::sliced_reader* prox) {
    std::get<DocAttr>(_attrs).value = 0;
    _freq.value = 0;
    _cookie = 0;
    _freq_in = freq;
    _posting = &posting;

    const auto& ppos = std::get<AttributePtr<PosAttr>>(_attrs);

    if (ppos.ptr && prox) {
      // reset positions only once,
      // as we need iterator for sequential reads
      _pos.Reset(*prox);
    }
  }

  uint64_t Cookie() const noexcept { return _cookie; }

  size_t Cost() const noexcept { return _posting->size; }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (_freq_in.eof()) {
      if (!_posting) {
        doc_value = doc_limits::eof();
        return false;
      }

      doc_value = _posting->doc;
      _freq.value = _posting->freq;

      if (_has_cookie) {
        // read last cookie
        _cookie = *_field->_int_writer->parent().seek(_posting->int_start + 3);
      }

      _posting = nullptr;
    } else {
      if (std::get<AttributePtr<FreqAttr>>(_attrs).ptr) {
        uint64_t delta;

        if (ShiftUnpack64(irs::vread<uint64_t>(_freq_in), delta)) {
          _freq.value = 1U;
        } else {
          _freq.value = irs::vread<uint32_t>(_freq_in);
        }

        SDB_ASSERT(delta < doc_limits::eof());
        doc_value += doc_id_t(delta);

        if (_has_cookie) {
          _cookie += ReadCookie(_freq_in);
        }
      } else {
        doc_value += irs::vread<uint32_t>(_freq_in);
      }

      SDB_ASSERT(doc_value != _posting->doc);
    }

    _pos.Clear();

    return true;
  }

  doc_id_t seek(doc_id_t doc) final {
    irs::seek(*this, doc);
    return value();
  }

 private:
  using Attributes =
    std::tuple<DocAttr, AttributePtr<FreqAttr>, AttributePtr<PosAttr>>;

  const FieldData* _field{};
  uint64_t _cookie{};
  FreqAttr _freq;
  PosIteratorImpl<byte_block_pool::sliced_reader> _pos;
  byte_block_pool::sliced_reader _freq_in;
  const Posting* _posting{};
  Attributes _attrs;
  bool _has_cookie{false};  // FIXME remove
};

class SortingDocIteratorImpl : public DocIterator {
 public:
  // reset field
  void Reset(const FieldData& field) {
    SDB_ASSERT(field.prox_random_access());
    _byte_pool = &field._byte_writer->parent();

    auto& pfreq = std::get<AttributePtr<FreqAttr>>(_attrs);
    auto& ppos = std::get<AttributePtr<PosAttr>>(_attrs);
    pfreq = nullptr;
    ppos = nullptr;

    const auto features = field.requested_features();
    if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
      pfreq = &_freq;

      if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
        _pos.Reset(features, _freq);
        ppos = &_pos;
      }
    }
  }

  // reset iterator,
  // docmap == null -> segment is already sorted
  void Reset(DocIteratorImpl& it, const DocMap* docmap) {
    const FreqAttr no_frequency;
    const FreqAttr* freq = &no_frequency;

    const auto* freq_attr = irs::get<FreqAttr>(it);
    if (freq_attr) {
      freq = freq_attr;
    }

    _docs.reserve(it.Cost());
    _docs.clear();

    if (!docmap) {
      ResetAlreadySorted(it, *freq);
    } else if (irs::UseDenseSort(it.Cost(),
                                 docmap->size() - 1)) {  // -1 for first element
      ResetDense(it, *freq, *docmap);
    } else {
      ResetSparse(it, *freq, *docmap);
    }

    std::get<DocAttr>(_attrs).value = irs::doc_limits::invalid();
    _freq.value = 0;
    _it = _docs.begin();
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() noexcept final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    while (_it != _docs.end()) {
      if (doc_limits::eof(_it->doc)) {
        // skip invalid docs
        ++_it;
        continue;
      }

      auto& doc = *_it;
      doc_value = doc.doc;
      _freq.value = doc.freq;

      if (doc.cookie) {  // we have proximity data
        _pos.Reset(GreedyReader(*_byte_pool, doc.cookie));
      }

      ++_it;
      return true;
    }

    _freq.value = 0;
    doc_value = doc_limits::eof();
    return false;
  }

  doc_id_t seek(doc_id_t doc) noexcept final {
    irs::seek(*this, doc);
    return value();
  }

 private:
  using Attributes =
    std::tuple<DocAttr, AttributePtr<FreqAttr>, AttributePtr<PosAttr>>;

  struct DocEntry {
    DocEntry() = default;
    DocEntry(doc_id_t doc, uint32_t freq, uint64_t cookie) noexcept
      : doc(doc), freq(freq), cookie(cookie) {}

    doc_id_t doc{doc_limits::eof()};  // doc_id
    uint32_t freq;                    // freq
    uint64_t cookie;                  // prox_cookie
  };

  void ResetDense(DocIteratorImpl& it, const FreqAttr& freq,
                  std::span<const doc_id_t> docmap) {
    SDB_ASSERT(!docmap.empty());
    SDB_ASSERT(irs::UseDenseSort(it.Cost(),
                                 docmap.size() - 1));  // -1 for first element

    _docs.resize(docmap.size() - 1);  // -1 for first element

    while (it.next()) {
      SDB_ASSERT(it.value() - doc_limits::min() < docmap.size());
      const auto new_doc = docmap[it.value()];

      if (doc_limits::eof(new_doc)) {
        // skip invalid documents
        continue;
      }

      auto& doc = _docs[new_doc - doc_limits::min()];
      doc.doc = new_doc;
      doc.freq = freq.value;
      doc.cookie = it.Cookie();
    }
  }

  void ResetSparse(DocIteratorImpl& it, const FreqAttr& freq,
                   std::span<const doc_id_t> docmap) {
    SDB_ASSERT(!docmap.empty());
    SDB_ASSERT(!irs::UseDenseSort(it.Cost(),
                                  docmap.size() - 1));  // -1 for first element

    while (it.next()) {
      SDB_ASSERT(it.value() - doc_limits::min() < docmap.size());
      const auto new_doc = docmap[it.value()];

      if (doc_limits::eof(new_doc)) {
        // skip invalid documents
        continue;
      }

      _docs.emplace_back(new_doc, freq.value, it.Cookie());
    }

    absl::c_sort(_docs, [](const DocEntry& lhs, const DocEntry& rhs) noexcept {
      return lhs.doc < rhs.doc;
    });
  }

  void ResetAlreadySorted(DocIteratorImpl& it, const FreqAttr& freq) {
    while (it.next()) {
      _docs.emplace_back(it.value(), freq.value, it.Cookie());
    }
  }

  const byte_block_pool* _byte_pool{};
  std::vector<DocEntry>::const_iterator _it;
  std::vector<DocEntry> _docs;
  PosIteratorImpl<byte_block_pool::sliced_greedy_reader> _pos;
  FreqAttr _freq;
  Attributes _attrs;
};

class TermIteratorImpl : public irs::TermIterator {
 public:
  explicit TermIteratorImpl(FieldsData::postings_ref_t& postings,
                            const DocMap* docmap) noexcept
    : _postings(&postings), _doc_map(docmap) {}

  void Reset(const FieldData& field, bytes_view& min, bytes_view& max) {
    _field = &field;

    _doc_itr.Reset(field);
    if (field.prox_random_access()) {
      _sorting_doc_itr.Reset(field);
    }

    // reset state
    _field->_terms.get_sorted_postings(*_postings);
    _next = _it = _postings->begin();
    _end = _postings->end();

    max = min = {};
    if (_it != _end) {
      min = (*_it)->term;
      max = (*(_end - 1))->term;
    }
  }

  bytes_view value() const noexcept final {
    SDB_ASSERT(_it != _end);
    return (*_it)->term;
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  void read() noexcept final {
    // Does nothing now
  }

  DocIterator::ptr postings(IndexFeatures /*features*/) const final {
    SDB_ASSERT(_it != _end);

    return (this->*kPostings[size_t(_field->prox_random_access())])(**_it);
  }

  bool next() final {
    if (_next == _end) {
      return false;
    }

    _it = _next;
    ++_next;
    return true;
  }

  const FieldMeta& Meta() const noexcept { return _field->meta(); }

 private:
  using PostingsF =
    DocIterator::ptr (TermIteratorImpl::*)(const Posting&) const;

  DocIterator::ptr Postings(const Posting& posting) const {
    SDB_ASSERT(!_doc_map);

    // where the term data starts
    auto ptr = _field->_int_writer->parent().seek(posting.int_start);
    const auto freq_end = *ptr;
    ++ptr;
    const auto prox_end = *ptr;
    ++ptr;
    const auto freq_begin = *ptr;
    ++ptr;
    const auto prox_begin = *ptr;

    auto& pool = _field->_byte_writer->parent();
    const byte_block_pool::sliced_reader freq(pool, freq_begin,
                                              freq_end);  // term's frequencies
    const byte_block_pool::sliced_reader prox(
      pool, prox_begin,
      prox_end);  // term's proximity // TODO: create on demand!!!

    _doc_itr.Reset(posting, freq, &prox);
    return memory::to_managed<DocIterator>(_doc_itr);
  }

  DocIterator::ptr SortPostings(const Posting& posting) const {
    // where the term data starts
    auto ptr = _field->_int_writer->parent().seek(posting.int_start);
    const auto freq_end = *ptr;
    ++ptr;
    const auto freq_begin = *ptr;

    auto& pool = _field->_byte_writer->parent();
    const byte_block_pool::sliced_reader freq(pool, freq_begin,
                                              freq_end);  // term's frequencies

    _doc_itr.Reset(posting, freq, nullptr);
    _sorting_doc_itr.Reset(_doc_itr, _doc_map);
    return memory::to_managed<DocIterator>(_sorting_doc_itr);
  }

  static inline const PostingsF kPostings[2]{&TermIteratorImpl::Postings,
                                             &TermIteratorImpl::SortPostings};

  FieldsData::postings_ref_t* _postings{};
  FieldsData::postings_ref_t::const_iterator _end;
  FieldsData::postings_ref_t::const_iterator _next;
  FieldsData::postings_ref_t::const_iterator _it;
  const FieldData* _field{};
  const DocMap* _doc_map{};
  mutable DocIteratorImpl _doc_itr;
  mutable SortingDocIteratorImpl _sorting_doc_itr;
};

namespace {

class TermReaderImpl final : public irs::BasicTermReader,
                             private util::Noncopyable {
 public:
  explicit TermReaderImpl(FieldsData::postings_ref_t& postings,
                          const DocMap* docmap) noexcept
    : _it(postings, docmap) {}

  void Reset(const FieldData& field) { _it.Reset(field, _min, _max); }

  bytes_view(min)() const noexcept final { return _min; }

  bytes_view(max)() const noexcept final { return _max; }

  const FieldMeta& Meta() const noexcept { return _it.Meta(); }

  std::string_view name() const noexcept final { return Meta().name; }

  FieldProperties properties() const noexcept final { return Meta(); }

  irs::TermIterator::ptr iterator() const noexcept final {
    return memory::to_managed<irs::TermIterator>(_it);
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

 private:
  mutable TermIteratorImpl _it;
  bytes_view _min{};
  bytes_view _max{};
};

}  // namespace

ResettableDocIterator::ptr CachedColumn::iterator(ColumnHint hint) const {
  // kPrevDoc isn't supported atm
  SDB_ASSERT(ColumnHint::Normal == (hint & ColumnHint::PrevDoc));

  // FIXME(gnusi): can avoid allocation with the help of managed_ptr
  return memory::make_managed<BufferedColumnIterator>(_stream.Index(),
                                                      _stream.Data());
}

FieldData::FieldData(std::string_view name,
                     const FeatureInfoProvider& feature_columns,
                     CachedColumns& cached_columns,
                     IndexFeatures cached_features, ColumnstoreWriter& columns,
                     byte_block_pool::inserter& byte_writer,
                     int_block_pool::inserter& int_writer,
                     IndexFeatures index_features, bool random_access)
  // Unset optional features
  : _meta{name, index_features & (~(IndexFeatures::Offs | IndexFeatures::Pay))},
    _terms{*byte_writer},
    _byte_writer{&byte_writer},
    _int_writer{&int_writer},
    _proc_table{kTermProcessingTables[size_t(random_access)]},
    _requested_features{index_features},
    _last_doc{doc_limits::invalid()} {
  auto& rm = cached_columns.get_allocator().Manager();

  auto write_feature = [&](IndexFeatures feature, field_id* id) {
    if (!IsSubsetOf(feature, index_features)) {
      return;
    }

    SDB_ASSERT(feature_columns);
    auto [feature_column_info, feature_writer_factory] =
      feature_columns(feature);

    auto feature_writer =
      feature_writer_factory ? (*feature_writer_factory)({}) : nullptr;
    *id = field_limits::invalid();
    if (!feature_writer) {
      return;
    }
    ColumnFinalizer finalizer = ColumnFinalizer{
      [writer = feature_writer.get()](DataOutput& out) { writer->finish(out); },
      [] { return std::string_view{}; },
    };

    // sorted index case or the feature is required for wand
    if (random_access || IsSubsetOf(feature, cached_features)) {
      auto& column = cached_columns.emplace_back(id, feature_column_info,
                                                 std::move(finalizer), rm);
      _features.emplace_back(std::move(feature_writer), column.Stream());
    } else {
      auto [column, out] =
        columns.push_column(feature_column_info, std::move(finalizer));
      _features.emplace_back(std::move(feature_writer), out);
      *id = column;
    }
  };

  write_feature(IndexFeatures::Norm, &_meta.norm);

  SDB_ASSERT(!field_limits::valid(_meta.norm) ||
             IsSubsetOf(IndexFeatures::Norm, _meta.index_features));
}

void FieldData::reset(doc_id_t doc_id) {
  SDB_ASSERT(doc_limits::valid(doc_id));

  if (doc_id == _last_doc) {
    return;  // nothing to do
  }

  _pos = pos_limits::invalid();
  _last_pos = 0;
  _stats = {};
  _offs = 0;
  _last_start_offs = 0;
  _last_doc = doc_id;
  _seen = false;
}

void FieldData::new_term(Posting& p, doc_id_t did, const PayAttr* pay,
                         const OffsAttr* offs) {
  // where pointers to data starts
  p.int_start = _int_writer->pool_offset();

  const auto freq_start =
    _byte_writer->alloc_slice();  // pointer to freq stream
  const auto prox_start =
    _byte_writer->alloc_slice();  // pointer to prox stream
  *_int_writer = freq_start;      // freq stream end
  *_int_writer = prox_start;      // prox stream end
  *_int_writer = freq_start;      // freq stream start
  *_int_writer = prox_start;      // prox stream start

  p.doc = did;
  if (IndexFeatures::None == (_requested_features & IndexFeatures::Freq)) {
    p.doc_code = did;
  } else {
    p.doc_code = uint64_t(did) << 1;
    p.freq = 1;

    if (IndexFeatures::None != (_requested_features & IndexFeatures::Pos)) {
      auto& prox_stream_end = *_int_writer->parent().seek(p.int_start + 1);
      byte_block_pool::sliced_inserter prox_out(*_byte_writer, prox_stream_end);

      WriteProx(prox_out, _pos, pay, _meta.index_features);

      if (offs) {
        WriteOffset(p, prox_out, _meta.index_features, _offs, *offs);
      }

      prox_stream_end = prox_out.pool_offset();
      p.pos = _pos;
    }
  }

  _stats.max_term_freq = std::max(1U, _stats.max_term_freq);
  ++_stats.num_unique;
}

void FieldData::add_term(Posting& p, doc_id_t did, const PayAttr* pay,
                         const OffsAttr* offs) {
  if (IndexFeatures::None == (_requested_features & IndexFeatures::Freq)) {
    if (p.doc != did) {
      SDB_ASSERT(did > p.doc);

      auto& doc_stream_end = *_int_writer->parent().seek(p.int_start);
      byte_block_pool::sliced_inserter doc_out(*_byte_writer, doc_stream_end);
      WriteVarint<uint32_t>(p.doc_code, doc_out);
      doc_stream_end = doc_out.pool_offset();

      p.doc_code = did - p.doc;
      p.doc = did;
      ++_stats.num_unique;
    }
  } else if (p.doc != did) {
    SDB_ASSERT(did > p.doc);

    auto& doc_stream_end = *_int_writer->parent().seek(p.int_start);
    byte_block_pool::sliced_inserter doc_out(*_byte_writer, doc_stream_end);

    if (1U == p.freq) {
      WriteVarint<uint64_t>(p.doc_code | UINT64_C(1), doc_out);
    } else {
      WriteVarint<uint64_t>(p.doc_code, doc_out);
      WriteVarint<uint32_t>(p.freq, doc_out);
    }

    p.doc_code = uint64_t(did - p.doc) << 1;
    p.freq = 1;

    p.doc = did;
    _stats.max_term_freq = std::max(1U, _stats.max_term_freq);
    ++_stats.num_unique;

    if (IndexFeatures::None != (_requested_features & IndexFeatures::Pos)) {
      auto& prox_stream_end = *_int_writer->parent().seek(p.int_start + 1);
      byte_block_pool::sliced_inserter prox_out(*_byte_writer, prox_stream_end);

      WriteProx(prox_out, _pos, pay, _meta.index_features);

      if (offs) {
        p.offs = 0;  // reset base offset
        WriteOffset(p, prox_out, _meta.index_features, _offs, *offs);
      }

      prox_stream_end = prox_out.pool_offset();
      p.pos = _pos;
    }

    doc_stream_end = doc_out.pool_offset();
  } else {  // exists in current doc
    _stats.max_term_freq = std::max(++p.freq, _stats.max_term_freq);
    if (IndexFeatures::None != (_requested_features & IndexFeatures::Pos)) {
      auto& prox_stream_end = *_int_writer->parent().seek(p.int_start + 1);
      byte_block_pool::sliced_inserter prox_out(*_byte_writer, prox_stream_end);

      WriteProx(prox_out, _pos - p.pos, pay, _meta.index_features);

      if (offs) {
        WriteOffset(p, prox_out, _meta.index_features, _offs, *offs);
      }

      prox_stream_end = prox_out.pool_offset();
      p.pos = _pos;
    }
  }
}

void FieldData::new_term_random_access(Posting& p, doc_id_t did,
                                       const PayAttr* pay,
                                       const OffsAttr* offs) {
  // where pointers to data starts
  p.int_start = _int_writer->pool_offset();

  const auto freq_start =
    _byte_writer->alloc_slice();  // pointer to freq stream
  const auto prox_start =
    _byte_writer->alloc_greedy_slice();  // pointer to prox stream
  *_int_writer = freq_start;             // freq stream end
  *_int_writer = freq_start;             // freq stream start

  const auto cookie = Cookie(prox_start, 1);
  *_int_writer = cookie;  // end cookie
  *_int_writer = cookie;  // start cookie
  *_int_writer = 0;       // last start cookie

  p.doc = did;
  if (IndexFeatures::None == (_requested_features & IndexFeatures::Freq)) {
    p.doc_code = did;
  } else {
    p.doc_code = uint64_t(did) << 1;
    p.freq = 1;

    if (IndexFeatures::None != (_requested_features & IndexFeatures::Pos)) {
      byte_block_pool::sliced_greedy_inserter prox_out(*_byte_writer,
                                                       prox_start, 1);

      WriteProx(prox_out, _pos, pay, _meta.index_features);

      if (offs) {
        WriteOffset(p, prox_out, _meta.index_features, _offs, *offs);
      }

      auto& end_cookie = *_int_writer->parent().seek(p.int_start + 2);
      end_cookie = Cookie(prox_out);  // prox stream end cookie

      p.pos = _pos;
    }
  }

  _stats.max_term_freq = std::max(1U, _stats.max_term_freq);
  ++_stats.num_unique;
}

void FieldData::add_term_random_access(Posting& p, doc_id_t did,
                                       const PayAttr* pay,
                                       const OffsAttr* offs) {
  if (IndexFeatures::None == (_requested_features & IndexFeatures::Freq)) {
    if (p.doc != did) {
      SDB_ASSERT(did > p.doc);

      auto& doc_stream_end = *_int_writer->parent().seek(p.int_start);
      byte_block_pool::sliced_inserter doc_out(*_byte_writer, doc_stream_end);
      WriteVarint<uint32_t>(p.doc_code, doc_out);
      doc_stream_end = doc_out.pool_offset();

      ++p.size;
      p.doc_code = did - p.doc;
      p.doc = did;
      ++_stats.num_unique;
    }
  } else if (p.doc != did) {
    SDB_ASSERT(did > p.doc);

    auto& doc_stream_end = *_int_writer->parent().seek(p.int_start);
    byte_block_pool::sliced_inserter doc_out(*_byte_writer, doc_stream_end);

    if (1U == p.freq) {
      WriteVarint<uint64_t>(p.doc_code | UINT64_C(1), doc_out);
    } else {
      WriteVarint<uint64_t>(p.doc_code, doc_out);
      WriteVarint<uint32_t>(p.freq, doc_out);
    }

    ++p.size;
    p.doc_code = uint64_t(did - p.doc) << 1;
    p.freq = 1;

    p.doc = did;
    _stats.max_term_freq = std::max(1U, _stats.max_term_freq);
    ++_stats.num_unique;

    if (IndexFeatures::None != (_requested_features & IndexFeatures::Pos)) {
      auto prox_stream_cookie = _int_writer->parent().seek(p.int_start + 2);

      auto& end_cookie = *prox_stream_cookie;
      ++prox_stream_cookie;
      auto& start_cookie = *prox_stream_cookie;
      ++prox_stream_cookie;
      auto& last_start_cookie = *prox_stream_cookie;

      WriteCookie(doc_out, start_cookie - last_start_cookie);
      last_start_cookie = start_cookie;  // update previous cookie
      start_cookie = end_cookie;         // update start cookie

      auto prox_out = GreedyWriter(*_byte_writer, end_cookie);

      WriteProx(prox_out, _pos, pay, _meta.index_features);

      if (offs) {
        p.offs = 0;  // reset base offset
        WriteOffset(p, prox_out, _meta.index_features, _offs, *offs);
      }

      end_cookie = Cookie(prox_out);
      p.pos = _pos;
    }

    doc_stream_end = doc_out.pool_offset();
  } else {  // exists in current doc
    _stats.max_term_freq = std::max(++p.freq, _stats.max_term_freq);
    if (IndexFeatures::None != (_requested_features & IndexFeatures::Pos)) {
      // update end cookie
      auto& end_cookie = *_int_writer->parent().seek(p.int_start + 2);
      auto prox_out = GreedyWriter(*_byte_writer, end_cookie);

      WriteProx(prox_out, _pos - p.pos, pay, _meta.index_features);

      if (offs) {
        WriteOffset(p, prox_out, _meta.index_features, _offs, *offs);
      }

      end_cookie = Cookie(prox_out);
      p.pos = _pos;
    }
  }
}

bool FieldData::invert(Tokenizer& stream, doc_id_t id) {
  SDB_ASSERT(id < doc_limits::eof());  // 0-based document id

  const auto* term = get<TermAttr>(stream);
  const auto* inc = get<IncAttr>(stream);
  const OffsAttr* offs = nullptr;
  const PayAttr* pay = nullptr;

  if (!inc) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "field '", _meta.name,
              "' missing required token_stream attribute '",
              Type<IncAttr>::name(), "'");
    return false;
  }

  if (!term) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "field '", _meta.name,
              "' missing required token_stream attribute '",
              Type<TermAttr>::name(), "'");
    return false;
  }

  if (IndexFeatures::None != (_requested_features & IndexFeatures::Offs)) {
    offs = get<OffsAttr>(stream);

    if (offs) {
      pay = get<PayAttr>(stream);
    }
  }

  reset(id);  // initialize field_data for the supplied doc_id

  while (stream.next()) {
    _pos += inc->value;

    if (_pos < _last_pos) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "invalid position ", _pos,
                " < ", _last_pos, " in field '", _meta.name, "'");
      return false;
    }

    if (_pos >= pos_limits::eof()) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "invalid position ", _pos,
                " >= ", pos_limits::eof(), " in field '", _meta.name, "'");
      return false;
    }

    if (0 == inc->value) {
      ++_stats.num_overlap;
    }

    if (offs) {
      const uint32_t start_offset = _offs + offs->start;
      const uint32_t end_offset = _offs + offs->end;

      if (start_offset < _last_start_offs || end_offset < start_offset) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "invalid offset start=", start_offset, " end=", end_offset,
                  " in field '", _meta.name, "'");
        return false;
      }

      _last_start_offs = start_offset;
    }

    auto* p = _terms.emplace(term->value);

    if (p == nullptr) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "skipping too long term of size: ", term->value.size(),
               " in field: ", _meta.name);
      SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH, "field: ", _meta.name,
                " contains too long term: ", ViewCast<char>(term->value));
      continue;
    }

    (this->*_proc_table[!doc_limits::valid(p->doc)])(*p, id, pay, offs);
    SDB_ASSERT(doc_limits::valid(p->doc));

    if (0 == ++_stats.len) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "too many tokens in field: ", _meta.name, ", document: ", id);
      return false;
    }

    _last_pos = _pos;
  }

  if (offs) {
    _offs += offs->end;
  }

  return true;
}

FieldsData::FieldsData(const FeatureInfoProvider& feature_info,
                       FieldData::CachedColumns& cached_columns,
                       IndexFeatures cached_features,
                       const Comparer* comparator)
  : _comparator{comparator},
    _feature_info{&feature_info},
    _fields{cached_columns.get_allocator()},
    _cached_columns{&cached_columns},
    _byte_pool{cached_columns.get_allocator()},
    _byte_writer{_byte_pool.begin()},
    _int_pool{cached_columns.get_allocator()},
    _int_writer{_int_pool.begin()},
    _cached_features{cached_features} {}

FieldData* FieldsData::emplace(const hashed_string_view& name,
                               IndexFeatures index_features,
                               ColumnstoreWriter& columns) {
  SDB_ASSERT(_fields_map.size() == _fields.size());

  auto it = _fields_map.lazy_emplace(
    name, [&name](const auto& ctor) { ctor(nullptr, name.Hash()); });

  if (!it->ref) {
    try {
      const_cast<FieldData*&>(it->ref) = &_fields.emplace_back(
        name, *_feature_info, *_cached_columns, _cached_features, columns,
        _byte_writer, _int_writer, index_features, (nullptr != _comparator));
    } catch (...) {
      _fields_map.erase(it);
      throw;
    }
  }

  return it->ref;
}

void FieldsData::flush(FieldWriter& fw, FlushState& state) {
  IndexFeatures index_features{IndexFeatures::None};

  // sort fields
  _sorted_fields.resize(_fields.size());
  auto begin = _sorted_fields.begin();
  for (auto& entry : _fields) {
    *begin = &entry;
    ++begin;

    const auto& meta = entry.meta();
    index_features |= static_cast<IndexFeatures>(meta.index_features);
  }

  state.index_features = static_cast<IndexFeatures>(index_features);

  absl::c_sort(_sorted_fields,
               [](const FieldData* lhs, const FieldData* rhs) noexcept {
                 return lhs->meta().name < rhs->meta().name;
               });

  TermReaderImpl terms(_sorted_postings, state.docmap);

  fw.prepare(state);
  for (auto* field : _sorted_fields) {
    // Reset reader
    terms.Reset(*field);

    // Write inverted data
    fw.write(terms);
  }

  fw.end();
}

void FieldsData::reset() noexcept {
  _byte_writer = _byte_pool.begin();  // reset position pointer to start of pool
  _fields.clear();
  _fields_map.clear();
  _int_writer = _int_pool.begin();  // reset position pointer to start of pool
}

size_t FieldsData::memory_active() const noexcept {
  return _byte_writer.pool_offset() +
         _int_writer.pool_offset() * sizeof(int_block_pool::value_type) +
         _fields_map.size() * sizeof(FieldsMap::value_type) +
         _fields.size() * sizeof(decltype(_fields)::value_type);
}

size_t FieldsData::memory_reserved() const noexcept {
  // FIXME(@gnusi): revisit the implementation
  return _byte_pool.size() + _int_pool.size();
}

template<typename Reader>
struct Type<PosIteratorImpl<Reader>> : Type<PosAttr> {};

}  // namespace irs
