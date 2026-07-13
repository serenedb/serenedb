////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "segment_reader_impl.hpp"

#include <duckdb/common/types.hpp>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

class AllIterator : public DocIterator {
 public:
  explicit AllIterator(doc_id_t docs_count) noexcept
    : _max_doc{doc_limits::min() + docs_count - 1} {}

  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }

  doc_id_t advance() noexcept final {
    _doc = _doc < _max_doc ? _doc + 1 : doc_limits::eof();
    return _doc;
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    _doc = target <= _max_doc ? target : doc_limits::eof();
    return _doc;
  }

  doc_id_t LazySeek(doc_id_t target) noexcept final { return seek(target); }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  uint32_t count() noexcept final {
    if (doc_limits::eof(_doc)) {
      return 0;
    }
    const auto count = _max_doc - _doc;
    _doc = doc_limits::eof();
    return count;
  }

  uint32_t EmitDocs(doc_id_t* out, doc_id_t max) final {
    return EmitDocsImpl(*this, out, max);
  }
  uint32_t EmitScoredDocs(doc_id_t* out, score_t* scores, doc_id_t max,
                          const ScoreFunction& scorer,
                          ColumnArgsFetcher* fetcher, doc_id_t min) final {
    return EmitScoredDocsImpl(*this, out, scores, max, scorer, fetcher, min);
  }
  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  const doc_id_t _max_doc;
};

class MaskDocIterator : public DocIterator {
 public:
  MaskDocIterator(DocIterator::ptr&& it, const DocumentMask& mask) noexcept
    : _mask{mask}, _it{std::move(it)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _it->GetMutable(type);
  }

  doc_id_t advance() final {
    while (true) {
      const auto doc = _it->advance();
      if (!_mask.contains(doc)) {
        return _doc = doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _it->seek(target);
    if (!_mask.contains(doc)) {
      return _doc = doc;
    }
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) final { return seek(target); }

  uint32_t EmitDocs(doc_id_t* out, doc_id_t max) final {
    // Delegate to the child (inherits its specialisation, e.g. posting bulk),
    // then compact out the sparse deleted ids in place.
    const auto raw = _it->EmitDocs(out, max);
    uint32_t n = 0;
    for (uint32_t i = 0; i < raw; ++i) {
      if (!_mask.contains(out[i])) {
        out[n++] = out[i];
      }
    }
    // Keep value() on a live doc >= max, matching advance()'s contract.
    auto doc = _it->value();
    while (!doc_limits::eof(doc) && _mask.contains(doc)) {
      doc = _it->advance();
    }
    _doc = doc;
    return n;
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    return _it->PrepareScore(ctx);
  }

  void FetchScoreArgs(uint16_t index) final { _it->FetchScoreArgs(index); }

  uint32_t count() final { return CountImpl(*this); }
  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }
  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }
  uint32_t EmitScoredDocs(doc_id_t* out, score_t* scores, doc_id_t max,
                          const ScoreFunction& scorer,
                          ColumnArgsFetcher* fetcher, doc_id_t min) final {
    return EmitScoredDocsImpl(*this, out, scores, max, scorer, fetcher, min);
  }

 private:
  const DocumentMask& _mask;
  DocIterator::ptr _it;
};

class MaskedDocIterator : public DocIterator {
 public:
  MaskedDocIterator(doc_id_t begin, doc_id_t end,
                    const DocumentMask& docs_mask) noexcept
    : _docs_mask{docs_mask}, _end{end}, _next{begin} {
    SDB_ASSERT(begin <= end);
    SDB_ASSERT(doc_limits::valid(begin));
    SDB_ASSERT(!doc_limits::eof(end));
  }

  Attribute* GetMutable(TypeInfo::type_id /*type*/) noexcept final {
    return nullptr;
  }

  doc_id_t advance() noexcept final {
    while (_next < _end) {
      _doc = _next++;
      if (!_docs_mask.contains(_doc)) {
        return _doc;
      }
    }
    return _doc = doc_limits::eof();
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    _next = target;
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) noexcept final { return seek(target); }

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  const DocumentMask& _docs_mask;
  const doc_id_t _end;
  doc_id_t _next;
};

FileRefs GetRefs(const Directory& dir, const SegmentMeta& meta) {
  FileRefs file_refs;
  file_refs.reserve(meta.files.size());

  auto& refs = dir.attributes().refs();
  for (auto& file : meta.files) {
    file_refs.emplace_back(refs.add(file));
  }
  return file_refs;
}

}  // namespace

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::Open(
  const Directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) {
  SDB_ASSERT(meta.codec);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = GetRefs(dir, meta);
  reader->_data = std::make_shared<ColumnData>();
  reader->_data->Open(dir, meta, options);
  reader->_field_reader = std::make_shared<burst_trie::FieldReader>(
    meta.codec->get_postings_reader(), *dir.ResourceManager().readers);
  if (options.index) {
    reader->_field_reader->prepare(ReaderState{
      .dir = &dir,
      .meta = &meta,
      .scorer = options.scorer,
      .idx = reader->_data->idx_reader.get(),
    });
  }
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::ReopenReader(
  const Directory& dir, const SegmentMeta& meta,
  const IndexReaderOptions& options) const {
  SDB_ASSERT(meta == _info);
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  reader->_refs = _refs;
  reader->_field_reader = _field_reader;
  reader->_data = std::make_shared<ColumnData>();
  reader->_data->Open(dir, meta, options);
  return reader;
}

std::shared_ptr<const SegmentReaderImpl> SegmentReaderImpl::UpdateMeta(
  const Directory& dir, const SegmentMeta& meta) const {
  auto reader = std::make_shared<SegmentReaderImpl>(PrivateTag{}, meta);
  SDB_ASSERT(_refs == GetRefs(dir, meta));
  reader->_refs = _refs;
  reader->_field_reader = _field_reader;
  reader->_data = _data;
  return reader;
}

uint64_t SegmentReaderImpl::CountMappedMemory() const {
  uint64_t bytes = 0;
  if (_field_reader != nullptr) {
    bytes += _field_reader->CountMappedMemory();
  }
  return bytes;
}

NormReader::ptr SegmentReaderImpl::norms(field_id field) const {
  if (!_data) {
    return {};
  }
  const auto* nc = _data->col_reader->NormColumn(field);
  if (!nc) {
    return {};
  }
  return MakePersistedNormReader(*nc);
}

const ColumnReader* SegmentReaderImpl::Column(field_id field) const {
  return _data->col_reader->Column(field);
}

const CentroidsTree* SegmentReaderImpl::Ivf(field_id field) const {
  if (!_data || !_data->idx_reader) {
    return nullptr;
  }
  return _data->idx_reader->Ivf(field);
}

IndexInput::ptr SegmentReaderImpl::ReopenIvf() const {
  if (!_data || !_data->idx_reader) {
    return nullptr;
  }
  return _data->idx_reader->ReopenIn();
}

DocIterator::ptr SegmentReaderImpl::docs_iterator() const {
  if (!_docs_mask) {
    return memory::make_managed<AllIterator>(_info.docs_count);
  }
  SDB_ASSERT(!_docs_mask->empty());

  return memory::make_managed<MaskedDocIterator>(
    doc_limits::min(), doc_limits::min() + _info.docs_count, *_docs_mask);
}

DocIterator::ptr SegmentReaderImpl::mask(DocIterator::ptr&& it) const {
  SDB_ASSERT(it);
  if (!_docs_mask) {
    return std::move(it);
  }
  SDB_ASSERT(!_docs_mask->empty());

  return memory::make_managed<MaskDocIterator>(std::move(it), *_docs_mask);
}

void SegmentReaderImpl::ColumnData::Open(const Directory& dir,
                                         const SegmentMeta& meta,
                                         const IndexReaderOptions& options) {
  SDB_ASSERT(options.db);
  col_reader = std::make_unique<ColReader>(dir, meta.name, *options.db);
  idx_reader = std::make_unique<IdxReader>(dir, meta.name);
}

}  // namespace irs
