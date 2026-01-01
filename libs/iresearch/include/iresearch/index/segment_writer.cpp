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

#include "iresearch/index/segment_writer.hpp"

#include "basics/logger/logger.h"
#include "basics/shared.hpp"
#include "index_meta.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/lz4compression.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

SegmentWriter::StoredColumn::StoredColumn(
  const hashed_string_view& name, ColumnstoreWriter& columnstore,
  IResourceManager& rm, const ColumnInfoProvider& column_info,
  std::deque<CachedColumn, ManagedTypedAllocator<CachedColumn>>& cached_columns,
  bool cache)
  : name{name}, name_hash{name.Hash()} {
  const auto info = column_info(name);

  ColumnFinalizer finalizer{
    [](DataOutput&) noexcept {},
    [this]() noexcept -> std::string_view {
      return std::string_view{this->name};
    },
  };

  // Force to cache column if HNSW index is requested
  if (!cache && !info.hnsw_info) {
    auto column = columnstore.push_column(info, std::move(finalizer));
    id = column.id;
    writer = &column.out;
  } else {
    cached = &cached_columns.emplace_back(&id, info, std::move(finalizer), rm);

    writer = &cached->Stream();
  }
}

doc_id_t SegmentWriter::begin(DocContext ctx, doc_id_t batch_size) {
  SDB_ASSERT(LastDocId() < doc_limits::eof());
  _valid = true;
  SDB_ASSERT(batch_size > 0);
  ResetNorms();

  const auto needed_docs = buffered_docs() + batch_size;

  if (needed_docs >= _docs_mask.set.capacity()) {
    // reserve in blocks of power-of-2
    const auto count = math::RoundupPower2(needed_docs);
    _docs_mask.set.reserve(count);
  }

  _batch_first_doc_id = LastDocId() + 1;
  _docs_context.insert(_docs_context.end(), batch_size, ctx);

  return _batch_first_doc_id;
}

std::unique_ptr<SegmentWriter> SegmentWriter::make(
  Directory& dir, const SegmentWriterOptions& options) {
  return std::make_unique<SegmentWriter>(ConstructToken{}, dir, options);
}

size_t SegmentWriter::memory_active() const noexcept {
  auto column_cache_active = absl::c_accumulate(
    _columns, size_t{0}, [](size_t lhs, const StoredColumn& rhs) noexcept {
      return lhs + rhs.name.size() + sizeof(rhs);
    });

  column_cache_active += absl::c_accumulate(
    _cached_columns, size_t{0}, [](size_t lhs, const CachedColumn& rhs) {
      return lhs + rhs.Stream().MemoryActive();
    });

  return _docs_context.size() * sizeof(DocContext) +
         bitset::bits_to_words(_docs_mask.count) * sizeof(bitset::word_t) +
         _fields.memory_active() + _sort.stream.MemoryActive() +
         column_cache_active;
}

size_t SegmentWriter::memory_reserved() const noexcept {
  auto column_cache_reserved =
    _columns.capacity() * sizeof(decltype(_columns)::value_type);

  column_cache_reserved += absl::c_accumulate(
    _columns, size_t{0}, [](size_t lhs, const StoredColumn& rhs) noexcept {
      return lhs + rhs.name.size();
    });

  column_cache_reserved += absl::c_accumulate(
    _cached_columns, size_t{0}, [](size_t lhs, const CachedColumn& rhs) {
      return lhs + rhs.Stream().MemoryActive() + sizeof(rhs);
    });

  return sizeof(SegmentWriter) + _docs_context.capacity() * sizeof(DocContext) +
         _docs_mask.set.capacity() / BitsRequired<char>() +
         _fields.memory_reserved() + _sort.stream.MemoryReserved() +
         column_cache_reserved;
}

bool SegmentWriter::remove(doc_id_t doc_id) noexcept {
  if (!doc_limits::valid(doc_id)) {
    return false;
  }
  const auto doc = doc_id - doc_limits::min();
  if (buffered_docs() <= doc) {
    return false;
  }
  if (_docs_mask.set.size() <= doc) {
    _docs_mask.set.resize</*Reserve=*/false>(doc + 1);
  }
  const bool inserted = _docs_mask.set.try_set(doc);
  _docs_mask.count += static_cast<size_t>(inserted);
  return inserted;
}

SegmentWriter::SegmentWriter(ConstructToken, Directory& dir,
                             const SegmentWriterOptions& options) noexcept
  : _dir{dir},
    _scorers{options.scorers},
    _cached_columns{{options.resource_manager}},
    _sort{options.column_info, {}, options.resource_manager},
    _docs_context{{options.resource_manager}},
    _fields{options.feature_info, _cached_columns, options.scorers_features,
            options.comparator},
    _columns{{options.resource_manager}},
    _column_info{&options.column_info} {
  _docs_mask.set = decltype(_docs_mask.set){{options.resource_manager}};
}

bool SegmentWriter::index(const hashed_string_view& name, doc_id_t doc,
                          IndexFeatures index_features, Tokenizer& tokens) {
  SDB_ASSERT(_col_writer);

  auto* slot = _fields.emplace(name, index_features, *_col_writer);

  // invert only if new field index features are a subset of slot index features
  if (IsSubsetOf(index_features, slot->requested_features()) &&
      slot->invert(tokens, doc)) {
    if (!slot->seen() && slot->has_features()) {
      _doc.emplace_back(slot);
      slot->seen(true);
    }

    return true;
  }

  _valid = false;
  return false;
}

ColumnOutput& SegmentWriter::stream(const hashed_string_view& name,
                                    const doc_id_t doc_id) {
  SDB_ASSERT(_column_info);
  auto& out = *_columns
                 .lazy_emplace(name,
                               [this, &name](const auto& ctor) {
                                 ctor(name, *_col_writer,
                                      _docs_context.get_allocator().Manager(),
                                      *_column_info, _cached_columns,
                                      nullptr != _fields.comparator());
                               })
                 ->writer;
  out.Prepare(doc_id);
  return out;
}

void SegmentWriter::FlushFields(FlushState& state) {
  SDB_ASSERT(_field_writer);

  try {
    _fields.flush(*_field_writer, state);
  } catch (...) {
    _field_writer.reset();  // invalidate field writer
    throw;
  }
}

[[nodiscard]] DocMap SegmentWriter::flush(IndexSegment& segment,
                                          DocsMask& docs_mask) {
  auto& meta = segment.meta;

  FlushState state{
    .dir = &_dir,
    .columns = this,
    .name = _seg_name,
    .scorers = _scorers,
    .doc_count = buffered_docs(),
  };

  DocMap docmap;
  if (_fields.comparator() != nullptr) {
    std::tie(docmap, _sort.id) = _sort.stream.Flush(
      *_col_writer, std::move(_sort.finalizer),
      static_cast<doc_id_t>(state.doc_count), *_fields.comparator());

    meta.sort = _sort.id;  // Store sorted column id in segment meta

    if (!docmap.empty()) {
      state.docmap = &docmap;
    }
  }

  // Flush all cached columns
  SDB_ASSERT(_column_ids.empty());
  _column_ids.reserve(_cached_columns.size());
  for (BufferedColumn::BufferedValues buffer{_cached_columns.get_allocator()};
       auto& column : _cached_columns) {
    if (!field_limits::valid(column.id())) [[likely]] {
      column.Flush(*_col_writer, docmap, buffer);
    }
    // invalid when was empty column
    if (field_limits::valid(column.id())) [[likely]] {
      [[maybe_unused]] auto [_, emplaced] =
        _column_ids.emplace(column.id(), &column);
      SDB_ASSERT(emplaced);
    }
  }

  // Flush columnstore
  meta.column_store = _col_writer->commit(state);

  // Flush fields metadata & inverted data,
  if (state.doc_count != 0) {
    FlushFields(state);
  }

  // Get document mask
  SDB_ASSERT(_docs_mask.set.count() == _docs_mask.count);
  docs_mask = std::move(_docs_mask);
  _docs_mask.count = 0;

  // Update segment metadata
  meta.docs_count = state.doc_count;
  meta.live_docs_count = meta.docs_count - docs_mask.count;
  meta.files = _dir.FlushTracked(meta.byte_size);

  return docmap;
}

void SegmentWriter::reset() noexcept {
  _initialized = false;
  _dir.ClearTracked();
  _docs_context.clear();
  _docs_mask.set.clear();
  _docs_mask.count = 0;
  _batch_first_doc_id = doc_limits::eof();
  _fields.reset();
  _columns.clear();
  _column_ids.clear();
  _cached_columns.clear();  // FIXME(@gnusi): we loose all per-column buffers
  _sort.stream.Clear();
  if (_col_writer) {
    _col_writer->rollback();
  }
}

void SegmentWriter::reset(const SegmentMeta& meta) {
  reset();

  _seg_name = meta.name;

  if (!_field_writer) {
    _field_writer = meta.codec->get_field_writer(
      false, _docs_context.get_allocator().Manager());
    SDB_ASSERT(_field_writer);
  }

  if (!_col_writer) {
    _col_writer = meta.codec->get_columnstore_writer(
      false, _docs_context.get_allocator().Manager());
    SDB_ASSERT(_col_writer);
  }

  _col_writer->prepare(_dir, meta);

  _initialized = true;
}

}  // namespace irs
