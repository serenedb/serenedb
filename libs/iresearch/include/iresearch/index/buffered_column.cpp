////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "buffered_column.hpp"

#include "basics/down_cast.h"
#include "basics/shared.hpp"
#include "iresearch/index/buffered_column_iterator.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

BufferedColumn::BufferedColumn(const ColumnInfo& info, IResourceManager& rm)
  : _data_buf{{rm}},
    _index{{rm}},
    _info{info},
    _hnsw_index{
      _info.hnsw_info
        ? std::make_unique<HNSWIndexWriter>(
            *_info.hnsw_info, [&] { return Iterator(); },
            [&](ResettableDocIterator::ptr& it) {
              sdb::basics::downCast<BufferedColumnIterator>(*it).Reset(
                _index, _data_buf);
            })
        : nullptr} {}

bool BufferedColumn::FlushSparsePrimary(DocMap& docmap, ColumnOutput& writer,
                                        doc_id_t docs_count,
                                        const Comparer& compare) {
  auto comparer = [&](const auto& lhs, const auto& rhs) {
    return compare.Compare(GetPayload(lhs), GetPayload(rhs));
  };

  if (absl::c_is_sorted(_index, [&](const auto& lhs, const auto& rhs) {
        return comparer(lhs, rhs) < 0;
      })) {
    return false;
  }

  docmap.resize(doc_limits::min() + docs_count);

  std::vector<size_t> sorted_index(_index.size());
  absl::c_iota(sorted_index, 0);
  absl::c_sort(sorted_index, [&](size_t lhs, size_t rhs) {
    SDB_ASSERT(lhs < _index.size());
    SDB_ASSERT(rhs < _index.size());
    if (const auto r = comparer(_index[lhs], _index[rhs]); r) {
      return r < 0;
    }
    return lhs < rhs;
  });

  doc_id_t new_doc = doc_limits::min();

  for (size_t idx : sorted_index) {
    const auto* value = &_index[idx];

    doc_id_t min = doc_limits::min();
    if (idx) [[likely]] {
      min += std::prev(value)->key;
    }

    for (const doc_id_t max = value->key; min < max; ++min) {
      docmap[min] = new_doc++;
    }

    docmap[min] = new_doc;
    writer.Prepare(new_doc);
    WriteValue(writer, *value);
    ++new_doc;
  }

  // Ensure that all docs up to new_doc are remapped without gaps
  SDB_ASSERT(std::all_of(docmap.begin() + 1, docmap.begin() + new_doc,
                         [](doc_id_t doc) { return doc_limits::valid(doc); }));
  // Ensure we reached the last doc in sort column
  SDB_ASSERT((std::prev(_index.end(), 1)->key + 1) == new_doc);
  // Handle docs without sort value that are placed after last filled sort doc
  for (auto begin = std::next(docmap.begin(), new_doc); begin != docmap.end();
       ++begin) {
    SDB_ASSERT(!doc_limits::valid(*begin));
    *begin = new_doc++;
  }

  return true;
}

std::pair<DocMap, field_id> BufferedColumn::Flush(ColumnstoreWriter& writer,
                                                  ColumnFinalizer finalizer,
                                                  doc_id_t docs_count,
                                                  const Comparer& compare) {
  SDB_ASSERT(_index.size() <= docs_count);
  SDB_ASSERT(_index.empty() || _index.back().key <= docs_count);

  Prepare(doc_limits::kEOF);  // Insert last pending value

  if (_index.empty()) [[unlikely]] {
    return {DocMap{_index.get_allocator()}, field_limits::invalid()};
  }

  DocMap docmap{_index.get_allocator()};

  if (_hnsw_index) {
    finalizer = ColumnFinalizer{
      [payload_finalizer = finalizer.ExtractPayloadFinalizer(),
       hnsw = std::move(_hnsw_index)](DataOutput& out) mutable {
        SDB_ASSERT(hnsw);
        hnsw->Serialize(out);
        payload_finalizer(out);
      },
      finalizer.ExtractNameFinalizer(),
    };
  }
  auto [column_id, column_writer] =
    writer.push_column(_info, std::move(finalizer));

  if (!FlushSparsePrimary(docmap, column_writer, docs_count, compare)) {
    FlushAlreadySorted(column_writer);
  }

  return {std::move(docmap), column_id};
}

void BufferedColumn::FlushAlreadySorted(ColumnOutput& writer) {
  for (const auto& value : _index) {
    writer.Prepare(value.key);
    WriteValue(writer, value);
  }
}

bool BufferedColumn::FlushDense(ColumnOutput& writer, DocMapView docmap,
                                BufferedValues& buffer) {
  SDB_ASSERT(!docmap.empty());

  const size_t total = docmap.size() - 1;  // -1 for the first element
  const size_t size = _index.size();

  if (!UseDenseSort(size, total)) {
    return false;
  }

  buffer.clear();
  buffer.resize(total);
  for (const auto& old_value : _index) {
    const auto new_key = docmap[old_value.key];
    auto& new_value = buffer[new_key - doc_limits::min()];
    new_value.key = new_key;
    new_value.begin = old_value.begin;
    new_value.size = old_value.size;
  }

  _index.clear();

  for (const auto& new_value : buffer) {
    if (doc_limits::valid(new_value.key)) {
      writer.Prepare(new_value.key);
      WriteValue(writer, new_value);
      _index.emplace_back(new_value);
    }
  }

  return true;
}

void BufferedColumn::FlushSparse(ColumnOutput& writer, DocMapView docmap) {
  SDB_ASSERT(!docmap.empty());

  for (auto& value : _index) {
    value.key = docmap[value.key];
  }

  absl::c_sort(_index, [](const auto& lhs, const auto& rhs) noexcept {
    return lhs.key < rhs.key;
  });

  FlushAlreadySorted(writer);
}

field_id BufferedColumn::Flush(ColumnstoreWriter& writer,
                               ColumnFinalizer finalizer, DocMapView docmap,
                               BufferedValues& buffer) {
  SDB_ASSERT(docmap.size() < doc_limits::kMaxCount);

  Prepare(doc_limits::kEOF);  // Insert last pending value

  if (_index.empty()) [[unlikely]] {
    return field_limits::invalid();
  }

  if (_hnsw_index) {
    finalizer = ColumnFinalizer{
      [payload_finalizer = finalizer.ExtractPayloadFinalizer(),
       hnsw = std::move(_hnsw_index)](DataOutput& out) mutable {
        SDB_ASSERT(hnsw);
        hnsw->Serialize(out);
        payload_finalizer(out);
      },
      finalizer.ExtractNameFinalizer(),
    };
  }

  auto [column_id, column_writer] =
    writer.push_column(_info, std::move(finalizer));

  if (docmap.empty()) {
    FlushAlreadySorted(column_writer);
  } else if (!FlushDense(column_writer, docmap, buffer)) {
    FlushSparse(column_writer, docmap);
  }

  return column_id;
}

ResettableDocIterator::ptr BufferedColumn::Iterator() const {
  return memory::make_managed<BufferedColumnIterator>(Index(), Data());
}

}  // namespace irs
