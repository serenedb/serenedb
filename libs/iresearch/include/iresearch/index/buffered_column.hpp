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

#pragma once

#include <vector>

#include "iresearch/formats/column/hnsw_index.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

class Comparer;

struct BufferedValue {
  constexpr BufferedValue() = default;
  constexpr BufferedValue(doc_id_t key, size_t begin, size_t size) noexcept
    : key{key}, begin{begin}, size{size} {}

  doc_id_t key{};
  size_t begin{};
  size_t size{};
};

class BufferedColumn final : public ColumnOutput, private util::Noncopyable {
 public:
  using BufferedValues = ManagedVector<BufferedValue>;
  // FIXME use memory_file or block_pool instead
  using Buffer = basic_string<byte_type, ManagedTypedAllocator<byte_type>>;

  explicit BufferedColumn(const ColumnInfo& info, IResourceManager& rm);

  void Prepare(doc_id_t key) final {
    SDB_ASSERT(key >= _pending_key);

    if (_pending_key != key) [[likely]] {
      const auto offset = _data_buf.size();

      if (doc_limits::valid(_pending_key)) [[likely]] {
        SDB_ASSERT(offset >= _pending_offset);
        _index.emplace_back(_pending_key, _pending_offset,
                            offset - _pending_offset);
        if (_hnsw_index) {
          _hnsw_index->Add(
            reinterpret_cast<const float*>(_data_buf.data() + _pending_offset),
            _pending_key);
        }
      }

      _pending_key = key;
      _pending_offset = offset;
    }
  }

  void WriteByte(byte_type b) final { _data_buf += b; }

  void WriteBytes(const byte_type* b, size_t size) final {
    _data_buf.append(b, size);
  }

  void Reset() final {
    if (!doc_limits::valid(_pending_key)) {
      return;
    }
    SDB_ASSERT(_pending_offset <= _data_buf.size());
    _data_buf.erase(_pending_offset);
    _pending_key = doc_limits::invalid();
  }

  bool Empty() const noexcept { return _index.empty(); }

  size_t Size() const noexcept { return _index.size(); }

  void Clear() noexcept {
    _data_buf.clear();
    _index.clear();
    _pending_key = doc_limits::invalid();
  }

  // 1st - doc map (old->new), empty -> already sorted
  // 2nd - flushed column identifier
  std::pair<DocMap, field_id> Flush(
    ColumnstoreWriter& writer, ColumnFinalizer header_writer,
    doc_id_t docs_count,  // total number of docs in segment
    const Comparer& compare);

  field_id Flush(ColumnstoreWriter& writer, ColumnFinalizer header_writer,
                 DocMapView docmap, BufferedValues& buffer);

  size_t MemoryActive() const noexcept {
    return _data_buf.size() +
           _index.size() * sizeof(decltype(_index)::value_type);
  }

  size_t MemoryReserved() const noexcept {
    return _data_buf.capacity() +
           _index.capacity() * sizeof(decltype(_index)::value_type);
  }

  const ColumnInfo& Info() const noexcept { return _info; }

  std::span<const BufferedValue> Index() const noexcept { return _index; }

  std::unique_ptr<HNSWIndexWriter>& HNSWIndex() noexcept { return _hnsw_index; }

  bytes_view Data() const noexcept { return _data_buf; }

  ResettableDocIterator::ptr Iterator() const;

 private:
  friend class BufferedColumnIterator;

  bytes_view GetPayload(const BufferedValue& value) noexcept {
    return {_data_buf.data() + value.begin, value.size};
  }

  void WriteValue(DataOutput& out, const BufferedValue& value) {
    const auto payload = GetPayload(value);
    out.WriteBytes(payload.data(), payload.size());
  }

  bool FlushSparsePrimary(DocMap& docmap, ColumnOutput& writer,
                          doc_id_t docs_count, const Comparer& compare);

  void FlushAlreadySorted(ColumnOutput& writer);

  bool FlushDense(ColumnOutput& writer, DocMapView docmap,
                  BufferedValues& buffer);

  void FlushSparse(ColumnOutput& writer, DocMapView docmap);

  Buffer _data_buf;
  BufferedValues _index;
  size_t _pending_offset{};
  doc_id_t _pending_key{doc_limits::invalid()};
  ColumnInfo _info;
  std::unique_ptr<HNSWIndexWriter> _hnsw_index;
};

}  // namespace irs
