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

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

class ColReader;
class NormColumnReader;

class SegmentReaderImpl final : public SubReader {
  struct PrivateTag final {
    explicit PrivateTag() = default;
  };

 public:
  SegmentReaderImpl(PrivateTag, const SegmentMeta& meta) noexcept
    : _info{meta}, _docs_mask{meta.docs_mask} {
    SDB_ASSERT(meta.live_docs_count <= meta.docs_count);
    SDB_ASSERT(RemovalCount(meta) <= meta.docs_count);
    _info.live_docs_count = meta.docs_count - RemovalCount(meta);
  }

  static std::shared_ptr<const SegmentReaderImpl> Open(
    const Directory& dir, const SegmentMeta& meta,
    const IndexReaderOptions& options);

  std::shared_ptr<const SegmentReaderImpl> ReopenReader(
    const Directory& dir, const SegmentMeta& meta,
    const IndexReaderOptions& options) const;
  std::shared_ptr<const SegmentReaderImpl> UpdateMeta(
    const Directory& dir, const SegmentMeta& meta) const;

  uint64_t CountMappedMemory() const final;

  const SegmentInfo& Meta() const final { return _info; }

  const DocumentMask* docs_mask() const final { return _docs_mask.get(); }

  DocIterator::ptr docs_iterator() const final;

  DocIterator::ptr mask(DocIterator::ptr&& it) const final;

  const TermReader* field(field_id id) const final {
    return _field_reader->field(id);
  }

  std::span<const field_id> field_ids() const final {
    return _field_reader->field_ids();
  }

  NormReader::ptr norms(field_id field) const final;

  const ColumnReader* Column(field_id field) const final;
  const CentroidsTree* Ivf(field_id field) const final;
  IndexInput::ptr ReopenIvf() const final;
  const ColReader* GetColReader() const final {
    return _data ? _data->col_reader.get() : nullptr;
  }

 private:
  struct ColumnData {
    std::unique_ptr<ColReader> col_reader;
    std::unique_ptr<IdxReader> idx_reader;

    void Open(const Directory& dir, const SegmentMeta& meta,
              const IndexReaderOptions& options);
  };

  FileRefs _refs;
  SegmentInfo _info;
  std::shared_ptr<const DocumentMask> _docs_mask;
  std::shared_ptr<ColumnData> _data;
  std::shared_ptr<burst_trie::FieldReader> _field_reader;
};

}  // namespace irs
