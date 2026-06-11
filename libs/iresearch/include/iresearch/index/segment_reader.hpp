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

#pragma once

#include "index_reader.hpp"

namespace irs {

class SegmentReaderImpl;

// Pimpl facade for segment reader
class SegmentReader final : public SubReader {
 public:
  SegmentReader() noexcept = default;
  SegmentReader(SegmentReader&&) noexcept = default;
  SegmentReader& operator=(SegmentReader&&) noexcept = default;
  SegmentReader(const SegmentReader& other) noexcept = default;
  SegmentReader& operator=(const SegmentReader& other) noexcept = default;

  explicit SegmentReader(std::shared_ptr<const SegmentReaderImpl> impl) noexcept
    : _impl{std::move(impl)} {}

  bool operator==(const SegmentReader& rhs) const noexcept {
    return _impl == rhs._impl;
  }

  bool operator==(std::nullptr_t) const noexcept { return !_impl; }

  SegmentReader& operator*() noexcept { return *this; }
  const SegmentReader& operator*() const noexcept { return *this; }
  SegmentReader* operator->() noexcept { return this; }
  const SegmentReader* operator->() const noexcept { return this; }

  uint64_t CountMappedMemory() const final;

  const SegmentInfo& Meta() const final;

  DocIterator::ptr docs_iterator() const final;

  const DocumentMask* docs_mask() const final;

  // FIXME find a better way to mask documents
  DocIterator::ptr mask(DocIterator::ptr&& it) const final;

  const TermReader* field(field_id id) const final;
  std::span<const field_id> field_ids() const final;

  NormReader::ptr norms(field_id field) const final;

  const ColumnReader* Column(field_id field) const final;
  const HnswReader* HNSW(field_id field) const final;
  const ColReader* GetColReader() const final;

  const std::shared_ptr<const SegmentReaderImpl>& GetImpl() const noexcept {
    return _impl;
  }

  explicit operator bool() const noexcept { return nullptr != _impl; }

 private:
  std::shared_ptr<const SegmentReaderImpl> _impl;
};

}  // namespace irs
