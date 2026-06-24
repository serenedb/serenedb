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

#include "segment_reader.hpp"

#include "iresearch/index/segment_reader_impl.hpp"

namespace irs {

uint64_t SegmentReader::CountMappedMemory() const {
  return _impl->CountMappedMemory();
}

std::span<const field_id> SegmentReader::field_ids() const {
  return _impl->field_ids();
}

NormReader::ptr SegmentReader::norms(field_id field) const {
  return _impl->norms(field);
}

const ColumnReader* SegmentReader::Column(field_id field) const {
  return _impl->Column(field);
}

const FlatCentroids* SegmentReader::Ivf(field_id field) const {
  return _impl->Ivf(field);
}

const ColReader* SegmentReader::GetColReader() const {
  return _impl ? _impl->GetColReader() : nullptr;
}

// FIXME find a better way to mask documents
DocIterator::ptr SegmentReader::mask(DocIterator::ptr&& it) const {
  return _impl->mask(std::move(it));
}

const TermReader* SegmentReader::field(field_id id) const {
  return _impl->field(id);
}

DocIterator::ptr SegmentReader::docs_iterator() const {
  return _impl->docs_iterator();
}

const SegmentInfo& SegmentReader::Meta() const { return _impl->Meta(); }

const DocumentMask* SegmentReader::docs_mask() const {
  return _impl->docs_mask();
}

}  // namespace irs
