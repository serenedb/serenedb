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

#include "segments.h"

#include <absl/algorithm/container.h>

#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/storage.hpp>
#include <memory>
#include <utility>

namespace examples {

void Segments::Add(std::span<const irs::IndexWriter::FlushedSegment> flushed) {
  for (const auto& segment : flushed) {
    _segments.push_back(segment.reader);
  }
}

void Segments::Remove(const irs::DocRemoval& removal) {
  for (const auto& group : removal.groups) {
    const auto name = irs::FileName(group.segment);
    const auto it = absl::c_find_if(_segments, [&](const auto& reader) {
      return reader->Meta().name == name;
    });
    if (it == _segments.end()) {
      continue;
    }
    irs::DocumentMaskBuilder docs{group.docs};
    if (const auto* removed = (*it)->docs_mask()) {
      docs.Merge(*removed);
    }
    *it = (*it)->WithDocsMask(
      std::make_shared<const irs::DocumentMask>(std::move(docs).Finish()));
  }
}

bool Segments::Replace(const irs::IndexWriter::Merged& merged) {
  if (!merged.reader) {
    return false;
  }
  for (const auto* candidate : merged.claim.candidates) {
    std::erase_if(_segments, [&](const auto& reader) {
      return reader->Meta().name == candidate->Meta().name;
    });
  }
  _segments.push_back(merged.reader);
  if (_released) {
    irs::RemoveReleased(_fs, _root, *_released);
  }
  return true;
}

irs::CompositeReader Segments::Reader() const {
  std::vector<irs::SegmentReader> readers;
  readers.reserve(_segments.size());
  for (const auto& segment : _segments) {
    readers.emplace_back(segment);
  }
  return {std::move(readers), true};
}

}  // namespace examples
