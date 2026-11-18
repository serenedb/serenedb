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
#include <iresearch/store/directory_cleaner.hpp>
#include <memory>
#include <utility>

namespace examples {

void Segments::Add(std::span<const irs::IndexWriter::FlushedSegment> flushed) {
  for (const auto& segment : flushed) {
    _segments.push_back(segment.meta);
  }
  Pin();
}

void Segments::Remove(const irs::DocRemoval& removal) {
  for (const auto& group : removal.groups) {
    const auto name = irs::FileName(group.segment);
    const auto it = absl::c_find_if(
      _segments,
      [&](const irs::SegmentMeta& meta) { return meta.name == name; });
    if (it == _segments.end()) {
      continue;
    }
    irs::DocumentMaskBuilder docs{group.docs};
    if (it->docs_mask) {
      docs.Merge(*it->docs_mask);
    }
    it->docs_mask =
      std::make_shared<const irs::DocumentMask>(std::move(docs).Finish());
    it->live_docs_count = it->docs_count - irs::RemovalCount(*it);
  }
}

bool Segments::Replace(const irs::IndexWriter::Merged& merged) {
  if (!merged.reader) {
    return false;
  }
  for (const auto* candidate : merged.claim.candidates) {
    std::erase_if(_segments, [&](const irs::SegmentMeta& meta) {
      return meta.name == candidate->Meta().name;
    });
  }
  _segments.push_back(merged.meta);
  Pin();
  irs::DirectoryCleaner::clean(_dir);
  return true;
}

irs::DirectoryReader Segments::Reader() const {
  std::vector<irs::SegmentReader> readers;
  readers.reserve(_segments.size());
  for (const auto& meta : _segments) {
    readers.emplace_back(irs::SegmentReaderImpl::Open(_dir, meta, _options));
  }
  return {_dir, _options, std::move(readers), /*stats=*/true};
}

void Segments::Pin() {
  irs::FileRefs pins;
  auto& refs = _dir.attributes().refs();
  for (const auto& meta : _segments) {
    for (const auto& file : irs::SegmentFiles(meta.name, meta.kinds)) {
      pins.emplace_back(refs.add(file));
    }
  }
  _pins = std::move(pins);
}

}  // namespace examples
