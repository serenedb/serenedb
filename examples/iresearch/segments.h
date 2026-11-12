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

#pragma once

#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <span>
#include <vector>

namespace examples {

class Segments {
 public:
  Segments(irs::Directory& dir, const irs::IndexReaderOptions& options)
    : _dir{dir}, _options{options} {}

  void Add(std::span<const irs::IndexWriter::FlushedSegment> flushed);
  void Remove(const irs::DocRemoval& removal);
  bool Replace(const irs::IndexWriter::Merged& merged);
  irs::DirectoryReader Reader() const;

 private:
  void Pin();

  irs::Directory& _dir;
  irs::IndexReaderOptions _options;
  std::vector<irs::SegmentMeta> _segments;
  irs::FileRefs _pins;
};

}  // namespace examples
