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

#include <duckdb/common/file_system.hpp>
#include <iresearch/index/composite_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/storage.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace examples {

class Segments {
 public:
  Segments(duckdb::FileSystem& fs, std::string_view root,
           std::shared_ptr<irs::ReleasedSegments> released = nullptr)
    : _fs{fs}, _root{root}, _released{std::move(released)} {}

  void Add(std::span<const irs::IndexWriter::FlushedSegment> flushed);
  void Remove(const irs::DocRemoval& removal);
  bool Replace(const irs::IndexWriter::Merged& merged);
  irs::CompositeReader Reader() const;

 private:
  duckdb::FileSystem& _fs;
  std::string _root;
  std::shared_ptr<irs::ReleasedSegments> _released;
  std::vector<std::shared_ptr<const irs::SegmentReaderImpl>> _segments;
};

}  // namespace examples
