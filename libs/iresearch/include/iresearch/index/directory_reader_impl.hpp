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

#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "iresearch/index/composite_reader_impl.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/store/directory_attributes.hpp"

namespace irs {

class DirectoryReaderImpl final
  : public CompositeReaderImpl<std::vector<SegmentReader>> {
 public:
  // open a new directory reader
  // if codec == nullptr then use the latest file for all known codecs
  // if cached != nullptr then try to reuse its segments
  static std::shared_ptr<const DirectoryReaderImpl> Open(
    const Directory& dir, const IndexReaderOptions& opts, Format::ptr codec,
    const std::shared_ptr<const DirectoryReaderImpl>& cached);

  DirectoryReaderImpl(const Directory& dir, Format::ptr codec,
                      const IndexReaderOptions& opts, DirectoryMeta&& meta,
                      ReadersType&& readers);

  const Directory& Dir() const noexcept { return _dir; }

  const DirectoryMeta& Meta() const noexcept { return _meta; }

  const IndexReaderOptions& Options() const noexcept { return _opts; }

  const Format::ptr& Codec() const noexcept { return _codec; }

  const duckdb::BaseStatistics* GetColumnStats(field_id field) const noexcept {
    const auto it = _column_stats.find(field);
    return it == _column_stats.end() ? nullptr : it->second.get();
  }

 private:
  struct Init;

  DirectoryReaderImpl(Init&& init, const Directory& dir, Format::ptr&& codec,
                      const IndexReaderOptions& opts, DirectoryMeta&& meta,
                      ReadersType&& readers);

  const Directory& _dir;
  Format::ptr _codec;
  FileRefs _file_refs;
  DirectoryMeta _meta;
  IndexReaderOptions _opts;
  sdb::containers::FlatHashMap<field_id,
                               duckdb::unique_ptr<duckdb::BaseStatistics>>
    _column_stats;
};

}  // namespace irs
