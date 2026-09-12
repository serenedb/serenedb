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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "index_reader.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

// Common implementation for readers composied of multiple other readers
// for use/inclusion into cpp files
template<typename Readers>
class CompositeReaderImpl : public IndexReader {
 public:
  using ReadersType = Readers;

  using ReaderType = typename std::enable_if_t<
    std::is_base_of_v<IndexReader, typename Readers::value_type>,
    typename Readers::value_type>;

  CompositeReaderImpl(ReadersType&& readers, uint64_t live_docs_count,
                      uint64_t docs_count) noexcept
    : _readers{std::move(readers)},
      _live_docs_count{live_docs_count},
      _docs_count{docs_count} {}

  // Returns corresponding sub-reader
  const ReaderType& operator[](size_t i) const noexcept final {
    SDB_ASSERT(i < _readers.size());
    return *(_readers[i]);
  }

  std::span<const ReaderType> GetReaders() const noexcept { return _readers; }
  std::span<ReaderType> GetMutReaders() noexcept { return _readers; }

  uint64_t CountMappedMemory() const final {
    uint64_t bytes = 0;
    for (const auto& segment : _readers) {
      bytes += segment.CountMappedMemory();
    }
    return bytes;
  }

  // maximum number of documents
  uint64_t docs_count() const noexcept final { return _docs_count; }

  // number of live documents
  uint64_t live_docs_count() const noexcept final { return _live_docs_count; }

  // returns total number of opened writers
  size_t size() const noexcept final { return _readers.size(); }

 private:
  ReadersType _readers;
  uint64_t _live_docs_count;
  uint64_t _docs_count;
};

}  // namespace irs
