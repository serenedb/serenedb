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

#include <atomic>
#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <span>
#include <vector>

namespace sdb::connector {

// TODO(Dronplane): a DELETE that deduplicates records every position twice:
// here and in each sink thread's DocRemovalBuilder. This bitset already holds
// the whole deduplicated set, so the DELETE could drop the per-thread builders
// and build its one removal from the bitset in Finalize, a single pass over the
// touched pages instead of a Finish per thread under the operator's mutex.
// Profile the joined DELETE first: worth it only if AddRowPositions and Finish
// show.
class RowPositionDeduplicator {
 public:
  explicit RowPositionDeduplicator(std::span<const irs::doc_id_t> segment_docs);
  explicit RowPositionDeduplicator(const irs::DirectoryReader& reader);
  ~RowPositionDeduplicator();

  RowPositionDeduplicator(const RowPositionDeduplicator&) = delete;
  RowPositionDeduplicator& operator=(const RowPositionDeduplicator&) = delete;

  duckdb::idx_t KeepFirst(duckdb::DataChunk& chunk, duckdb::idx_t rowid);

#ifdef SDB_DEV
  bool Covers(const irs::DirectoryReader& reader) const noexcept {
    return &reader == _reader;
  }
#endif

 private:
  using Word = std::atomic<uint64_t>;

  struct Segment {
    irs::doc_id_t docs = 0;
    std::unique_ptr<std::atomic<Word*>[]> pages;
  };

  Word& WordOf(uint32_t index, irs::doc_id_t doc);

  std::vector<Segment> _segments;
#ifdef SDB_DEV
  const irs::DirectoryReader* _reader = nullptr;
#endif
};

}  // namespace sdb::connector
