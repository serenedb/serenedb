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
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/types.hpp>
#include <memory>

namespace sdb::connector {

class RowPositionDeduplicator {
 public:
  explicit RowPositionDeduplicator(const irs::IndexReader& view);
  ~RowPositionDeduplicator();

  RowPositionDeduplicator(const RowPositionDeduplicator&) = delete;
  RowPositionDeduplicator& operator=(const RowPositionDeduplicator&) = delete;

  duckdb::idx_t KeepFirst(duckdb::DataChunk& chunk, duckdb::idx_t rowid);

  irs::DocRemovalBuilder Positions() &&;

 private:
  using Bitset = irs::DocumentMaskBuilder::Bitset;

  uint64_t* WordsOf(uint32_t unit, irs::doc_id_t doc);

  uint32_t _units;
  std::unique_ptr<uint32_t[]> _first_page;
  std::unique_ptr<std::atomic<Bitset*>[]> _pages;
};

}  // namespace sdb::connector
