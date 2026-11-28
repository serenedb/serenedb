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

#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/write_stream.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class IdxWriter final {
 public:
  explicit IdxWriter(FileTracker& tracker);

  IdxWriter(const IdxWriter&) = delete;
  IdxWriter& operator=(const IdxWriter&) = delete;

  NormColumnsWriter& Norms(duckdb::Allocator& allocator);

  SealedNorms SealNorms(uint64_t rows);

  duckdb::WriteStream& BlocksOut();

  void AddIvf(field_id id, IvfCentroidMeta meta);

  void AddHnsw(field_id id, HnswMeta meta);

  void SetTerms(term_dict::TermsMeta terms) noexcept;

  std::unique_ptr<IdxReader> Commit(HnswBases* hnsw_bases);

 private:
  void EnsureOut();
  duckdb::WriteStream& NormsOut();
  duckdb::shared_ptr<duckdb::InputFile> ReadNorms();
  bool Empty() const noexcept;

  FileTracker& _tracker;
  std::string _filename;
  std::unique_ptr<duckdb::BufferedFileWriter> _out;
  IdxFooter _footer;
  duckdb::Allocator* _allocator = nullptr;
  std::optional<duckdb::MemoryStream> _fallback_norms;
  std::optional<NormColumnsWriter> _norms;
};

}  // namespace irs
