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
#include <duckdb/common/file_system.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/file_output.hpp"
#include "iresearch/store/index_files.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct IvfCentroidEntry {
  field_id id;
  IvfCentroidMeta meta;
};

struct HnswEntry {
  field_id id;
  HnswMeta meta;
};

class IdxWriter final {
 public:
  IdxWriter(duckdb::FileSystem& fs, std::string_view root,
            const FileObservers& observers, std::string_view segment_name);

  IdxWriter(const IdxWriter&) = delete;
  IdxWriter& operator=(const IdxWriter&) = delete;

  DataOutput& BlocksOut();

  void AddIvf(field_id id, IvfCentroidMeta meta);

  void AddHnsw(field_id id, HnswMeta meta);

  void SetTerms(term_dict::TermsMeta terms) noexcept;

  void Commit();

 private:
  void EnsureOut();
  bool Empty() const noexcept;

  duckdb::FileSystem& _fs;
  std::string _root;
  FileObservers _observers;
  std::string _filename;
  std::unique_ptr<FileOutput> _out;
  std::vector<IvfCentroidEntry> _ivf_entries;
  std::vector<HnswEntry> _hnsw_entries;
  term_dict::TermsMeta _terms;
};

}  // namespace irs
