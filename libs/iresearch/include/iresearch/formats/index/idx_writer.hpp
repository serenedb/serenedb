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

#include <faiss/impl/HNSW.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "iresearch/formats/index/idx_reader.hpp"  // TermDictMeta, kIdxFormat*
#include "iresearch/index/column_info.hpp"         // HNSWInfo
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class Directory;
class IndexOutput;

class IdxWriter final {
 public:
  IdxWriter(Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db);
  ~IdxWriter();

  IdxWriter(const IdxWriter&) = delete;
  IdxWriter& operator=(const IdxWriter&) = delete;

  IndexOutput& BlocksOut();

  void AddHNSW(field_id id, const HNSWInfo& info,
               std::shared_ptr<const faiss::HNSW> graph);

  void AddTermDictEntry(field_id id, TermDictMeta meta);

  void Commit();
  void Rollback() noexcept;

 private:
  void EnsureOut();
  bool Empty() const noexcept;

  struct Impl;
  std::unique_ptr<Impl> _impl;
};

}  // namespace irs
