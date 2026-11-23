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

#include <duckdb/common/file_system.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <string_view>

namespace irs {

class IdxReader;
struct SegmentMeta;

struct PostingsInputs {
  duckdb::shared_ptr<duckdb::InputFile> doc;
  duckdb::shared_ptr<duckdb::InputFile> pos;
  duckdb::shared_ptr<duckdb::InputFile> pay;
};

struct ReaderState {
  duckdb::DatabaseInstance& db;
  duckdb::FileSystem& fs;
  std::string_view root;
  const SegmentMeta& meta;
  IdxReader* idx = nullptr;
  PostingsInputs* postings = nullptr;
};

}  // namespace irs
