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
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/query_context.hpp>
#include <string>

namespace irs::file_utils {

inline std::string Slurp(duckdb::QueryContext context, duckdb::FileSystem& fs,
                         const std::string& path) {
  const auto handle = fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
  std::string bytes(duckdb::NumericCast<size_t>(handle->GetFileSize()), '\0');
  if (!bytes.empty()) {
    handle->Read(context, bytes.data(), bytes.size(), 0);
  }
  return bytes;
}

}  // namespace irs::file_utils
