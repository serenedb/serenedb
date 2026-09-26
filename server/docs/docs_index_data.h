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
#include <span>
#include <string>
#include <string_view>

namespace sdb::docs {

inline constexpr std::string_view kLayoutFile = "sdb_docs.layout";
inline constexpr std::string_view kObjectsFile = "sdb_docs.objects";

struct IndexFile {
  std::string_view name;
  std::span<const std::uint8_t> bytes;
};

struct IndexBlob {
  std::string name;
  std::string bytes;
};

std::span<const IndexFile> GetDocsIndex();

}  // namespace sdb::docs
