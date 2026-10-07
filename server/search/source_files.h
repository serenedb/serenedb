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
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class FileSystem;

}  // namespace duckdb
namespace sdb::search {

struct SourceFile {
  uint64_t id = 0;
  std::string path;
  std::string version;
};

class SourceFiles {
 public:
  SourceFiles() = default;
  explicit SourceFiles(std::vector<SourceFile> files);

  const SourceFile* Find(uint64_t id) const noexcept;
  uint64_t NextId() const noexcept;
  size_t Size() const noexcept { return _files.size(); }
  std::span<const SourceFile> Files() const noexcept { return _files; }

 private:
  std::vector<SourceFile> _files;
};

using SourceFilesPtr = std::shared_ptr<const SourceFiles>;

struct SourceFilesUpdate {
  std::vector<SourceFile> added;
  std::optional<std::vector<uint64_t>> live;
};

inline constexpr std::string_view kSourceFilesName = "source_files";
inline constexpr std::string_view kSourceFilesTmpName = "source_files.tmp";

struct StoredSourceFiles {
  std::vector<SourceFile> files;
  bool torn = false;
};

StoredSourceFiles ReadSourceFiles(duckdb::FileSystem& fs,
                                  const std::string& path);
void AppendSourceFiles(duckdb::FileSystem& fs, const std::string& path,
                       std::span<const SourceFile> files);
void WriteSourceFiles(duckdb::FileSystem& fs, const std::string& tmp_path,
                      const std::string& path,
                      std::span<const SourceFile> files);

}  // namespace sdb::search
