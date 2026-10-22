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

#include "iresearch/analysis/text/dict/stopwords_loader.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_split.h>

#include <cctype>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/query_context.hpp>
#include <string>
#include <vector>

#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/file_error.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis::dict {
namespace {

void LoadStopwordsFile(duckdb::FileSystem& fs, const std::string& file,
                       StringSet<std::string>& buf) {
  auto handle = fs.OpenFile(file, duckdb::FileFlags::FILE_FLAGS_READ);
  std::string text(duckdb::NumericCast<size_t>(handle->GetFileSize()), '\0');
  handle->Read(duckdb::QueryContext{}, text.data(), text.size(), 0);
  for (const std::string_view line : absl::StrSplit(text, '\n')) {
    const auto end = absl::c_find_if(line, [](char c) {
      return std::isspace(static_cast<unsigned char>(c)) != 0;
    });
    if (end != line.begin()) {
      buf.Insert(std::string_view{line.begin(), end});
    }
  }
}

}  // namespace

void LoadStopwords(StringSet<std::string>& buf, std::string_view path) try {
  const std::string root{path};
  auto fs = duckdb::FileSystem::CreateLocal();
  if (fs->FileExists(root)) {
    LoadStopwordsFile(*fs, root, buf);
    return;
  }
  if (!fs->DirectoryExists(root)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CONFIG_FILE_ERROR),
      ERR_MSG("stop-word file or directory \"", root, "\" does not exist"));
  }
  std::vector<std::string> files;
  fs->ListFiles(root, [&](const std::string& name, bool is_directory) {
    if (!is_directory) {
      files.push_back(fs->JoinPath(root, name));
    }
  });
  for (const auto& file : files) {
    LoadStopwordsFile(*fs, file, buf);
  }
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

}  // namespace irs::analysis::dict
