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

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>

#include "absl/strings/str_cat.h"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis::dict {
namespace {

[[noreturn]] void ThrowStopwordError(std::string_view action,
                                     const std::filesystem::path& path,
                                     int error) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONFIG_FILE_ERROR),
                  ERR_MSG("could not ", action, " \"", path.string(),
                          "\": ", std::strerror(error)));
}

void LoadStopwordsFile(const std::filesystem::path& file,
                       StringSet<std::string>& buf) {
  std::ifstream in(file.native());
  if (!in) {
    ThrowStopwordError("open stop-word file", file, errno);
  }
  for (std::string line; std::getline(in, line);) {
    size_t i = 0;
    for (size_t length = line.size();
         i < length && !std::isspace(static_cast<unsigned char>(line[i]));
         ++i) {
    }
    if (i > 0) {
      buf.Insert(std::string_view{line.data(), i});
    }
  }
}

}  // namespace

void LoadStopwords(StringSet<std::string>& buf, std::string_view language,
                   std::string_view path) {
  std::filesystem::path stopword_path;
  bool custom_stopword_path = true;
  if (!path.empty()) {
    stopword_path.assign(path.begin(), path.end());
  } else if (const auto* env = std::getenv(kStopwordPathEnvVariable)) {
    stopword_path.assign(env);
  } else {
    custom_stopword_path = false;
  }
  if (custom_stopword_path) {
    file_utils::EnsureAbsolute(stopword_path);
  } else {
    std::filesystem::path::string_type cwd;
    file_utils::ReadCwd(cwd);
    stopword_path = std::move(cwd);
  }

  bool direct_file = false;
  if (!file_utils::ExistsFile(direct_file, stopword_path.c_str())) {
    ThrowStopwordError("access stop-word path", stopword_path, errno);
  }
  if (direct_file) {
    LoadStopwordsFile(stopword_path, buf);
    return;
  }

  stopword_path /= language;

  bool exists = false;
  if (!file_utils::ExistsDirectory(exists, stopword_path.c_str())) {
    ThrowStopwordError("access stop-word directory", stopword_path, errno);
  }
  if (!exists) {
    if (custom_stopword_path) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONFIG_FILE_ERROR),
                      ERR_MSG("stop-word file or directory \"",
                              stopword_path.string(), "\" does not exist"));
    }
    SDB_TRACE(IRESEARCH,
              absl::StrCat("Failed to load stopwords from default path: ",
                           stopword_path.string(),
                           ". Tokenizer will continue without stopwords"));
    return;
  }

  const auto visitor = [&](auto name) -> bool {
    const auto file = stopword_path / name;
    bool is_file = false;
    if (!file_utils::ExistsFile(is_file, file.c_str())) {
      ThrowStopwordError("access stop-word file", file, errno);
    }
    if (is_file) {
      LoadStopwordsFile(file, buf);
    }
    return true;
  };
  if (!file_utils::VisitDirectory(stopword_path.c_str(), visitor, false)) {
    ThrowStopwordError("read stop-word directory", stopword_path, errno);
  }
}

}  // namespace irs::analysis::dict
