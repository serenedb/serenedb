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
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "absl/strings/str_cat.h"
#include "basics/file_utils_ext.hpp"
#include "basics/log.h"

namespace irs::analysis::dict {

namespace {

bool LoadStopwordsFile(const std::filesystem::path& file,
                       StringSet<std::string>& buf) {
  std::ifstream in(file.native());
  if (!in) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Failed to load stopwords from path: ",
                                      file.string()));
    return false;
  }
  for (std::string line; std::getline(in, line);) {
    size_t i = 0;
    for (size_t length = line.size();
         i < length && !std::isspace(static_cast<unsigned char>(line[i]));
         ++i) {
    }
    if (i > 0) {
      buf.Insert(std::string{line.data(), i});
    }
  }
  return true;
}

}  // namespace

bool LoadStopwords(StringSet<std::string>& buf, std::string_view language,
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

  try {
    bool direct_file = false;
    if (file_utils::ExistsFile(direct_file, stopword_path.c_str()) &&
        direct_file) {
      return LoadStopwordsFile(stopword_path, buf);
    }

    stopword_path /= language;

    bool exists = false;
    if (!file_utils::ExistsDirectory(exists, stopword_path.c_str()) ||
        !exists) {
      if (custom_stopword_path) {
        SDB_ERROR(IRESEARCH,
                  absl::StrCat("Failed to load stopwords from path: ",
                               stopword_path.string()));
        return false;
      }
      SDB_TRACE(IRESEARCH,
                absl::StrCat("Failed to load stopwords from default path: ",
                             stopword_path.string(),
                             ". Tokenizer will continue without stopwords"));
      return true;
    }

    const auto visitor = [&](auto name) -> bool {
      const auto file = stopword_path / name;
      bool is_file = false;
      if (!file_utils::ExistsFile(is_file, file.c_str())) {
        SDB_ERROR(IRESEARCH, absl::StrCat("Failed to identify stopword path: ",
                                          file.string()));
        return false;
      }
      if (!is_file) {
        return true;
      }
      return LoadStopwordsFile(file, buf);
    };
    return file_utils::VisitDirectory(stopword_path.c_str(), visitor, false);
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("Caught error while loading stopwords from path: ",
                           stopword_path.string()));
  }
  return false;
}

bool ResolveStopwords(StringSet<std::string>& buf, std::string_view language,
                      std::string_view path, bool explicit_set) {
  if (path.empty() || path[0] != 0) {
    return LoadStopwords(buf, language, path);
  }
  if (!explicit_set && buf.Empty()) {
    return LoadStopwords(buf, language);
  }
  return true;
}

}  // namespace irs::analysis::dict
