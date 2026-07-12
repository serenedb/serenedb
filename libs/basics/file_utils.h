////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <stddef.h>

#include <functional>
#include <string>
#include <vector>

#include "basics/common.h"
#include "basics/operating-system.h"

namespace sdb::basics::file_utils {

// removes trailing path separators from path, path will be modified in-place
std::string_view RemoveTrailingSeparator(std::string_view name);

// normalizes path, path will be modified in-place
void NormalizePath(std::string& name);

// creates a filename
std::string BuildFilename(std::string_view path, std::string_view name);

template<typename... Args>
inline std::string BuildFilename(std::string_view path, std::string_view name,
                                 Args... args) {
  return BuildFilename(BuildFilename(path, name), args...);
}

// reads file into string or buffer
void Slurp(const char* filename, std::string& result);
std::string Slurp(const char* filename);
inline std::string Slurp(const std::string& filename) {
  return Slurp(filename.c_str());
}

// creates file and writes string to it
void Spit(const char* filename, std::string_view content, bool sync = false);
inline void Spit(const std::string& filename, std::string_view content,
                 bool sync = false) {
  return Spit(filename.c_str(), content, sync);
}

// creates a new directory

// checks if path is a directory
bool IsDirectory(const char* path);
inline bool IsDirectory(const std::string& path) {
  return IsDirectory(path.c_str());
}

}  // namespace sdb::basics::file_utils
