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
#include "basics/result.h"

namespace sdb::basics::file_utils {

// removes trailing path separators from path, path will be modified in-place
std::string_view RemoveTrailingSeparator(std::string_view name);

// normalizes path, path will be modified in-place
void NormalizePath(std::string& name);

// creates a filename
std::string BuildFilename(const char* path, const char* name);

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

// if a file could be removed returns ERROR_OK.
// otherwise, returns ERROR_SYS_ERROR and sets LastError.
[[nodiscard]] ErrorCode Remove(const char* file_name);
[[nodiscard]] inline ErrorCode Remove(const std::string& file_name) {
  return Remove(file_name.c_str());
}

// creates a new directory
bool CreateDirectory(const char* name, ErrorCode* error_number = nullptr);
bool CreateDirectory(const char* name, int mask,
                     ErrorCode* error_number = nullptr);

// returns list of files / subdirectories / links in a directory.
// does not recurse into subdirectories. will throw an exception in
// case the directory cannot be opened for iteration.
std::vector<std::string> ListFiles(const char* directory);
inline std::vector<std::string> ListFiles(const std::string& directory) {
  return ListFiles(directory.c_str());
}

// returns the number of files / subdirectories / links in a directory.
// does not recurse into subdirectories. will throw an exception in
// case the directory cannot be opened for iteration.
size_t CountFiles(const char* directory);

// checks if path is a directory
bool IsDirectory(const char* path);
inline bool IsDirectory(const std::string& path) {
  return IsDirectory(path.c_str());
}

// checks if path exists
bool Exists(const char* path);
inline bool Exists(const std::string& path) { return Exists(path.c_str()); }

// strip extension
std::string_view StripExtension(std::string_view path,
                                std::string_view extension);


// returns the home directory
std::string HomeDirectory();

// returns the config directory
std::string ConfigDirectory(const char* binary_path);

// returns the dir name of a path
std::string_view Dirname(std::string_view);

}  // namespace sdb::basics::file_utils
