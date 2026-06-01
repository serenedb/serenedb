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

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

#include "basics/common.h"
#include "basics/debugging.h"
#include "basics/result.h"

struct stat;

namespace sdb {

inline constexpr size_t kReadBufferSize = 8192;

// returns the size of a file. Negative on error.
int64_t SdbSizeFile(const char* path);

bool SdbIsRegularFile(const char* path);

bool SdbIsSymbolicLink(const char* path);

bool SdbExistsFile(const char* path);

// creates a directory, recursively
ErrorCode SdbCreateRecursiveDirectory(std::string_view path, long& system_error,
                                      std::string& system_error_str);

ErrorCode SdbCreateDirectory(const char* path, long& system_error,
                             std::string& system_error_str);

std::string_view SdbDirname(std::string_view path);

std::vector<std::string> SdbFilesDirectory(const char* path);

std::vector<std::string> SdbFullTreeDirectory(const char* path);

ErrorCode SdbRenameFile(const char* old, const char* filename,
                        long* system_error = nullptr,
                        std::string* system_error_str = nullptr);

ErrorCode SdbUnlinkFile(const char* filename);

bool Sdbfsync(int fd);

bool SdbSlurpFile(const char* filename, std::string& result);

// slurps in a file that is compressed and return uncompressed contents
bool SdbSlurpGzipFile(const char* filename, std::string& result);

////////////////////////////////////////////////////////////////////////////////
/// creates a lock file based on the PID
///
/// Creates a file containing a the current process identifier and locks
/// that file. Under Unix the call uses the @FN{open} system call with
/// O_EXCL to ensure that the file is created atomically. Then the
/// file is filled with the process identifier as decimal number and a
/// lock on the file is obtained using @FN{flock}.
///
/// On success @ref ERROR_OK is returned.
///
/// Internally, the functions keeps a list of open pid files. Calling the
/// function twice with the same @FA{filename} will succeed and will not
/// create a new entry in this list. The system uses @FN{atexit} to release
/// all open locks upon exit.
////////////////////////////////////////////////////////////////////////////////

ErrorCode SdbCreateLockFile(const char* filename);

////////////////////////////////////////////////////////////////////////////////
/// verifies a lock file based on the PID
///
/// The function checks if the file named @FA{filename} exists. If the
/// file exists, then the following checks are performed:
///
/// - Does the file contain a valid decimal number?
/// - Does this number belong to a living process?
/// - Is it possible to lock the file using @FN{flock}. This should failed.
///   If the lock can be obtained, then it is assume that the lock is invalid.
///
/// If the verification returns an error, than @FN{SdbUnlinkFile} should be
/// used to remove the lock file. If the verification returns @ref
/// ERROR_OK than the file is locked and the lock is valid.
////////////////////////////////////////////////////////////////////////////////

ErrorCode SdbVerifyLockFile(const char* filename);

ErrorCode SdbDestroyLockFile(const char* filename);

std::string SdbGetAbsolutePath(std::string_view file_name,
                               std::string_view current_working_directory);

std::string_view SdbHomeDirectory();

ErrorCode SdbCrc32File(const char* path, uint32_t* crc);

// this API allows passing already retrieved stat info to the copy routine, in
// order to avoid extra stat calls
bool SdbCopyFile(const char* src, const char* dst, std::string& error,
                 struct stat* statbuf = nullptr);
inline bool SdbCopyFile(const std::string& src, const std::string& dst,
                        std::string& error, struct stat* statbuf = nullptr) {
  return SdbCopyFile(src.c_str(), dst.c_str(), error, statbuf);
}

bool SdbCopyAttributes(const char* src_item, const char* dst_item,
                       std::string& error);

bool SdbCopySymlink(const char* src_item, const char* dst_item,
                    std::string& error);

bool SdbCreateHardlink(const char* existing_file, const char* new_file,
                       std::string& error);

std::string SdbLocateConfigDirectory(const char* binary_path);

/// return the amount of total and free disk space for the given path
sdb::Result SdbGetDiskSpaceInfo(const char* path, uint64_t& total_space,
                                uint64_t& free_space);

/// return the amount of total and free inodes for the given path.
sdb::Result SdbGetINodesInfo(const char* path, uint64_t& total_i_nodes,
                             uint64_t& free_i_nodes);

// reads an environment variable. returns false if env var was not set.
// if env var was set, returns env variable value in "value" and returns true.
bool SdbGETENV(const char* which, std::string& value);

struct Sha256Functor {
  Sha256Functor();
  ~Sha256Functor();

  bool operator()(const char* data, size_t size) noexcept;

  std::string finalize();

 private:
  void* _context;
};

}  // namespace sdb
