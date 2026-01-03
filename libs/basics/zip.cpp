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

#include "zip.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unzip.h>
#include <zip.h>

#include <algorithm>
#include <cstring>
#include <new>

#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/operating-system.h"

#ifdef _WIN32
#define USEWIN32IOAPI
#include "Zip/iowin32.h"
#endif

namespace sdb {
namespace {

const char* TranslateError(int err) {
  switch (err) {
    // UNZ_OK and UNZ_EOF have the same numeric value...
    case UNZ_OK:
      return "no error";
    case UNZ_END_OF_LIST_OF_FILE:
      return "end of list of file";
    case UNZ_PARAMERROR:
      return "parameter error";
    case UNZ_BADZIPFILE:
      return "bad zip file";
    case UNZ_INTERNALERROR:
      return "internal error";
    case UNZ_CRCERROR:
      return "crc error";
    default:
      return "unknown error";
  }
}

ErrorCode ExtractCurrentFile(unzFile uf, void* buffer, const size_t buffer_size,
                             const char* out_path, const bool skip_paths,
                             const bool overwrite, const char* password,
                             std::string& error_message) {
  char filename_in_zip[256];
  char* filename_without_path;
  char* p;
  unz_file_info64 file_info;
  long system_error;
  int err;

  FILE* fout;

  filename_in_zip[0] = '\0';
  err = unzGetCurrentFileInfo64(uf, &file_info, filename_in_zip,
                                sizeof(filename_in_zip), NULL, 0, NULL, 0);
  if (err != UNZ_OK) {
    error_message = std::string("Failed to get file info for ") +
                    filename_in_zip + ": " + std::to_string(err);
    return ERROR_INTERNAL;
  }

  // adjust file name
  p = filename_in_zip;
  while (*p != '\0') {
#ifdef _WIN32
    if (*p == '/') {
      *p = '\\';
    }
#else
    if (*p == '\\') {
      *p = '/';
    }
#endif
    ++p;
  }

  p = filename_without_path = filename_in_zip;

  // get the file name without any path prefix
  while (*p != '\0') {
    if (*p == '/' || *p == '\\' || *p == ':') {
      filename_without_path = p + 1;
    }

    p++;
  }

  std::string full_path;

  // found a directory
  if (*filename_without_path == '\0') {
    if (!skip_paths) {
      full_path = basics::file_utils::BuildFilename(out_path, filename_in_zip);
      auto res =
        SdbCreateRecursiveDirectory(full_path, system_error, error_message);

      if (res != ERROR_OK) {
        return res;
      }
    }
  }

  // found a file
  else {
    const char* write_filename;

    if (!skip_paths) {
      write_filename = filename_in_zip;
    } else {
      write_filename = filename_without_path;
    }

    err = unzOpenCurrentFilePassword(uf, password);
    if (err != UNZ_OK) {
      error_message = "failed to authenticate the password in the zip: " +
                      std::to_string(err);
      return ERROR_INTERNAL;
    }

    // prefix the name from the zip file with the path specified
    full_path = basics::file_utils::BuildFilename(out_path, write_filename);

    if (!overwrite && SdbExistsFile(full_path.c_str())) {
      error_message = std::string("not allowed to overwrite file ") + full_path;
      return ERROR_CANNOT_OVERWRITE_FILE;
    }

    // try to write the outfile
    fout = SERENEDB_FOPEN(full_path.c_str(), "wb");

    // cannot write to outfile. this may be due to the target directory missing
    if (fout == nullptr && !skip_paths &&
        filename_without_path != (char*)filename_in_zip) {
      char c = *(filename_without_path - 1);
      *(filename_without_path - 1) = '\0';

      // create target directory recursively
      auto tmp = basics::file_utils::BuildFilename(out_path, filename_in_zip);
      auto res = SdbCreateRecursiveDirectory(tmp, system_error, error_message);

      // write back the original value
      *(filename_without_path - 1) = c;

      if (res != ERROR_OK) {
        return res;
      }

      // try again
      fout = SERENEDB_FOPEN(full_path.c_str(), "wb");
    } else if (fout == nullptr) {
      // try to create the target directory recursively
      // strip filename so we only have the directory name
      auto dir = SdbDirname(
        basics::file_utils::BuildFilename(out_path, filename_in_zip));
      auto res = SdbCreateRecursiveDirectory(dir, system_error, error_message);

      if (res != ERROR_OK) {
        return res;
      }

      // try again
      fout = SERENEDB_FOPEN(full_path.c_str(), "wb");
    }

    if (fout == nullptr) {
      error_message = absl::StrCat("failed to open file '", full_path,
                                   "' for writing: ", strerror(errno));
      return ERROR_CANNOT_WRITE_FILE;
    }

    while (true) {
      int result = unzReadCurrentFile(uf, buffer, (unsigned int)buffer_size);

      if (result < 0) {
        error_message =
          absl::StrCat("failed to read from zip file: ", strerror(errno));
        fclose(fout);
        return ERROR_CANNOT_WRITE_FILE;
      }

      if (result > 0) {
        if (fwrite(buffer, result, 1, fout) != 1) {
          error_message = absl::StrCat("failed to write file ", full_path,
                                       " - ", strerror(errno));
          fclose(fout);
          SetError(ERROR_SYS_ERROR);
          return ERROR_SYS_ERROR;
        }
      } else {
        SDB_ASSERT(result == 0);
        break;
      }
    }
    fclose(fout);
  }

  int ret = unzCloseCurrentFile(uf);
  if (ret < 0 && ret != UNZ_PARAMERROR) {
    // we must ignore UNZ_PARAMERROR here.
    // this error is returned if some of the internal zip file structs are not
    // properly set up. but this is not a real error here
    // we want to catch CRC errors here though
    error_message =
      std::string("cannot read from zip file: ") + TranslateError(ret);
    return ERROR_CANNOT_WRITE_FILE;
  }

  return ERROR_OK;
}

ErrorCode UnzipFile(unzFile uf, void* buffer, const size_t buffer_size,
                    const char* out_path, const bool skip_paths,
                    const bool overwrite, const char* password,
                    std::string& error_message) {
  unz_global_info64 gi;
  uLong i;
  auto res = ERROR_OK;
  int err;

  err = unzGetGlobalInfo64(uf, &gi);
  if (err != UNZ_OK) {
    error_message = "failed to get info: " + std::to_string(err);
    return ERROR_INTERNAL;
  }

  for (i = 0; i < gi.number_entry; i++) {
    res = ExtractCurrentFile(uf, buffer, buffer_size, out_path, skip_paths,
                             overwrite, password, error_message);

    if (res != ERROR_OK) {
      break;
    }

    if ((i + 1) < gi.number_entry) {
      err = unzGoToNextFile(uf);
      if (err == UNZ_END_OF_LIST_OF_FILE) {
        break;
      } else if (err != UNZ_OK) {
        error_message = "Failed to jump to next file: " + std::to_string(err);
        res = ERROR_INTERNAL;
        break;
      }
    }
  }

  return res;
}

}  // namespace

ErrorCode ZipFile(const char* filename, const char* dir,
                  const std::vector<std::string>& files, const char* password) {
  char* buffer;
#ifdef USEWIN32IOAPI
  zlib_filefunc64_def ffunc;
#endif

  if (SdbExistsFile(filename)) {
    return ERROR_CANNOT_OVERWRITE_FILE;
  }

  constexpr int kBufferSize = 16384;
  buffer = new (std::nothrow) char[kBufferSize];

  if (buffer == nullptr) {
    return ERROR_OUT_OF_MEMORY;
  }

#ifdef USEWIN32IOAPI
  fill_win32_filefunc64A(&ffunc);
  zipFile zf = zipOpen2_64(filename, 0, NULL, &ffunc);
#else
  zipFile zf = zipOpen64(filename, 0);
#endif

  if (zf == nullptr) {
    delete[] buffer;

    return ERROR_CANNOT_WRITE_FILE;
  }

  auto res = ERROR_OK;

  size_t n = files.size();
  for (size_t i = 0; i < n; ++i) {
    std::string fullfile;

    if (*dir == '\0') {
      fullfile = files[i];
    } else {
      fullfile = sdb::basics::file_utils::BuildFilename(dir, files[i]);
    }

    zip_fileinfo zi;
    memset(&zi, 0, sizeof(zi));

    uint32_t crc;
    res = SdbCrc32File(fullfile.c_str(), &crc);

    if (res != ERROR_OK) {
      break;
    }

    int is_large = (SdbSizeFile(files[i].c_str()) > 0xFFFFFFFFLL);

    const char* save_name = files[i].c_str();

    while (*save_name == '\\' || *save_name == '/') {
      ++save_name;
    }

    if (zipOpenNewFileInZip3_64(
          zf, save_name, &zi, NULL, 0, NULL, 0, NULL, /* comment*/
          Z_DEFLATED, Z_DEFAULT_COMPRESSION, 0, -MAX_WBITS, DEF_MEM_LEVEL,
          Z_DEFAULT_STRATEGY, password, (unsigned long)crc,
          is_large) != ZIP_OK) {
      res = ERROR_INTERNAL;
      break;
    }

    FILE* fin = SERENEDB_FOPEN(fullfile.c_str(), "rb");

    if (fin == nullptr) {
      break;
    }

    while (true) {
      int size_read = (int)fread(buffer, 1, kBufferSize, fin);
      if (size_read < kBufferSize) {
        if (feof(fin) == 0) {
          res = ERROR_SYS_ERROR;
          SetError(res);
          break;
        }
      }

      if (size_read > 0) {
        if (0 != zipWriteInFileInZip(zf, buffer, size_read)) {
          break;
        }
      } else /* if (sizeRead <= 0) */ {
        break;
      }
    }

    fclose(fin);

    zipCloseFileInZip(zf);

    if (res != ERROR_OK) {
      break;
    }
  }

  zipClose(zf, NULL);

  delete[] buffer;

  return res;
}

ErrorCode Adler32(const char* filename, uint32_t& checksum) {
  checksum = 0;

  if (!SdbIsRegularFile(filename) && !SdbIsSymbolicLink(filename)) {
    return ERROR_FILE_NOT_FOUND;
  }

  int fd = SERENEDB_OPEN(filename, O_RDONLY);
  if (fd < 0) {
    return ERROR_FILE_NOT_FOUND;
  }
  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  struct stat statbuf;
  int res = SERENEDB_FSTAT(fd, &statbuf);
  if (res < 0) {
    SetError(ERROR_SYS_ERROR);
    return ERROR_SYS_ERROR;
  }

  size_t chunk_remain = static_cast<size_t>(statbuf.st_size);
  char* buf = new (std::nothrow) char[131072];

  if (buf == nullptr) {
    return ERROR_OUT_OF_MEMORY;
  }

  uLong adler = adler32(0L, Z_NULL, 0);
  while (chunk_remain > 0) {
    size_t read_chunk;
    if (chunk_remain > 131072) {
      read_chunk = 131072;
    } else {
      read_chunk = chunk_remain;
    }

    ssize_t n_read = SERENEDB_READ(fd, buf, read_chunk);

    if (n_read < 0) {
      delete[] buf;
      return ERROR_INTERNAL;
    }

    adler = adler32(adler, reinterpret_cast<const unsigned char*>(buf),
                    static_cast<uInt>(n_read));
    chunk_remain -= n_read;
  }

  delete[] buf;

  checksum = static_cast<uint32_t>(adler);
  return ERROR_OK;
}

ErrorCode UnzipFile(const char* filename, const char* out_path, bool skip_paths,
                    bool overwrite, const char* password,
                    std::string& error_message) {
#ifdef USEWIN32IOAPI
  zlib_filefunc64_def ffunc;
#endif
  static constexpr size_t kBufferSize = 16384;
  char* buffer = new (std::nothrow) char[kBufferSize];

  if (buffer == nullptr) {
    return ERROR_OUT_OF_MEMORY;
  }

#ifdef USEWIN32IOAPI
  fill_win32_filefunc64A(&ffunc);
  unzFile uf = unzOpen2_64(filename, &ffunc);
#else
  unzFile uf = unzOpen64(filename);
#endif
  if (uf == nullptr) {
    delete[] buffer;
    error_message = std::string("unable to open zip file '") + filename + "'";
    return ERROR_INTERNAL;
  }

  auto res = UnzipFile(uf, buffer, kBufferSize, out_path, skip_paths, overwrite,
                       password, error_message);

  unzClose(uf);

  delete[] buffer;

  return res;
}

}  // namespace sdb
