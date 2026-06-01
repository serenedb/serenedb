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

#include "files.h"

#include <absl/cleanup/cleanup.h>
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>

#include <openssl/evp.h>

#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/operating-system.h"
#include "basics/string_utils.h"

namespace sdb {

bool SdbSlurpFile(const char* filename, std::string& result) {
  SetError(ERROR_OK);
  const int fd = SERENEDB_OPEN(filename, O_RDONLY | SERENEDB_O_CLOEXEC);
  if (fd == -1) {
    SetError(ERROR_SYS_ERROR);
    return false;
  }
  absl::Cleanup guard = [&]() noexcept { SERENEDB_CLOSE(fd); };

  auto true_size = result.size();
  while (true) {
    basics::StrResizeAmortized(result, true_size + kReadBufferSize);
    auto n = SERENEDB_READ(fd, result.data() + true_size, kReadBufferSize);
    if (n == 0) {
      result.erase(true_size);
      return true;
    }
    if (n < 0) {
      result = {};
      SetError(ERROR_SYS_ERROR);
      return false;
    }
    true_size += n;
  }
}

bool SdbSlurpGzipFile(const char* filename, std::string& result) {
  SetError(ERROR_OK);
  gzFile gz_fd = gzopen(filename, "rb");
  if (!gz_fd) {
    SetError(ERROR_SYS_ERROR);
    return false;
  }
  absl::Cleanup fd_guard = [&gz_fd]() noexcept { gzclose(gz_fd); };

  auto true_size = result.size();
  while (true) {
    basics::StrResizeAmortized(result, true_size + kReadBufferSize);
    const auto n = gzread(gz_fd, result.data() + true_size, kReadBufferSize);
    if (n == 0) {
      result.erase(true_size);
      return true;
    }
    if (n < 0) {
      result = {};
      SetError(ERROR_SYS_ERROR);
      return false;
    }
    true_size += n;
  }
}

bool SdbGETENV(const char* which, std::string& value) {
  const char* v = std::getenv(which);
  if (v == nullptr) {
    return false;
  }
  value = v;
  return true;
}

Sha256Functor::Sha256Functor()
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  : _context(EVP_MD_CTX_new()) {
#else
  : _context(EVP_MD_CTX_create()) {
#endif
  auto* context = static_cast<EVP_MD_CTX*>(_context);
  if (context == nullptr) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }
  if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 0) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    EVP_MD_CTX_free(context);
#else
    EVP_MD_CTX_destroy(_context);
#endif
    SDB_THROW(ERROR_INTERNAL, "unable to initialize SHA256 processor");
  }
}

Sha256Functor::~Sha256Functor() {
  auto* context = static_cast<EVP_MD_CTX*>(_context);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  EVP_MD_CTX_free(context);
#else
  EVP_MD_CTX_destroy(context);
#endif
}

bool Sha256Functor::operator()(const char* data, size_t size) noexcept {
  auto* context = static_cast<EVP_MD_CTX*>(_context);
  return EVP_DigestUpdate(context, static_cast<const void*>(data), size) == 1;
}

std::string Sha256Functor::finalize() {
  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int length_of_hash = 0;
  auto* context = static_cast<EVP_MD_CTX*>(_context);
  if (EVP_DigestFinal_ex(context, hash, &length_of_hash) == 0) {
    SDB_ASSERT(false);
  }
  return basics::string_utils::EncodeHex(
    reinterpret_cast<const char*>(&hash[0]), length_of_hash);
}

}  // namespace sdb
