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

#include "ssl_interface.h"

#include <absl/cleanup/cleanup.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <cstring>
#include <iostream>
#include <new>

#include "basics/exceptions.h"
#include "basics/misc.hpp"
#include "basics/random/uniform_character.h"
#include "basics/string_utils.h"

namespace sdb::rest::ssl_interface {

void SslMD5(const char* input_str, size_t length, char* output_str) noexcept {
  SDB_ASSERT(input_str != nullptr);
  SDB_ASSERT(output_str != nullptr);
  MD5((const unsigned char*)input_str, length, (unsigned char*)output_str);
}

void SslShA1(const char* input_str, size_t length, char* output_str) noexcept {
  SHA1((const unsigned char*)input_str, length, (unsigned char*)output_str);
}

void SslShA224(const char* input_str, size_t length,
               char* output_str) noexcept {
  SDB_ASSERT(input_str != nullptr);
  SDB_ASSERT(output_str != nullptr);
  SHA224((const unsigned char*)input_str, length, (unsigned char*)output_str);
}

void SslShA256(const char* input_str, size_t length,
               char* output_str) noexcept {
  SDB_ASSERT(input_str != nullptr);
  SDB_ASSERT(output_str != nullptr);
  SHA256((const unsigned char*)input_str, length, (unsigned char*)output_str);
}

void SslShA384(const char* input_str, size_t length,
               char* output_str) noexcept {
  SDB_ASSERT(input_str != nullptr);
  SDB_ASSERT(output_str != nullptr);
  SHA384((const unsigned char*)input_str, length, (unsigned char*)output_str);
}

void SslShA512(const char* input_str, size_t length,
               char* output_str) noexcept {
  SDB_ASSERT(input_str != nullptr);
  SDB_ASSERT(output_str != nullptr);
  SHA512((const unsigned char*)input_str, length, (unsigned char*)output_str);
}

}  // namespace sdb::rest::ssl_interface
