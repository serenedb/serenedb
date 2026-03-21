////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vpack/slice.h>

#include <chrono>
#include <string>

namespace sdb::fuerte::jwt {

/// Generate JWT token as used by internal serenedb communication
std::string GenerateInternalToken(std::string_view secret);

/// Generate JWT token as used for 'users' in serenedb
std::string GenerateUserToken(
  std::string_view secret, std::string_view username,
  std::chrono::seconds valid_for = std::chrono::seconds{0});

std::string GenerateRawJwt(std::string_view secret, vpack::Slice body);

enum Algorithm {
  kAlgorithmSha256 = 0,
  kAlgorithmSha1 = 1,
  kAlgorithmMD5 = 2,
  kAlgorithmSha224 = 3,
  kAlgorithmSha384 = 4,
  kAlgorithmSha512 = 5
};

std::string SslHmac(const char* key, size_t key_length, const char* message,
                    size_t message_len, Algorithm algorithm);

bool VerifyHMAC(const char* challenge, size_t challenge_length,
                const char* secret, size_t secret_len, const char* response,
                size_t response_len, Algorithm algorithm);

}  // namespace sdb::fuerte::jwt
