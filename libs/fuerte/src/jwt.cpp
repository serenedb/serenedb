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

#include <absl/strings/escaping.h>
#include <absl/strings/internal/escaping.h>
#include <absl/strings/str_cat.h>
#include <fuerte/helper.h>
#include <fuerte/jwt.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <vpack/builder.h>

#include <chrono>

#include "basics/assert.h"

namespace sdb::fuerte {

/// generate a JWT token for internal cluster communication
std::string jwt::GenerateInternalToken(std::string_view secret) {
  std::chrono::seconds iss = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::system_clock::now().time_since_epoch());
  /*std::chrono::seconds exp =
   std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
   + _valid_for;*/

  vpack::Builder body_builder;
  body_builder.openObject();
  body_builder.add("server_id", vpack::Slice::nullSlice());
  body_builder.add("iss", "serenedb");
  body_builder.add("iat", static_cast<uint64_t>(iss.count()));
  // bodyBuilder.add("exp", exp.count());
  body_builder.close();
  return GenerateRawJwt(secret, body_builder.slice());
}

/// Generate JWT token as used for 'users' in serenedb
std::string jwt::GenerateUserToken(std::string_view secret,
                                   std::string_view username,
                                   std::chrono::seconds valid_for) {
  SDB_ASSERT(!secret.empty());

  std::chrono::seconds iss = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::system_clock::now().time_since_epoch());

  vpack::Builder body_builder;
  body_builder.openObject(/*unindexed*/ true);
  body_builder.add("preferred_username", username);
  body_builder.add("iss", "serenedb");
  body_builder.add("iat", static_cast<uint64_t>(iss.count()));
  if (valid_for.count() > 0) {
    body_builder.add("exp", static_cast<uint64_t>((iss + valid_for).count()));
  }
  body_builder.close();
  return GenerateRawJwt(secret, body_builder.slice());
}

std::string jwt::GenerateRawJwt(std::string_view secret,
                                vpack::Slice body_slice) {
  vpack::Builder header_builder;
  {
    vpack::ObjectBuilder h(&header_builder);
    header_builder.add("alg", "HS256");
    header_builder.add("typ", "JWT");
  }

  // https://tools.ietf.org/html/rfc7515#section-2 requires
  // JWT to use base64-encoding without trailing padding `=` chars
  // TODO: Maybe I wrong but it looks like we **should** use only base64url
  //  Which is by default without padding.

  auto header = header_builder.toJson();
  auto body = body_slice.toJson();
  std::string header_base64;
  std::string body_base64;

  absl::strings_internal::Base64EscapeInternal(
    reinterpret_cast<const unsigned char*>(header.data()), header.size(),
    &header_base64, false, absl::strings_internal::kBase64Chars);
  absl::strings_internal::Base64EscapeInternal(
    reinterpret_cast<const unsigned char*>(body.data()), body.size(),
    &body_base64, false, absl::strings_internal::kBase64Chars);

  auto full_message = absl::StrCat(header_base64, ".", body_base64);

  std::string signature =
    SslHmac(secret.data(), secret.size(), full_message.data(),
            full_message.size(), Algorithm::kAlgorithmSha256);

  return absl::StrCat(full_message, ".", absl::WebSafeBase64Escape(signature));
}

// code from SereneDBs SslInterface.cpp

std::string jwt::SslHmac(const char* key, size_t key_length,
                         const char* message, size_t message_len,
                         Algorithm algorithm) {
  EVP_MD* evp_md = nullptr;

  if (algorithm == Algorithm::kAlgorithmSha1) {
    evp_md = const_cast<EVP_MD*>(EVP_sha1());
  } else if (algorithm == jwt::Algorithm::kAlgorithmSha224) {
    evp_md = const_cast<EVP_MD*>(EVP_sha224());
  } else if (algorithm == jwt::Algorithm::kAlgorithmMD5) {
    evp_md = const_cast<EVP_MD*>(EVP_md5());
  } else if (algorithm == jwt::Algorithm::kAlgorithmSha384) {
    evp_md = const_cast<EVP_MD*>(EVP_sha384());
  } else if (algorithm == jwt::Algorithm::kAlgorithmSha512) {
    evp_md = const_cast<EVP_MD*>(EVP_sha512());
  } else {
    // default
    evp_md = const_cast<EVP_MD*>(EVP_sha256());
  }

  unsigned char* md = nullptr;
  try {
    md = static_cast<unsigned char*>(malloc(EVP_MAX_MD_SIZE + 1));
    if (!md) {
      return "";
    }
    unsigned int md_len;
    HMAC(evp_md, key, (int)key_length, (const unsigned char*)message,
         message_len, md, &md_len);
    std::string copy((char*)md, md_len);
    free(md);
    return copy;
  } catch (...) {
    free(md);
  }
  return "";
}

bool jwt::VerifyHMAC(const char* challenge, size_t challenge_length,
                     const char* secret, size_t secret_len,
                     const char* response, size_t response_len,
                     jwt::Algorithm algorithm) {
  // challenge = key
  // secret, secretLen = message
  // result must == BASE64(response, responseLen)

  std::string s =
    SslHmac(challenge, challenge_length, secret, secret_len, algorithm);

  if (s.length() == response_len &&
      s.compare(std::string(response, response_len)) == 0) {
    return true;
  }

  return false;
}

}  // namespace sdb::fuerte
