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

std::string SslMD5(std::string_view input) {
  char hash[16];
  SslMD5(input.data(), input.size(), &hash[0]);

  char hex[32];
  ssl_interface::SslHex(hash, 16, &hex[0]);

  return std::string(hex, 32);
}

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

void SslHex(const char* input_str, size_t length, char* output_str) noexcept {
  SDB_ASSERT(input_str != nullptr);
  SDB_ASSERT(output_str != nullptr);

  constexpr char kHexval[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  const char* e = input_str + length;
  char* p = output_str;

  for (const char* q = input_str; q < e; ++q) {
    *p++ = kHexval[(*q >> 4) & 0xF];
    *p++ = kHexval[*q & 0x0F];
  }
  SDB_ASSERT(static_cast<size_t>(p - output_str) == 2 * length);
}

std::string SslPbkdF2HS1(const char* salt, size_t salt_length, const char* pass,
                         size_t pass_length, int iter, int key_length) {
  unsigned char* dk = new (std::nothrow) unsigned char[key_length + 1];
  if (dk == nullptr) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }
  irs::Finally sg = [&] noexcept { delete[] dk; };

  PKCS5_PBKDF2_HMAC_SHA1(pass, (int)pass_length, (const unsigned char*)salt,
                         (int)salt_length, iter, key_length, dk);

  return basics::string_utils::EncodeHex((char*)dk, key_length);
}

std::string SslPbkdF2(const char* salt, size_t salt_length, const char* pass,
                      size_t pass_length, int iter, int key_length,
                      Algorithm algorithm) {
  EVP_MD* evp_md = nullptr;

  if (algorithm == Algorithm::kAlgorithmSha1) {
    evp_md = const_cast<EVP_MD*>(EVP_sha1());
  } else if (algorithm == Algorithm::kAlgorithmSha224) {
    evp_md = const_cast<EVP_MD*>(EVP_sha224());
  } else if (algorithm == Algorithm::kAlgorithmMD5) {
    evp_md = const_cast<EVP_MD*>(EVP_md5());
  } else if (algorithm == Algorithm::kAlgorithmSha384) {
    evp_md = const_cast<EVP_MD*>(EVP_sha384());
  } else if (algorithm == Algorithm::kAlgorithmSha512) {
    evp_md = const_cast<EVP_MD*>(EVP_sha512());
  } else {
    evp_md = const_cast<EVP_MD*>(EVP_sha256());
  }

  unsigned char* dk = new (std::nothrow) unsigned char[key_length + 1];
  if (dk == nullptr) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }

  irs::Finally sg = [&] noexcept { delete[] dk; };

  PKCS5_PBKDF2_HMAC(pass, (int)pass_length, (const unsigned char*)salt,
                    (int)salt_length, iter, evp_md, key_length, dk);

  return basics::string_utils::EncodeHex((char*)dk, key_length);
}

std::string SslHmac(const char* key, size_t key_length, const char* message,
                    size_t message_len, Algorithm algorithm) {
  EVP_MD* evp_md = nullptr;

  if (algorithm == Algorithm::kAlgorithmSha1) {
    evp_md = const_cast<EVP_MD*>(EVP_sha1());
  } else if (algorithm == Algorithm::kAlgorithmSha224) {
    evp_md = const_cast<EVP_MD*>(EVP_sha224());
  } else if (algorithm == Algorithm::kAlgorithmMD5) {
    evp_md = const_cast<EVP_MD*>(EVP_md5());
  } else if (algorithm == Algorithm::kAlgorithmSha384) {
    evp_md = const_cast<EVP_MD*>(EVP_sha384());
  } else if (algorithm == Algorithm::kAlgorithmSha512) {
    evp_md = const_cast<EVP_MD*>(EVP_sha512());
  } else {
    // default
    evp_md = const_cast<EVP_MD*>(EVP_sha256());
  }

  unsigned char* md = new (std::nothrow) unsigned char[EVP_MAX_MD_SIZE + 1];
  if (md == nullptr) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }

  irs::Finally sg = [&] noexcept { delete[] md; };
  unsigned int md_len;

  HMAC(evp_md, key, (int)key_length, (const unsigned char*)message, message_len,
       md, &md_len);

  return std::string((char*)md, md_len);
}

bool VerifyHMAC(const char* challenge, size_t challenge_length,
                const char* secret, size_t secret_len, const char* response,
                size_t response_len, Algorithm algorithm) {
  // challenge = key
  // secret, secretLen = message
  // result must == BASE64(response, responseLen)

  std::string s =
    SslHmac(challenge, challenge_length, secret, secret_len, algorithm);
  return s == std::string_view{response, response_len};
}

int SslRand(uint64_t* value) {
  if (!RAND_bytes((unsigned char*)value, sizeof(uint64_t))) {
    return 1;
  }

  return 0;
}

int SslRand(int64_t* value) {
  if (!RAND_bytes((unsigned char*)value, sizeof(int64_t))) {
    return 1;
  }

  return 0;
}

int SslRand(int32_t* value) {
  if (!RAND_bytes((unsigned char*)value, sizeof(int32_t))) {
    return 1;
  }

  return 0;
}

int RsaPrivSign(EVP_MD_CTX* ctx, EVP_PKEY* pkey, const std::string& msg,
                std::string& sign, std::string& error) {
  size_t sign_length;
  if (EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) == 0) {
    error.append("EVP_DigestSignInit failed: ")
      .append(ERR_error_string(ERR_get_error(), nullptr));
    return 1;
  }
  if (EVP_DigestSignUpdate(ctx, msg.c_str(), msg.size()) == 0) {
    error.append("EVP_DigestSignUpdate failed: ")
      .append(ERR_error_string(ERR_get_error(), nullptr));
    return 1;
  }
  if (EVP_DigestSignFinal(ctx, nullptr, &sign_length) == 0) {
    error.append("EVP_DigestSignFinal failed: ")
      .append(ERR_error_string(ERR_get_error(), nullptr));
    return 1;
  }
  sign.resize(sign_length);
  if (EVP_DigestSignFinal(ctx, (unsigned char*)sign.data(), &sign_length) ==
      0) {
    error.append("EVP_DigestSignFinal failed, return code: ")
      .append(ERR_error_string(ERR_get_error(), nullptr));
    return 1;
  }
  return 0;
}

int RsaPrivSign(const std::string& pem, const std::string& msg,
                std::string& sign, std::string& error) {
  BIO* keybio = BIO_new_mem_buf(pem.c_str(), -1);
  if (keybio == nullptr) {
    error.append("Failed to initialize keybio.");
    return 1;
  }
  irs::Finally cleanup_bio = [&]() noexcept { BIO_free_all(keybio); };

  RSA* rsa = RSA_new();
  if (rsa == nullptr) {
    error.append("Failed to initialize RSA algorithm.");
    return 1;
  }
  rsa = PEM_read_bio_RSAPrivateKey(keybio, &rsa, nullptr, nullptr);
  EVP_PKEY* p_key = EVP_PKEY_new();
  if (p_key == nullptr) {
    error.append("Failed to initialize public key.");
    return 1;
  }
  EVP_PKEY_assign_RSA(p_key, rsa);
  irs::Finally cleanup_keys = [&]() noexcept { EVP_PKEY_free(p_key); };

  auto* ctx = EVP_MD_CTX_new();
  irs::Finally cleanup_context = [&]() noexcept { EVP_MD_CTX_free(ctx); };
  if (ctx == nullptr) {
    error.append("EVP_MD_CTX_create failed,: ")
      .append(ERR_error_string(ERR_get_error(), nullptr));
    return 1;
  }

  return RsaPrivSign(ctx, p_key, msg, sign, error);
}

}  // namespace sdb::rest::ssl_interface
