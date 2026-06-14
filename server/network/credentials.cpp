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

#include "network/credentials.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <charconv>
#include <cstring>

// SCRAM-SHA-256 crypto (RFC 5802) over OpenSSL -- the same primitives libpq's
// scram-common wraps. SASLprep (RFC 4013) is not applied: ASCII passwords are
// unaffected; non-ASCII normalization is a follow-up.
namespace sdb::network {
namespace {

constexpr char kB64[] =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

void Hmac256(std::span<const uint8_t> key, std::string_view msg, uint8_t* out) {
  unsigned int len = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const uint8_t*>(msg.data()), msg.size(), out, &len);
}

}  // namespace

std::string Base64Encode(std::span<const uint8_t> in) {
  std::string out;
  out.reserve((in.size() + 2) / 3 * 4);
  size_t i = 0;
  for (; i + 3 <= in.size(); i += 3) {
    const uint32_t n = (uint32_t{in[i]} << 16) | (uint32_t{in[i + 1]} << 8) |
                       uint32_t{in[i + 2]};
    out += kB64[(n >> 18) & 63];
    out += kB64[(n >> 12) & 63];
    out += kB64[(n >> 6) & 63];
    out += kB64[n & 63];
  }
  if (in.size() - i == 1) {
    const uint32_t n = uint32_t{in[i]} << 16;
    out += kB64[(n >> 18) & 63];
    out += kB64[(n >> 12) & 63];
    out += '=';
    out += '=';
  } else if (in.size() - i == 2) {
    const uint32_t n = (uint32_t{in[i]} << 16) | (uint32_t{in[i + 1]} << 8);
    out += kB64[(n >> 18) & 63];
    out += kB64[(n >> 12) & 63];
    out += kB64[(n >> 6) & 63];
    out += '=';
  }
  return out;
}

std::optional<std::vector<uint8_t>> Base64Decode(std::string_view in) {
  const auto value = [](char c) -> int {
    if (c >= 'A' && c <= 'Z') {
      return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
      return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9') {
      return c - '0' + 52;
    }
    if (c == '+') {
      return 62;
    }
    if (c == '/') {
      return 63;
    }
    return -1;
  };
  std::vector<uint8_t> out;
  uint32_t buf = 0;
  int bits = 0;
  for (const char c : in) {
    if (c == '=') {
      break;
    }
    const int v = value(c);
    if (v < 0) {
      return std::nullopt;
    }
    buf = (buf << 6) | static_cast<uint32_t>(v);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push_back(static_cast<uint8_t>((buf >> bits) & 0xFF));
    }
  }
  return out;
}

bool RandomBytes(std::span<uint8_t> out) {
  return RAND_bytes(out.data(), static_cast<int>(out.size())) == 1;
}

std::string SaslPrep(std::string_view password) {
  // ASCII passthrough; full RFC 4013 NFKC normalization is a follow-up.
  return std::string{password};
}

std::optional<ScramVerifier> BuildScramVerifier(std::string_view password) {
  ScramVerifier verifier;
  verifier.iterations = 4096;
  verifier.salt.resize(16);
  if (!RandomBytes(verifier.salt)) {
    return std::nullopt;
  }
  const std::string prepared = SaslPrep(password);
  uint8_t salted[kScramKeyLen];
  if (PKCS5_PBKDF2_HMAC(
        prepared.data(), static_cast<int>(prepared.size()),
        verifier.salt.data(), static_cast<int>(verifier.salt.size()),
        verifier.iterations, EVP_sha256(), kScramKeyLen, salted) != 1) {
    return std::nullopt;
  }
  uint8_t client_key[kScramKeyLen];
  Hmac256({salted, kScramKeyLen}, "Client Key", client_key);
  SHA256(client_key, kScramKeyLen, verifier.stored_key.data());
  Hmac256({salted, kScramKeyLen}, "Server Key", verifier.server_key.data());
  return verifier;
}

std::optional<ScramVerifier> ParseScramVerifier(std::string_view verifier) {
  constexpr std::string_view kPrefix = "SCRAM-SHA-256$";
  if (!verifier.starts_with(kPrefix)) {
    return std::nullopt;
  }
  verifier.remove_prefix(kPrefix.size());
  const auto colon = verifier.find(':');
  if (colon == std::string_view::npos) {
    return std::nullopt;
  }
  ScramVerifier result;
  const auto iters = verifier.substr(0, colon);
  if (std::from_chars(iters.data(), iters.data() + iters.size(),
                      result.iterations)
          .ec != std::errc{} ||
      result.iterations <= 0) {
    return std::nullopt;
  }
  verifier.remove_prefix(colon + 1);
  const auto dollar = verifier.find('$');
  if (dollar == std::string_view::npos) {
    return std::nullopt;
  }
  auto salt = Base64Decode(verifier.substr(0, dollar));
  if (!salt) {
    return std::nullopt;
  }
  result.salt = std::move(*salt);
  verifier.remove_prefix(dollar + 1);
  const auto sep = verifier.find(':');
  if (sep == std::string_view::npos) {
    return std::nullopt;
  }
  const auto stored = Base64Decode(verifier.substr(0, sep));
  const auto server = Base64Decode(verifier.substr(sep + 1));
  if (!stored || stored->size() != kScramKeyLen || !server ||
      server->size() != kScramKeyLen) {
    return std::nullopt;
  }
  std::memcpy(result.stored_key.data(), stored->data(), kScramKeyLen);
  std::memcpy(result.server_key.data(), server->data(), kScramKeyLen);
  return result;
}

bool ConstantTimeEqual(std::span<const uint8_t> a, std::span<const uint8_t> b) {
  if (a.size() != b.size()) {
    return false;
  }
  uint8_t diff = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    diff |= a[i] ^ b[i];
  }
  return diff == 0;
}

bool VerifyCleartextAgainstScram(const ScramVerifier& verifier,
                                 std::string_view password) {
  const std::string prepared = SaslPrep(password);
  uint8_t salted[kScramKeyLen];
  if (PKCS5_PBKDF2_HMAC(
        prepared.data(), static_cast<int>(prepared.size()),
        verifier.salt.data(), static_cast<int>(verifier.salt.size()),
        verifier.iterations, EVP_sha256(), kScramKeyLen, salted) != 1) {
    return false;
  }
  uint8_t client_key[kScramKeyLen];
  Hmac256({salted, kScramKeyLen}, "Client Key", client_key);
  uint8_t computed_stored[kScramKeyLen];
  SHA256(client_key, kScramKeyLen, computed_stored);
  return ConstantTimeEqual({computed_stored, kScramKeyLen},
                           verifier.stored_key);
}

bool VerifyClientProof(const ScramVerifier& verifier,
                       std::string_view auth_message,
                       std::span<const uint8_t> proof) {
  if (proof.size() != kScramKeyLen) {
    return false;
  }
  uint8_t client_signature[kScramKeyLen];
  Hmac256(verifier.stored_key, auth_message, client_signature);
  uint8_t client_key[kScramKeyLen];
  for (int i = 0; i < kScramKeyLen; ++i) {
    client_key[i] = proof[i] ^ client_signature[i];
  }
  uint8_t computed_stored[kScramKeyLen];
  SHA256(client_key, kScramKeyLen, computed_stored);
  return ConstantTimeEqual({computed_stored, kScramKeyLen},
                           verifier.stored_key);
}

std::array<uint8_t, kScramKeyLen> ScramServerSignature(
  const ScramVerifier& verifier, std::string_view auth_message) {
  std::array<uint8_t, kScramKeyLen> signature{};
  Hmac256(verifier.server_key, auth_message, signature.data());
  return signature;
}

}  // namespace sdb::network
