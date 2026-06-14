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

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::network {

inline constexpr int kScramKeyLen = 32;  // SCRAM_SHA_256_KEY_LEN

// A parsed SCRAM-SHA-256 verifier (the secret a server keeps; never the
// password). StoredKey = H(HMAC(SaltedPassword,"Client Key")),
// ServerKey = HMAC(SaltedPassword,"Server Key").
struct ScramVerifier {
  std::array<uint8_t, kScramKeyLen> stored_key{};
  std::array<uint8_t, kScramKeyLen> server_key{};
  std::vector<uint8_t> salt;
  int iterations = 0;
};

// What's needed to authenticate a user -- NOT how it's stored. `scram` enables
// the SCRAM mechanism; `cleartext` enables AuthenticationCleartextPassword and
// is what a future RBAC layer that only has plaintext would supply.
struct Credential {
  std::optional<ScramVerifier> scram;
  std::optional<std::string> cleartext;
};

// The storage-decoupled seam. Returning nullopt => no credential for this user
// => trust (unconfigured-server behavior). The colleague's RBAC provides a real
// implementation later; nothing else in the auth path changes.
class CredentialProvider {
 public:
  virtual ~CredentialProvider() = default;
  virtual std::optional<Credential> LookupCredential(
    std::string_view username) const = 0;
};

// --- crypto helpers (auth.cpp wraps the vendored libpq src/common crypto) ---

// Parse "SCRAM-SHA-256$<iters>:<b64 salt>$<b64 StoredKey>:<b64 ServerKey>".
std::optional<ScramVerifier> ParseScramVerifier(std::string_view verifier);

// Derive a fresh verifier from a cleartext password (random salt, default
// iterations) -- used by the temporary config credential source.
std::optional<ScramVerifier> BuildScramVerifier(std::string_view password);

// RFC 4013 SASLprep; falls back to the raw bytes on invalid-UTF8/prohibited.
std::string SaslPrep(std::string_view password);

// Cryptographically strong random bytes (pg_strong_random). false on failure.
bool RandomBytes(std::span<uint8_t> out);

std::string Base64Encode(std::span<const uint8_t> data);
std::optional<std::vector<uint8_t>> Base64Decode(std::string_view text);

// Verify the SCRAM client proof against the verifier + AuthMessage
// (constant-time). `proof` must be kScramKeyLen bytes.
bool VerifyClientProof(const ScramVerifier& verifier,
                       std::string_view auth_message,
                       std::span<const uint8_t> proof);

// Verify a cleartext password against a stored SCRAM verifier (derive
// StoredKey, constant-time compare). Lets HTTP Basic auth share the same
// credential store as the pg wire's SCRAM exchange.
bool VerifyCleartextAgainstScram(const ScramVerifier& verifier,
                                 std::string_view password);

// ServerSignature = HMAC(ServerKey, AuthMessage), for the server-final v=.
std::array<uint8_t, kScramKeyLen> ScramServerSignature(
  const ScramVerifier& verifier, std::string_view auth_message);

// Constant-time equality.
bool ConstantTimeEqual(std::span<const uint8_t> a, std::span<const uint8_t> b);

}  // namespace sdb::network
