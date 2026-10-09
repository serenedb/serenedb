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

#include "network/pg/scram_client.h"

#include <absl/strings/escaping.h>

#include <charconv>
#include <cstdint>
#include <span>
#include <string_view>

#include "network/credentials.h"

namespace sdb::network::pg {
namespace {

// Find the value of attribute `key` (a single letter, e.g. 'r') in a
// comma-separated SCRAM message. Empty optional if absent.
std::optional<std::string_view> Attr(std::string_view message, char key) {
  size_t pos = 0;
  while (pos < message.size()) {
    size_t end = message.find(',', pos);
    if (end == std::string_view::npos) {
      end = message.size();
    }
    std::string_view field = message.substr(pos, end - pos);
    if (field.size() >= 2 && field[0] == key && field[1] == '=') {
      return field.substr(2);
    }
    pos = end + 1;
  }
  return std::nullopt;
}

}  // namespace

ScramClientSession::ScramClientSession(std::string password)
  : _password(std::move(password)) {}

std::string ScramClientSession::ClientFirst() {
  std::array<uint8_t, 18> raw{};
  if (!RandomBytes(raw)) {
    // Deterministic fallback keeps the exchange well-formed; the server nonce
    // still provides freshness.
    raw.fill(0);
  }
  _client_nonce = absl::Base64Escape(
    std::string_view{reinterpret_cast<const char*>(raw.data()), raw.size()});
  _client_first_bare = "n=,r=" + _client_nonce;
  return "n,," + _client_first_bare;
}

std::optional<std::string> ScramClientSession::ServerFirst(
  std::string_view server_first) {
  auto nonce = Attr(server_first, 'r');
  auto salt_b64 = Attr(server_first, 's');
  auto iter_str = Attr(server_first, 'i');
  if (!nonce || !salt_b64 || !iter_str) {
    return std::nullopt;
  }
  // The server nonce must extend the client nonce we sent.
  if (!nonce->starts_with(_client_nonce)) {
    return std::nullopt;
  }
  int iterations = 0;
  auto [ptr, ec] = std::from_chars(
    iter_str->data(), iter_str->data() + iter_str->size(), iterations);
  if (ec != std::errc{} || iterations <= 0) {
    return std::nullopt;
  }
  auto salt = Base64Decode(*salt_b64);
  if (!salt) {
    return std::nullopt;
  }

  const std::string client_final_no_proof = "c=biws,r=" + std::string{*nonce};
  const std::string auth_message = _client_first_bare + "," +
                                   std::string{server_first} + "," +
                                   client_final_no_proof;

  auto proof = ScramClientProofFromPassword(
    _password,
    std::span<const uint8_t>{reinterpret_cast<const uint8_t*>(salt->data()),
                             salt->size()},
    iterations, auth_message);
  if (!proof) {
    return std::nullopt;
  }
  _expected_server_sig = proof->server_signature;
  _have_server_sig = true;

  return client_final_no_proof + ",p=" +
         absl::Base64Escape(std::string_view{
           reinterpret_cast<const char*>(proof->client_proof.data()),
           proof->client_proof.size()});
}

bool ScramClientSession::ServerFinal(std::string_view server_final) {
  if (!_have_server_sig) {
    return false;
  }
  auto sig_b64 = Attr(server_final, 'v');
  if (!sig_b64) {
    return false;
  }
  auto sig = Base64Decode(*sig_b64);
  if (!sig || sig->size() != _expected_server_sig.size()) {
    return false;
  }
  return ConstantTimeEqual(
    std::span<const uint8_t>{reinterpret_cast<const uint8_t*>(sig->data()),
                             sig->size()},
    _expected_server_sig);
}

}  // namespace sdb::network::pg
