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
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <span>
#include <string_view>

#include "network/credentials.h"

namespace sdb::network::pg {
namespace {

std::optional<std::string_view> Attr(std::string_view message, char key) {
  size_t pos = 0;
  while (pos < message.size()) {
    size_t end = message.find(',', pos);
    if (end == std::string_view::npos) {
      end = message.size();
    }
    const auto field = message.substr(pos, end - pos);
    if (field.size() >= 2 && field[0] == key && field[1] == '=') {
      return field.substr(2);
    }
    pos = end + 1;
  }
  return std::nullopt;
}

std::span<const uint8_t> Bytes(std::string_view text) {
  return {reinterpret_cast<const uint8_t*>(text.data()), text.size()};
}

std::string_view Chars(std::span<const uint8_t> bytes) {
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

}  // namespace

ScramClientSession::ScramClientSession(std::string password)
  : _password(std::move(password)) {}

std::optional<std::string> ScramClientSession::ClientFirst() {
  std::array<uint8_t, 18> raw{};
  if (!RandomBytes(raw)) {
    return std::nullopt;
  }
  _client_nonce = absl::Base64Escape(Chars(raw));
  _client_first_bare = absl::StrCat("n=,r=", _client_nonce);
  return absl::StrCat("n,,", _client_first_bare);
}

std::optional<std::string> ScramClientSession::ServerFirst(
  std::string_view server_first) {
  const auto nonce = Attr(server_first, 'r');
  const auto salt_b64 = Attr(server_first, 's');
  const auto iter_str = Attr(server_first, 'i');
  if (!nonce || !salt_b64 || !iter_str || !nonce->starts_with(_client_nonce)) {
    return std::nullopt;
  }
  int iterations = 0;
  if (!absl::SimpleAtoi(*iter_str, &iterations) || iterations <= 0) {
    return std::nullopt;
  }
  const auto salt = Base64Decode(*salt_b64);
  if (!salt) {
    return std::nullopt;
  }
  const auto client_final_no_proof = absl::StrCat("c=biws,r=", *nonce);
  const auto proof = ScramClientProofFromPassword(
    _password, Bytes(*salt), iterations,
    absl::StrCat(_client_first_bare, ",", server_first, ",",
                 client_final_no_proof));
  if (!proof) {
    return std::nullopt;
  }
  _expected_server_sig = proof->server_signature;
  return absl::StrCat(client_final_no_proof,
                      ",p=", absl::Base64Escape(Chars(proof->client_proof)));
}

bool ScramClientSession::ServerFinal(std::string_view server_final) {
  if (!_expected_server_sig) {
    return false;
  }
  const auto sig_b64 = Attr(server_final, 'v');
  if (!sig_b64) {
    return false;
  }
  const auto sig = Base64Decode(*sig_b64);
  return sig && sig->size() == _expected_server_sig->size() &&
         ConstantTimeEqual(Bytes(*sig), *_expected_server_sig);
}

}  // namespace sdb::network::pg
