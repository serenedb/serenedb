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
#include <string>
#include <string_view>

namespace sdb::network::pg {

// The client half of the SCRAM-SHA-256 SASL exchange, the mirror of the
// server-side verifier in credentials.cpp. Drives the three client steps a
// pg-wire client runs to authenticate to a publisher: emit client-first, turn
// server-first into client-final (deriving the proof from the password + the
// server's salt/iterations), and verify server-final. Pure state -- no IO.
class ScramClientSession {
 public:
  static constexpr std::string_view kMechanism = "SCRAM-SHA-256";

  explicit ScramClientSession(std::string password);

  // client-first-message: "n,,n=,r=<client-nonce>". The username is left empty
  // (as libpq does): the publisher takes it from the StartupMessage. Called
  // once; the nonce is generated here.
  std::string ClientFirst();

  // Consume server-first-message "r=<nonce>,s=<b64 salt>,i=<iters>" and produce
  // client-final-message "c=biws,r=<nonce>,p=<b64 proof>". nullopt on a
  // malformed message, a nonce that does not extend ours, or a crypto failure.
  std::optional<std::string> ServerFirst(std::string_view server_first);

  // Verify server-final-message "v=<b64 server-signature>" against the value we
  // derived in ServerFirst. Must be called after a successful ServerFirst.
  bool ServerFinal(std::string_view server_final);

 private:
  std::string _password;
  std::string _client_nonce;
  std::string _client_first_bare;  // "n=,r=<nonce>"
  std::array<uint8_t, 32> _expected_server_sig{};
  bool _have_server_sig = false;
};

}  // namespace sdb::network::pg
