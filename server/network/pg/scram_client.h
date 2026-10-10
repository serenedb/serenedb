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

class ScramClientSession {
 public:
  static constexpr std::string_view kMechanism = "SCRAM-SHA-256";

  explicit ScramClientSession(std::string password);

  std::optional<std::string> ClientFirst();
  std::optional<std::string> ServerFirst(std::string_view server_first);
  bool ServerFinal(std::string_view server_final);

 private:
  std::string _password;
  std::string _client_nonce;
  std::string _client_first_bare;
  std::optional<std::array<uint8_t, 32>> _expected_server_sig;
};

}  // namespace sdb::network::pg
