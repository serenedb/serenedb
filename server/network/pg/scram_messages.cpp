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

#include "network/pg/scram_messages.h"

namespace sdb::network::pg {

std::optional<ScramClientFirst> ParseScramClientFirst(
  std::string_view message) {
  const auto comma1 = message.find(',');
  const auto comma2 = comma1 == std::string_view::npos
                        ? std::string_view::npos
                        : message.find(',', comma1 + 1);
  if (comma2 == std::string_view::npos) {
    return std::nullopt;
  }
  return ScramClientFirst{
    .cbind_flag = message.substr(0, comma1),
    .authzid = message.substr(comma1 + 1, comma2 - comma1 - 1),
    .gs2_header = message.substr(0, comma2 + 1),
    .client_first_bare = message.substr(comma2 + 1),
  };
}

std::optional<ScramClientFinal> ParseScramClientFinal(
  std::string_view message) {
  const auto proof_pos = message.rfind(",p=");
  if (proof_pos == std::string_view::npos || !message.starts_with("c=")) {
    return std::nullopt;
  }
  const auto without_proof = message.substr(0, proof_pos);
  const auto r_idx = without_proof.find(",r=");
  if (r_idx == std::string_view::npos) {
    return std::nullopt;
  }
  return ScramClientFinal{
    .channel_binding = without_proof.substr(2, r_idx - 2),
    .nonce = without_proof.substr(r_idx + 3),
    .proof = message.substr(proof_pos + 3),
    .without_proof = without_proof,
  };
}

}  // namespace sdb::network::pg
