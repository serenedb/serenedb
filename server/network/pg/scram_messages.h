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

#include <optional>
#include <string_view>

namespace sdb::network::pg {

// The structural pieces of a SCRAM client-first-message
// (RFC 5802 gs2-header + client-first-message-bare). All views borrow the input
// buffer. Channel-binding *policy* (n/y/p, TLS, downgrade), the empty-authzid
// requirement, and the nonce value are the caller's to enforce -- this only
// splits the message.
struct ScramClientFirst {
  std::string_view cbind_flag;  // gs2 cbind flag: "n", "y", or "p=<type>"
  std::string_view authzid;     // authorization identity (must be empty)
  std::string_view gs2_header;  // through the second comma, inclusive
  std::string_view client_first_bare;  // everything after the gs2 header
};

// Split a client-first-message at its two leading commas. Returns nullopt when
// the second comma is absent (malformed gs2 header).
std::optional<ScramClientFirst> ParseScramClientFirst(std::string_view message);

// The structural pieces of a SCRAM client-final-message. All views borrow the
// input buffer. The caller verifies the nonce echo, channel binding, and proof.
struct ScramClientFinal {
  std::string_view
    channel_binding;       // c= value: base64(gs2-header [+ cbind data])
  std::string_view nonce;  // r= value: must echo the combined nonce
  std::string_view proof;  // p= value: base64 ClientProof
  std::string_view without_proof;  // the message up to (excluding) ",p="
};

// Split a client-final-message into c=/r=/p=. Returns nullopt when it does not
// start with "c=", has no ",p=", or has no ",r=" before the proof.
std::optional<ScramClientFinal> ParseScramClientFinal(std::string_view message);

}  // namespace sdb::network::pg
