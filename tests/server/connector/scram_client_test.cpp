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

// Round-trips the client SCRAM state machine (ScramClientSession) against
// SereneDB's own server-side SCRAM crypto (BuildScramVerifier /
// VerifyClientProof / ScramServerSignature). If the client half agrees with the
// server half, a real publisher (postgres, same algorithm) will accept it too.

#include <absl/strings/escaping.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "network/credentials.h"
#include "network/pg/scram_client.h"

namespace sdb::network::pg {
namespace {

// Minimal server side: given the client-first-bare and a verifier, produce the
// server-first + server-final a real server would send, and check the proof.
struct FakeServer {
  ScramVerifier verifier;
  std::string server_nonce = "serverPART9999";

  std::string BuildServerFirst(std::string_view client_first_bare) const {
    auto client_nonce =
      client_first_bare.substr(client_first_bare.find("r=") + 2);
    std::string full_nonce = std::string{client_nonce} + server_nonce;
    std::string salt_b64 = absl::Base64Escape(verifier.salt);
    return "r=" + full_nonce + ",s=" + salt_b64 +
           ",i=" + std::to_string(verifier.iterations);
  }

  // Returns the server-final ("v=...") if the proof is valid, else empty.
  std::string VerifyAndFinal(std::string_view client_first_bare,
                             std::string_view server_first,
                             std::string_view client_final) const {
    auto ppos = client_final.find(",p=");
    std::string client_final_no_proof{client_final.substr(0, ppos)};
    std::string proof_b64{client_final.substr(ppos + 3)};
    auto proof = Base64Decode(proof_b64);
    if (!proof) {
      return {};
    }
    std::string auth_message = std::string{client_first_bare} + "," +
                               std::string{server_first} + "," +
                               client_final_no_proof;
    if (!VerifyClientProof(verifier, auth_message, *proof)) {
      return {};
    }
    auto sig = ScramServerSignature(verifier, auth_message);
    return "v=" + absl::Base64Escape(std::string_view{
                    reinterpret_cast<const char*>(sig.data()), sig.size()});
  }
};

TEST(ScramClient, RoundTripSucceeds) {
  auto verifier = BuildScramVerifier("s3cr3t-pw");
  ASSERT_TRUE(verifier.has_value());
  FakeServer server{*verifier};

  ScramClientSession client{"s3cr3t-pw"};
  const std::string client_first = client.ClientFirst();
  ASSERT_TRUE(client_first.starts_with("n,,n=,r="));
  std::string_view client_first_bare = std::string_view{client_first}.substr(3);

  const std::string server_first = server.BuildServerFirst(client_first_bare);
  auto client_final = client.ServerFirst(server_first);
  ASSERT_TRUE(client_final.has_value());

  const std::string server_final =
    server.VerifyAndFinal(client_first_bare, server_first, *client_final);
  ASSERT_FALSE(server_final.empty()) << "server rejected the client proof";

  EXPECT_TRUE(client.ServerFinal(server_final));
}

TEST(ScramClient, WrongPasswordRejected) {
  auto verifier = BuildScramVerifier("correct-pw");
  ASSERT_TRUE(verifier.has_value());
  FakeServer server{*verifier};

  ScramClientSession client{"wrong-pw"};
  const std::string client_first = client.ClientFirst();
  std::string_view client_first_bare = std::string_view{client_first}.substr(3);
  const std::string server_first = server.BuildServerFirst(client_first_bare);
  auto client_final = client.ServerFirst(server_first);
  ASSERT_TRUE(client_final.has_value());

  // A wrong password yields a wrong proof, so the server rejects it.
  EXPECT_TRUE(
    server.VerifyAndFinal(client_first_bare, server_first, *client_final)
      .empty());
}

TEST(ScramClient, MalformedServerFirstRejected) {
  ScramClientSession client{"pw"};
  client.ClientFirst();
  EXPECT_FALSE(client.ServerFirst("garbage").has_value());
  EXPECT_FALSE(client.ServerFirst("r=x,s=notbase64!!,i=4096").has_value());
}

}  // namespace
}  // namespace sdb::network::pg
