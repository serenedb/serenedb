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

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string_view>

#include "network/credentials.h"
#include "network/pg/scram_messages.h"

using namespace sdb::network::pg;

// --- md5 password verifier crypto ------------------------------------------

TEST(NetworkPgMd5, VerifierRecognitionAndBuild) {
  // md5<hex> = "md5" + md5(password + username); stored form for user eph_md5,
  // password 'md5pw'.
  const std::string want = "md5d8e52850c67d91f23e0f978386689553";
  EXPECT_TRUE(sdb::network::IsMd5Verifier(want));
  EXPECT_EQ(sdb::network::BuildMd5Verifier("eph_md5", "md5pw"), want);

  EXPECT_FALSE(sdb::network::IsMd5Verifier("plaintext"));
  EXPECT_FALSE(sdb::network::IsMd5Verifier("md5short"));
  EXPECT_FALSE(sdb::network::IsMd5Verifier(
    "md5zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"));  // non-hex
  EXPECT_FALSE(sdb::network::IsMd5Verifier(
    "SCRAM-SHA-256$4096:AAA$BBB:CCC"));  // a scram verifier is not md5
}

TEST(NetworkPgMd5, ResponseFromStoredMatchesFromPlaintext) {
  // The crux of md5 auth against rolpassword: the server's expected response
  // computed from the STORED verifier must equal the one computed from the
  // plaintext (which is what BuildMd5Password does, and what the client sends).
  const std::array<uint8_t, 4> salt{0x01, 0x02, 0x03, 0x04};
  const std::string stored = sdb::network::BuildMd5Verifier("eph_md5", "md5pw");
  EXPECT_EQ(sdb::network::BuildMd5Response(stored, salt),
            sdb::network::BuildMd5Password("eph_md5", "md5pw", salt));
}

TEST(NetworkPgMd5, VerifyCleartext) {
  const std::string stored = sdb::network::BuildMd5Verifier("eph_md5", "md5pw");
  EXPECT_TRUE(
    sdb::network::VerifyCleartextAgainstMd5(stored, "eph_md5", "md5pw"));
  EXPECT_FALSE(
    sdb::network::VerifyCleartextAgainstMd5(stored, "eph_md5", "wrong"));
  EXPECT_FALSE(  // right password, wrong user -> different hash
    sdb::network::VerifyCleartextAgainstMd5(stored, "other", "md5pw"));
}

TEST(NetworkPgScram, ClientFirstNoBinding) {
  const auto p = ParseScramClientFirst("n,,n=,r=clientnonce");
  ASSERT_TRUE(p.has_value());
  EXPECT_EQ(p->cbind_flag, "n");
  EXPECT_TRUE(p->authzid.empty());
  EXPECT_EQ(p->gs2_header, "n,,");
  EXPECT_EQ(p->client_first_bare, "n=,r=clientnonce");
}

TEST(NetworkPgScram, ClientFirstBindingDeclinedAndPlus) {
  const auto y = ParseScramClientFirst("y,,n=,r=abc");
  ASSERT_TRUE(y.has_value());
  EXPECT_EQ(y->cbind_flag, "y");
  EXPECT_EQ(y->gs2_header, "y,,");

  const auto plus = ParseScramClientFirst("p=tls-server-end-point,,n=,r=xyz");
  ASSERT_TRUE(plus.has_value());
  EXPECT_EQ(plus->cbind_flag, "p=tls-server-end-point");
  EXPECT_EQ(plus->gs2_header, "p=tls-server-end-point,,");
  EXPECT_EQ(plus->client_first_bare, "n=,r=xyz");
}

TEST(NetworkPgScram, ClientFirstAuthzidIsExposed) {
  const auto p = ParseScramClientFirst("n,a=admin,n=,r=abc");
  ASSERT_TRUE(p.has_value());
  EXPECT_EQ(p->authzid, "a=admin");  // caller rejects a non-empty authzid
  EXPECT_EQ(p->gs2_header, "n,a=admin,");
}

TEST(NetworkPgScram, ClientFirstRejectsMissingSecondComma) {
  EXPECT_FALSE(ParseScramClientFirst("n").has_value());
  EXPECT_FALSE(ParseScramClientFirst("n,").has_value());
  EXPECT_FALSE(ParseScramClientFirst("n,onlyonecomma").has_value());
}

TEST(NetworkPgScram, ClientFinalWellFormed) {
  const auto p = ParseScramClientFinal("c=biws,r=clientserver,p=cHJvb2Y=");
  ASSERT_TRUE(p.has_value());
  EXPECT_EQ(p->channel_binding, "biws");
  EXPECT_EQ(p->nonce, "clientserver");
  EXPECT_EQ(p->proof, "cHJvb2Y=");
  EXPECT_EQ(p->without_proof, "c=biws,r=clientserver");
}

TEST(NetworkPgScram, ClientFinalRejectsMalformed) {
  EXPECT_FALSE(ParseScramClientFinal("c=biws,r=xyz").has_value());    // no ,p=
  EXPECT_FALSE(ParseScramClientFinal("x=biws,r=y,p=z").has_value());  // no c=
  EXPECT_FALSE(ParseScramClientFinal("c=biws,p=proof").has_value());  // no ,r=
}
