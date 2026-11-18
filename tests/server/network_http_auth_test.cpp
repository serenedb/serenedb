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

#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "network/credentials.h"
#include "network/http/auth.h"

namespace sdb::network::http {
namespace {

class NoCredentials final : public CredentialProvider {
 public:
  std::optional<Credential> LookupCredential(std::string_view) const final {
    return std::nullopt;
  }

  ScramVerifier MockScram(std::string_view) const final { return {}; }
};

class RegisteredBearer final : public BearerValidator {
 public:
  std::optional<AuthContext> Validate(std::string_view token) const final {
    if (token != "registered-token") {
      return std::nullopt;
    }
    return AuthContext{.user = "registered_role"};
  }
};

std::unique_ptr<BearerValidator> MakeRegisteredBearer() {
  return std::make_unique<RegisteredBearer>();
}

SDB_BEARER_VALIDATOR kRegisteredBearer{"auth-test", &MakeRegisteredBearer};

TEST(HttpAuthenticator, EveryBearerValidatorIsAsked) {
  std::vector<std::unique_ptr<BearerValidator>> validators;
  validators.push_back(
    std::make_unique<FlagBearerValidator>("flag-token", "flag_user"));
  validators.push_back(
    Registry<BearerValidatorFactory>::Find("auth-test")->make());
  const NoCredentials credentials;
  const HttpAuthenticator auth{&credentials, nullptr, validators};

  auto result = auth.Authenticate("Bearer flag-token", false);
  EXPECT_EQ(result.status, HttpStatus::None);
  EXPECT_EQ(result.context.user, "flag_user");

  result = auth.Authenticate("Bearer registered-token", false);
  EXPECT_EQ(result.status, HttpStatus::None);
  EXPECT_EQ(result.context.user, "registered_role");

  EXPECT_EQ(auth.Authenticate("Bearer other-token", false).status,
            HttpStatus::Unauthorized);

  const HttpAuthenticator without_validators{&credentials, nullptr, {}};
  EXPECT_EQ(without_validators.Authenticate("Bearer flag-token", false).status,
            HttpStatus::Unauthorized);
}

}  // namespace
}  // namespace sdb::network::http
