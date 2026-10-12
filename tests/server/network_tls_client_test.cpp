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
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>

#include "network/tls_context.h"

namespace {

using KeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using CertPtr = std::unique_ptr<X509, decltype(&X509_free)>;
using CrlPtr = std::unique_ptr<X509_CRL, decltype(&X509_CRL_free)>;

class TlsClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _dir = std::filesystem::temp_directory_path() /
           ("sdb_tls_client_" + std::to_string(::getpid()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::create_directories(_dir);
    _key.reset(EVP_RSA_gen(2048));
    ASSERT_NE(_key, nullptr);
    _cert.reset(X509_new());
    X509_set_version(_cert.get(), 2);
    ASN1_INTEGER_set(X509_get_serialNumber(_cert.get()), 1);
    X509_gmtime_adj(X509_getm_notBefore(_cert.get()), 0);
    X509_gmtime_adj(X509_getm_notAfter(_cert.get()), 3600);
    X509_set_pubkey(_cert.get(), _key.get());
    auto* name = X509_get_subject_name(_cert.get());
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>("ca"), -1,
                               -1, 0);
    X509_set_issuer_name(_cert.get(), name);
    ASSERT_GT(X509_sign(_cert.get(), _key.get(), EVP_sha256()), 0);
  }

  void TearDown() override { std::filesystem::remove_all(_dir); }

  template<typename Write>
  std::string WritePem(std::string_view file, Write write) {
    const auto path = (_dir / file).string();
    FILE* out = std::fopen(path.c_str(), "w");
    EXPECT_NE(out, nullptr);
    EXPECT_GT(write(out), 0);
    std::fclose(out);
    return path;
  }

  std::string Cert() {
    return WritePem(
      "cert.pem", [&](FILE* out) { return PEM_write_X509(out, _cert.get()); });
  }

  std::string EncryptedKey(const std::string& password) {
    return WritePem("key.pem", [&](FILE* out) {
      return PEM_write_PrivateKey(
        out, _key.get(), EVP_aes_256_cbc(),
        reinterpret_cast<const unsigned char*>(password.data()),
        static_cast<int>(password.size()), nullptr, nullptr);
    });
  }

  std::string Crl() {
    CrlPtr crl{X509_CRL_new(), &X509_CRL_free};
    X509_CRL_set_version(crl.get(), 1);
    X509_CRL_set_issuer_name(crl.get(), X509_get_subject_name(_cert.get()));
    std::unique_ptr<ASN1_TIME, decltype(&ASN1_TIME_free)> now{
      X509_gmtime_adj(nullptr, 0), &ASN1_TIME_free};
    X509_CRL_set1_lastUpdate(crl.get(), now.get());
    X509_CRL_sign(crl.get(), _key.get(), EVP_sha256());
    return WritePem("root.crl", [&](FILE* out) {
      return PEM_write_X509_CRL(out, crl.get());
    });
  }

  std::filesystem::path _dir;
  KeyPtr _key{nullptr, &EVP_PKEY_free};
  CertPtr _cert{nullptr, &X509_free};
};

TEST_F(TlsClientTest, EncryptedKeyNeedsItsPassword) {
  const auto cert = Cert();
  const auto key = EncryptedKey("open sesame");
  EXPECT_NO_THROW(sdb::network::BuildClientTlsContext(
    {.cert_file = cert, .key_file = key, .key_password = "open sesame"}));
  EXPECT_ANY_THROW(sdb::network::BuildClientTlsContext(
    {.cert_file = cert, .key_file = key, .key_password = "wrong"}));
}

TEST_F(TlsClientTest, RevocationListTurnsOnCrlChecks) {
  const auto root = Cert();
  const auto crl = Crl();
  auto checked = sdb::network::BuildClientTlsContext(
    {.verify_peer = true, .root_cert = root, .crl_file = crl});
  auto* store = SSL_CTX_get_cert_store(checked.native_handle());
  const auto flags = X509_VERIFY_PARAM_get_flags(X509_STORE_get0_param(store));
  EXPECT_NE(flags & X509_V_FLAG_CRL_CHECK, 0);
  EXPECT_NE(flags & X509_V_FLAG_CRL_CHECK_ALL, 0);
  auto by_dir = sdb::network::BuildClientTlsContext(
    {.verify_peer = true, .root_cert = root, .crl_dir = _dir.string()});
  EXPECT_NE(X509_VERIFY_PARAM_get_flags(X509_STORE_get0_param(
              SSL_CTX_get_cert_store(by_dir.native_handle()))) &
              X509_V_FLAG_CRL_CHECK,
            0);
  auto unchecked = sdb::network::BuildClientTlsContext(
    {.verify_peer = true, .root_cert = root});
  EXPECT_EQ(X509_VERIFY_PARAM_get_flags(X509_STORE_get0_param(
              SSL_CTX_get_cert_store(unchecked.native_handle()))) &
              X509_V_FLAG_CRL_CHECK,
            0);
}

}  // namespace
