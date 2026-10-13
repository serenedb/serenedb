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
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "replication/conninfo.h"

using namespace sdb::replication;

namespace {

class ConnInfoTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _dir = std::filesystem::temp_directory_path() /
           ("sdb_conninfo_" + std::to_string(::getpid()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::create_directories(_dir);
    std::vector<std::string> names;
    for (char** env = environ; *env != nullptr; ++env) {
      const std::string_view entry{*env};
      if (entry.starts_with("PG")) {
        names.emplace_back(entry.substr(0, entry.find('=')));
      }
    }
    for (const auto& name : names) {
      ::unsetenv(name.c_str());
    }
    ::setenv("HOME", _dir.c_str(), 1);
    ::setenv("PGSYSCONFDIR", (_dir / "sysconf").c_str(), 1);
  }

  void TearDown() override { std::filesystem::remove_all(_dir); }

  std::string Write(std::string_view name, std::string_view text,
                    mode_t mode = 0600) {
    const auto path = _dir / name;
    std::filesystem::create_directories(path.parent_path());
    std::ofstream{path} << text;
    ::chmod(path.c_str(), mode);
    return path.string();
  }

  std::filesystem::path _dir;
};

TEST_F(ConnInfoTest, DefaultsToTheUnixSocketDirectory) {
  const auto info = ParseConnInfo("dbname=db user=u");
  ASSERT_EQ(info.hosts.size(), 1);
  EXPECT_EQ(info.hosts[0].host, "/tmp");
  EXPECT_EQ(info.hosts[0].port, "5432");
  EXPECT_TRUE(info.hosts[0].IsUnixSocket());
  EXPECT_EQ(info.target_session_attrs, SessionAttrs::Any);
  EXPECT_FALSE(info.load_balance_hosts);
}

TEST_F(ConnInfoTest, UnixAndTcpHosts) {
  const auto info =
    ParseConnInfo("host=/var/run/postgresql,@abstract,db.example port=1,2,3");
  ASSERT_EQ(info.hosts.size(), 3);
  EXPECT_TRUE(info.hosts[0].IsUnixSocket());
  EXPECT_TRUE(info.hosts[1].IsUnixSocket());
  EXPECT_FALSE(info.hosts[2].IsUnixSocket());
  EXPECT_EQ(info.hosts[2].port, "3");
  const auto by_address = ParseConnInfo("host=/tmp hostaddr=127.0.0.1");
  EXPECT_FALSE(by_address.hosts[0].IsUnixSocket());
}

TEST_F(ConnInfoTest, SessionAttrsAndLoadBalance) {
  EXPECT_EQ(
    ParseConnInfo("target_session_attrs=read-write").target_session_attrs,
    SessionAttrs::ReadWrite);
  EXPECT_EQ(
    ParseConnInfo("target_session_attrs=read-only").target_session_attrs,
    SessionAttrs::ReadOnly);
  EXPECT_EQ(ParseConnInfo("target_session_attrs=primary").target_session_attrs,
            SessionAttrs::Primary);
  EXPECT_EQ(ParseConnInfo("target_session_attrs=standby").target_session_attrs,
            SessionAttrs::Standby);
  EXPECT_EQ(
    ParseConnInfo("target_session_attrs=prefer-standby").target_session_attrs,
    SessionAttrs::PreferStandby);
  EXPECT_THROW(ParseConnInfo("target_session_attrs=master"), irs::SqlException);
  EXPECT_TRUE(ParseConnInfo("load_balance_hosts=random").load_balance_hosts);
  EXPECT_FALSE(ParseConnInfo("load_balance_hosts=disable").load_balance_hosts);
  EXPECT_THROW(ParseConnInfo("load_balance_hosts=sometimes"),
               irs::SqlException);
}

TEST_F(ConnInfoTest, ArrangeHosts) {
  auto info = ParseConnInfo(
    "host=a,b,c,d,e,f,g,h load_balance_hosts=random "
    "target_session_attrs=prefer-standby");
  auto first = info;
  ArrangeHosts(first, 1, false);
  auto again = info;
  ArrangeHosts(again, 1, false);
  EXPECT_EQ(first.hosts, again.hosts);
  EXPECT_TRUE(std::is_permutation(first.hosts.begin(), first.hosts.end(),
                                  info.hosts.begin()));
  EXPECT_EQ(first.target_session_attrs, SessionAttrs::PreferStandby);
  auto fallback = info;
  ArrangeHosts(fallback, 1, true);
  EXPECT_EQ(fallback.target_session_attrs, SessionAttrs::Any);
  auto ordered = ParseConnInfo("host=a,b,c");
  ArrangeHosts(ordered, 7, false);
  EXPECT_EQ(ordered.hosts[0].host, "a");
  EXPECT_EQ(ordered.hosts[2].host, "c");
}

TEST_F(ConnInfoTest, SslOptions) {
  const auto info = ParseConnInfo(
    "sslmode=verify-ca sslcrl=/c.crl sslcrldir=/crls sslpassword=secret");
  EXPECT_EQ(info.sslcrl, "/c.crl");
  EXPECT_EQ(info.sslcrldir, "/crls");
  EXPECT_EQ(info.sslpassword, "secret");
  EXPECT_TRUE(ParseConnInfo("").sslcrl.empty());
  const auto crl = Write(".postgresql/root.crl", "");
  EXPECT_EQ(ParseConnInfo("").sslcrl, crl);
}

TEST_F(ConnInfoTest, ServiceFromHomeFile) {
  Write(".pg_service.conf",
        "# comment\n[other]\nhost=wrong\n\n[mydb]\n  host=svc.example  \n"
        "port=6543\nuser=svc_user\n[after]\nhost=wrong\n");
  const auto info = ParseConnInfo("service=mydb user=explicit");
  EXPECT_EQ(info.hosts[0].host, "svc.example");
  EXPECT_EQ(info.hosts[0].port, "6543");
  EXPECT_EQ(info.user, "explicit");
}

TEST_F(ConnInfoTest, ServiceFromEnvironmentAndSystemFile) {
  Write("sysconf/pg_service.conf", "[sys]\ndbname=sysdb\n");
  ::setenv("PGSERVICE", "sys", 1);
  EXPECT_EQ(ParseConnInfo("").dbname, "sysdb");
  ::unsetenv("PGSERVICE");
  const auto file = Write("custom.conf", "[env]\ndbname=envdb\n");
  ::setenv("PGSERVICEFILE", file.c_str(), 1);
  EXPECT_EQ(ParseConnInfo("service=env").dbname, "envdb");
  EXPECT_EQ(ParseConnInfo("service=sys").dbname, "sysdb");
}

TEST_F(ConnInfoTest, ServiceErrors) {
  EXPECT_THROW(ParseConnInfo("service=missing"), irs::SqlException);
  Write(".pg_service.conf", "[bad]\nnot a setting\n");
  EXPECT_THROW(ParseConnInfo("service=bad"), irs::SqlException);
  Write(".pg_service.conf", "[nested]\nservice=other\n");
  EXPECT_THROW(ParseConnInfo("service=nested"), irs::SqlException);
  Write(".pg_service.conf", "[unknown]\nnosuchoption=1\n");
  EXPECT_THROW(ParseConnInfo("service=unknown"), irs::SqlException);
}

TEST_F(ConnInfoTest, PasswordFile) {
  Write(".pgpass",
        "# comment\n"
        "other:5432:db:u:wrong\n"
        "db.example:5432:db:u:first\\:part\n"
        "localhost:*:*:u:local\n"
        "*:6000:db:*:any\\\\host\n");
  auto info = ParseConnInfo("host=db.example dbname=db user=u");
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "first:part");
  info = ParseConnInfo("host=/tmp dbname=db user=u");
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "local");
  info = ParseConnInfo("host=/var/run/postgresql dbname=db user=u");
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "local");
  info = ParseConnInfo("host=x port=6000 dbname=db user=someone");
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "any\\host");
  info = ParseConnInfo("host=localhost user=u");
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "local");
}

TEST_F(ConnInfoTest, PasswordFileOptionAndPermissions) {
  const auto file = Write("custom.pgpass", "*:*:*:*:from_option\n");
  auto info = ParseConnInfo("host=h dbname=db user=u passfile=" + file);
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "from_option");
  ::setenv("PGPASSFILE", file.c_str(), 1);
  info = ParseConnInfo("host=h dbname=db user=u");
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "from_option");
  ::chmod(file.c_str(), 0644);
  EXPECT_EQ(PasswordFromFile(info, info.hosts[0]), "");
}

TEST_F(ConnInfoTest, EnvironmentDefaults) {
  ::setenv("PGHOST", "env.example", 1);
  ::setenv("PGPORT", "6544", 1);
  ::setenv("PGSSLMODE", "require", 1);
  ::setenv("PGUSER", "env_user", 1);
  auto info = ParseConnInfo("dbname=db");
  EXPECT_EQ(info.hosts[0].host, "env.example");
  EXPECT_EQ(info.hosts[0].port, "6544");
  EXPECT_EQ(info.sslmode, SslMode::Require);
  EXPECT_EQ(info.user, "env_user");
  info = ParseConnInfo("host=explicit port=1 sslmode=disable user=u");
  EXPECT_EQ(info.hosts[0].host, "explicit");
  EXPECT_EQ(info.hosts[0].port, "1");
  EXPECT_EQ(info.sslmode, SslMode::Disable);
  EXPECT_EQ(info.user, "u");
  Write(".pg_service.conf", "[svc]\nport=7000\n");
  EXPECT_EQ(ParseConnInfo("service=svc").hosts[0].port, "7000");
}

TEST_F(ConnInfoTest, Options) {
  EXPECT_EQ(ParseConnInfo("host=h options='-c work_mem=8MB'").options,
            "-c work_mem=8MB");
  ::setenv("PGOPTIONS", "-c geqo=off", 1);
  EXPECT_EQ(ParseConnInfo("host=h").options, "-c geqo=off");
  EXPECT_EQ(ParseConnInfo("host=h options=").options, "");
}

TEST_F(ConnInfoTest, SystemRootCertDefaultsToVerifyFull) {
  EXPECT_EQ(ParseConnInfo("host=h sslrootcert=system").sslmode,
            SslMode::VerifyFull);
  EXPECT_EQ(ParseConnInfo("host=h sslrootcert=system sslmode=require").sslmode,
            SslMode::Require);
  ::setenv("PGSSLMODE", "verify-ca", 1);
  EXPECT_EQ(ParseConnInfo("host=h sslrootcert=system").sslmode,
            SslMode::VerifyCa);
}

TEST_F(ConnInfoTest, EncryptionOrderFollowsSslMode) {
  const auto order = [](std::string_view conninfo) {
    const auto info = ParseConnInfo(conninfo);
    const auto span = EncryptionOrder(info, info.hosts[0]);
    return std::vector<Encryption>{span.begin(), span.end()};
  };
  using enum Encryption;
  EXPECT_EQ(order("host=h sslmode=disable"), std::vector{Plain});
  EXPECT_EQ(order("host=h sslmode=allow"), (std::vector{Plain, Tls}));
  EXPECT_EQ(order("host=h sslmode=prefer"), (std::vector{Tls, Plain}));
  EXPECT_EQ(order("host=h sslmode=require"), std::vector{Tls});
  EXPECT_EQ(order("host=h sslmode=verify-full"), std::vector{Tls});
  EXPECT_EQ(order("host=/tmp sslmode=require"), std::vector{Plain});
}

}  // namespace
