////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "catalog/object.h"
#include "catalog/options.h"

namespace duckdb {

class Serializer;
class Deserializer;
class ClientContext;
class Connection;

}  // namespace duckdb
namespace sdb::catalog {

class ForeignServer : public Object {
 public:
  static std::shared_ptr<ForeignServer> Deserialize(duckdb::Deserializer& src,
                                                    ReadContext ctx);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  ForeignServer(Permissions perm, ObjectId schema_id, ObjectId id,
                std::string_view name, std::string fdw_name, Options options);

  std::string_view GetFdwName() const noexcept { return _fdw_name; }

  const Options& GetOptions() const noexcept { return _options; }

 private:
  std::string _fdw_name;
  Options _options;
};

// True when the FDW name maps to a connector storage type (clickhouse_fdw,
// postgres_fdw and their bare aliases).
bool IsSupportedFdw(std::string_view fdw_name);

// Registers the transient secret, runs the ATTACH on `conn`, drops the secret.
// nullopt = unsupported FDW (nothing run); "" = success; else the REDACTED
// error. Credentials come from the server's OPTIONS.
std::optional<std::string> RunForeignServerAttach(
  duckdb::Connection& conn, const ForeignServer& server,
  std::string_view alias = {});

// Best-effort DETACH of a server's live (instance-global) DuckDB attachment,
// on a fresh engine connection. Used by DROP SERVER and by the DROP SCHEMA /
// DROP DATABASE cascade sweeps -- the generic drop plan removes catalog state
// only, never the attachment. The attachment may legitimately be absent (boot
// replay skips a down remote), so errors are swallowed.
void DetachForeignServerAttachment(std::string_view server_name);

// Quote an SQL identifier with double quotes, doubling any embedded quote.
std::string QuoteSqlIdentifier(std::string_view name);

// Redact the value of any `password=`/`passwd=` option occurring in a
// connection string (or an error message that echoes one, as the postgres
// connector does). Handles both the bare `password=secret` and the DSN-quoted
// `password='se\'cret'` forms. Used before a connector's connect error is
// surfaced to the client or the log, so credentials in a failed ATTACH never
// leak.
std::string RedactConnstrSecrets(std::string_view text);

}  // namespace sdb::catalog
