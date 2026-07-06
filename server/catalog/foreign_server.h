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
#include <vector>

#include "catalog/object.h"

namespace duckdb {

class Serializer;
class Deserializer;
class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class ForeignServer : public Object {
 public:
  static std::shared_ptr<ForeignServer> Deserialize(duckdb::Deserializer& src,
                                                    ReadContext ctx);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  ForeignServer(ObjectId schema_id, ObjectId id, std::string_view name,
                std::string fdw_name, std::vector<std::string> option_keys,
                std::vector<std::string> option_values);

  std::string_view GetFdwName() const noexcept { return _fdw_name; }

  const std::vector<std::string>& GetOptionKeys() const noexcept {
    return _option_keys;
  }
  const std::vector<std::string>& GetOptionValues() const noexcept {
    return _option_values;
  }

 private:
  std::string _fdw_name;
  std::vector<std::string> _option_keys;
  std::vector<std::string> _option_values;
};

class UserMapping;

// The deterministic transient-secret name serenedb registers for a foreign
// server's ATTACH `alias` (sanitised to an identifier). Pass it to
// PrepareForeignServerAttach and DropForeignServerSecret.
std::string MakeForeignServerSecretName(std::string_view alias);

// Registers a TEMPORARY DuckDB secret named `secret_name` carrying the server's
// connection options merged with a PUBLIC user mapping's (its user/password
// win), keys canonicalised to the connector's secret parameters, and returns the
// `ATTACH '' AS "<alias>" (TYPE <storage>, SECRET <secret_name>)` statement that
// consumes it. Option values are stored as duckdb Values (no connstr quoting),
// so a password may contain spaces/quotes freely and never appears in SQL text.
// Maps the FDW name to the connector storage type (clickhouse_fdw -> clickhouse,
// postgres_fdw -> postgres); returns "" (registering nothing) for an unsupported
// FDW. `alias` defaults to the server name; a throwaway alias probes new
// credentials before detaching the live attachment. Shared by create-time attach
// and boot replay. Drop the secret with DropForeignServerSecret once the ATTACH
// has run -- the connector captures the resolved params at attach time.
std::string PrepareForeignServerAttach(
  duckdb::ClientContext& context, std::string_view secret_name,
  const ForeignServer& server, const UserMapping* public_mapping = nullptr,
  std::string_view alias = {});

// Drops a transient secret registered by PrepareForeignServerAttach; a missing
// secret is ignored (best-effort cleanup).
void DropForeignServerSecret(duckdb::ClientContext& context,
                             std::string_view secret_name);

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
