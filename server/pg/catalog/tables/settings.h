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

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace sdb::pg {

struct Guc {
  std::string_view name;
  std::string_view setting;
  std::string_view unit;
  std::string_view category;
  std::string_view short_desc;
  std::string_view extra_desc;
  std::string_view context;
  std::string_view vartype;
  std::string_view min_val;
  std::string_view max_val;
  std::span<const std::string_view> enumvals;
  std::string (*configured)() = nullptr;
};

std::string MaxConnectionsSetting();

inline constexpr std::string_view kByteaOutputs[] = {"escape", "hex"};
inline constexpr std::string_view kIntervalStyles[] = {
  "postgres", "postgres_verbose", "sql_standard", "iso_8601"};
inline constexpr std::string_view kIsolationLevels[] = {"repeatable read",
                                                        "read committed"};
inline constexpr std::string_view kMessageLevels[] = {
  "debug5", "debug4", "debug3",  "debug2", "debug1",
  "log",    "notice", "warning", "error"};
inline constexpr std::string_view kPasswordEncryptions[] = {"md5",
                                                            "scram-sha-256"};
inline constexpr std::string_view kSynchronousCommits[] = {
  "local", "remote_write", "remote_apply", "on", "off"};
inline constexpr std::string_view kXmlOptions[] = {"content", "document"};

inline constexpr std::string_view kStatementBehavior =
  "Client Connection Defaults / Statement Behavior";
inline constexpr std::string_view kLocaleAndFormatting =
  "Client Connection Defaults / Locale and Formatting";
inline constexpr std::string_view kAuthentication =
  "Connections and Authentication / Authentication";
inline constexpr std::string_view kPresetOptions = "Preset Options";
inline constexpr std::string_view kMemory = "Resource Usage / Memory";
inline constexpr std::string_view kNoEffect =
  "Accepted for compatibility; it has no effect in SereneDB.";
inline constexpr std::string_view kNoMemoryEffect =
  "Accepted for compatibility; memory_limit bounds the memory of every "
  "operation.";
inline constexpr std::string_view kCLocale =
  "Accepted for compatibility; SereneDB formats in the C locale.";

inline constexpr Guc kGucs[] = {
  {.name = "application_name",
   .category = "Reporting and Logging / What to Log",
   .short_desc =
     "Sets the application name to be reported in statistics and logs.",
   .context = "user",
   .vartype = "string"},
  {.name = "bytea_output",
   .setting = "hex",
   .category = kStatementBehavior,
   .short_desc = "Sets the output format for bytea.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kByteaOutputs},
  {.name = "check_function_bodies",
   .setting = "on",
   .category = kStatementBehavior,
   .short_desc =
     "Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE.",
   .extra_desc = "SereneDB always checks them; turning this off has no effect.",
   .context = "user",
   .vartype = "bool"},
  {.name = "client_encoding",
   .setting = "UTF8",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the client's character set encoding.",
   .extra_desc = "UTF8 is the only encoding.",
   .context = "user",
   .vartype = "string"},
  {.name = "client_min_messages",
   .setting = "notice",
   .category = kStatementBehavior,
   .short_desc = "Sets the message levels that are sent to the client.",
   .extra_desc = "Accepted for compatibility; SereneDB sends every message.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kMessageLevels},
  {.name = "DateStyle",
   .setting = "ISO, MDY",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the display format for date and time values.",
   .extra_desc = "SereneDB always displays and reads dates in the ISO format.",
   .context = "user",
   .vartype = "string"},
  {.name = "default_table_access_method",
   .setting = "heap",
   .category = kStatementBehavior,
   .short_desc = "Sets the default table access method for new tables.",
   .extra_desc = kNoEffect,
   .context = "user",
   .vartype = "string"},
  {.name = "default_tablespace",
   .category = kStatementBehavior,
   .short_desc = "Sets the default tablespace to create tables and indexes in.",
   .extra_desc = "Accepted for compatibility; SereneDB has no tablespaces.",
   .context = "user",
   .vartype = "string"},
  {.name = "default_transaction_isolation",
   .setting = "repeatable read",
   .category = kStatementBehavior,
   .short_desc =
     "Sets the transaction isolation level of each new transaction.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kIsolationLevels},
  {.name = "default_transaction_read_only",
   .setting = "off",
   .category = kStatementBehavior,
   .short_desc = "Sets the default read-only status of new transactions.",
   .context = "user",
   .vartype = "bool"},
  {.name = "extra_float_digits",
   .setting = "1",
   .category = kLocaleAndFormatting,
   .short_desc =
     "Sets the number of digits displayed for floating-point values.",
   .context = "user",
   .vartype = "integer",
   .min_val = "-15",
   .max_val = "3"},
  {.name = "idle_in_transaction_session_timeout",
   .setting = "0",
   .unit = "ms",
   .category = kStatementBehavior,
   .short_desc = "Sets the maximum allowed idle time between queries, when "
                 "in a transaction.",
   .extra_desc =
     "Accepted for compatibility; SereneDB does not end idle transactions.",
   .context = "user",
   .vartype = "integer",
   .min_val = "0",
   .max_val = "2147483647"},
  {.name = "in_hot_standby",
   .setting = "off",
   .category = kPresetOptions,
   .short_desc = "Shows whether hot standby is currently active.",
   .context = "internal",
   .vartype = "bool"},
  {.name = "integer_datetimes",
   .setting = "on",
   .category = kPresetOptions,
   .short_desc = "Shows whether datetimes are integer based.",
   .context = "internal",
   .vartype = "bool"},
  {.name = "IntervalStyle",
   .setting = "postgres",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the display format for interval values.",
   .extra_desc = "SereneDB always displays intervals in the postgres style.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kIntervalStyles},
  {.name = "lc_messages",
   .setting = "C",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the language in which messages are displayed.",
   .extra_desc = "Accepted for compatibility; messages are in English.",
   .context = "superuser",
   .vartype = "string"},
  {.name = "lc_monetary",
   .setting = "C",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the locale for formatting monetary amounts.",
   .extra_desc = kCLocale,
   .context = "user",
   .vartype = "string"},
  {.name = "lc_numeric",
   .setting = "C",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the locale for formatting numbers.",
   .extra_desc = kCLocale,
   .context = "user",
   .vartype = "string"},
  {.name = "lc_time",
   .setting = "C",
   .category = kLocaleAndFormatting,
   .short_desc = "Sets the locale for formatting date and time values.",
   .extra_desc = kCLocale,
   .context = "user",
   .vartype = "string"},
  {.name = "lock_timeout",
   .setting = "0",
   .unit = "ms",
   .category = kStatementBehavior,
   .short_desc = "Sets the maximum allowed duration of any wait for a lock.",
   .extra_desc = "Accepted for compatibility; SereneDB never waits for a lock, "
                 "a conflicting write fails at once.",
   .context = "user",
   .vartype = "integer",
   .min_val = "0",
   .max_val = "2147483647"},
  {.name = "maintenance_work_mem",
   .setting = "64MB",
   .unit = "kB",
   .category = kMemory,
   .short_desc =
     "Sets the maximum memory to be used for maintenance operations.",
   .extra_desc = kNoMemoryEffect,
   .context = "user",
   .vartype = "integer",
   .min_val = "1024",
   .max_val = "2147483647"},
  {.name = "max_connections",
   .category = "Connections and Authentication / Connection Settings",
   .short_desc = "Sets the maximum number of concurrent client connections.",
   .extra_desc = "Set with --max_connections; 0 means no limit. A listener may "
                 "override it with ?max_connections=.",
   .context = "postmaster",
   .vartype = "integer",
   .min_val = "0",
   .max_val = "4294967295",
   .configured = &MaxConnectionsSetting},
  {.name = "max_identifier_length",
   .setting = "63",
   .category = kPresetOptions,
   .short_desc = "Shows the maximum identifier length.",
   .extra_desc = "SereneDB keeps longer identifiers in full.",
   .context = "internal",
   .vartype = "integer",
   .min_val = "63",
   .max_val = "63"},
  {.name = "max_prepared_transactions",
   .setting = "0",
   .category = kPresetOptions,
   .short_desc =
     "Shows the maximum number of simultaneously prepared transactions.",
   .extra_desc = "SereneDB has no PREPARE TRANSACTION.",
   .context = "internal",
   .vartype = "integer",
   .min_val = "0",
   .max_val = "0"},
  {.name = "password_encryption",
   .setting = "scram-sha-256",
   .category = kAuthentication,
   .short_desc = "Chooses the algorithm for encrypting passwords.",
   .extra_desc = "SereneDB stores a password given in plain text as "
                 "SCRAM-SHA-256; clients read this setting to choose how they "
                 "encrypt one themselves.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kPasswordEncryptions},
  {.name = "row_security",
   .setting = "on",
   .category = kStatementBehavior,
   .short_desc = "Enables row security.",
   .extra_desc =
     "Accepted for compatibility; SereneDB has no row security policies.",
   .context = "user",
   .vartype = "bool"},
  {.name = "scram_iterations",
   .setting = "4096",
   .category = kAuthentication,
   .short_desc = "Sets the iteration count for SCRAM secret generation.",
   .extra_desc = "SereneDB generates its own secrets with 4096 iterations; "
                 "clients read this setting for the secrets they generate.",
   .context = "user",
   .vartype = "integer",
   .min_val = "1",
   .max_val = "2147483647"},
  {.name = "search_path",
   .setting = "\"$user\", public",
   .category = kStatementBehavior,
   .short_desc = "Sets the schema search order for names that are not "
                 "schema-qualified.",
   .context = "user",
   .vartype = "string"},
  {.name = "server_encoding",
   .setting = "UTF8",
   .category = kPresetOptions,
   .short_desc = "Shows the server (database) character set encoding.",
   .context = "internal",
   .vartype = "string"},
  {.name = "server_version",
   .setting = "18.3",
   .category = kPresetOptions,
   .short_desc = "Shows the PostgreSQL version SereneDB is compatible with.",
   .context = "internal",
   .vartype = "string"},
  {.name = "server_version_num",
   .setting = "180003",
   .category = kPresetOptions,
   .short_desc = "Shows the PostgreSQL version SereneDB is compatible with, "
                 "as an integer.",
   .context = "internal",
   .vartype = "integer",
   .min_val = "180003",
   .max_val = "180003"},
  {.name = "standard_conforming_strings",
   .setting = "on",
   .category =
     "Version and Platform Compatibility / Previous PostgreSQL Versions",
   .short_desc = "Causes '...' strings to treat backslashes literally.",
   .context = "user",
   .vartype = "bool"},
  {.name = "statement_timeout",
   .setting = "0",
   .unit = "ms",
   .category = kStatementBehavior,
   .short_desc = "Sets the maximum allowed duration of any statement.",
   .extra_desc = "0 disables the timeout.",
   .context = "user",
   .vartype = "integer",
   .min_val = "0",
   .max_val = "2147483647"},
  {.name = "synchronous_commit",
   .setting = "on",
   .category = "Write-Ahead Log / Settings",
   .short_desc = "Sets the current transaction's synchronization level.",
   .extra_desc = kNoEffect,
   .context = "user",
   .vartype = "enum",
   .enumvals = kSynchronousCommits},
  {.name = "TimeZone",
   .setting = "Etc/UTC",
   .category = kLocaleAndFormatting,
   .short_desc =
     "Sets the time zone for displaying and interpreting time stamps.",
   .context = "user",
   .vartype = "string"},
  {.name = "transaction_deferrable",
   .setting = "off",
   .category = kStatementBehavior,
   .short_desc = "Whether to defer a read-only serializable transaction until "
                 "it can be executed with no possible serialization failures.",
   .extra_desc = kNoEffect,
   .context = "user",
   .vartype = "bool"},
  {.name = "transaction_isolation",
   .setting = "repeatable read",
   .category = kStatementBehavior,
   .short_desc = "Sets the current transaction's isolation level.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kIsolationLevels},
  {.name = "transaction_read_only",
   .setting = "off",
   .category = kStatementBehavior,
   .short_desc = "Sets the current transaction's read-only status.",
   .extra_desc = kNoEffect,
   .context = "user",
   .vartype = "bool"},
  {.name = "transaction_timeout",
   .setting = "0",
   .unit = "ms",
   .category = kStatementBehavior,
   .short_desc = "Sets the maximum allowed duration of any transaction within "
                 "a session (not a prepared transaction).",
   .extra_desc =
     "Accepted for compatibility; SereneDB does not time transactions out.",
   .context = "user",
   .vartype = "integer",
   .min_val = "0",
   .max_val = "2147483647"},
  {.name = "work_mem",
   .setting = "4MB",
   .unit = "kB",
   .category = kMemory,
   .short_desc = "Sets the maximum memory to be used for query workspaces.",
   .extra_desc = kNoMemoryEffect,
   .context = "user",
   .vartype = "integer",
   .min_val = "64",
   .max_val = "2147483647"},
  {.name = "xmloption",
   .setting = "content",
   .category = kStatementBehavior,
   .short_desc = "Sets whether XML data in implicit parsing and serialization "
                 "operations is to be considered as documents or content "
                 "fragments.",
   .extra_desc = "Accepted for compatibility; SereneDB has no xml type.",
   .context = "user",
   .vartype = "enum",
   .enumvals = kXmlOptions},
};

const Guc* FindGuc(std::string_view name);

int64_t CheckGucInteger(const Guc& guc, std::string_view text);
std::string GucIntegerText(const Guc& guc, int64_t value);
std::string NormalizeGucValue(const Guc& guc, std::string_view text);

}  // namespace sdb::pg
