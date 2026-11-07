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

#include <atomic>
#include <duckdb/common/enums/set_scope.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/setting_info.hpp>
#include <duckdb/parser/parsed_data/transaction_info.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <magic_enum/magic_enum.hpp>
#include <span>
#include <string>
#include <string_view>

namespace duckdb {

class ClientContext;
struct DBConfig;

}  // namespace duckdb
namespace sdb {

enum class ByteaOutput : uint8_t {
  Hex,
  Escape,
};

using IsolationLevel = duckdb::TransactionIsolationLevel;

uint32_t GetIntSetting(duckdb::ClientContext& context, std::string_view name);

int64_t GetSignedIntSetting(duckdb::ClientContext& context,
                            std::string_view name);

double GetDoubleSetting(duckdb::ClientContext& context, std::string_view name);

bool GetBoolSetting(duckdb::ClientContext& context, std::string_view name);

uint32_t GetEnumSetting(duckdb::ClientContext& context, std::string_view name,
                        std::span<const std::string_view> options);

duckdb::Value GetGlobalSetting(const duckdb::DBConfig& config,
                               std::string_view name);

struct VariableDescription {
  duckdb::LogicalTypeId type;
  std::string_view description;
  duckdb::Value (*default_value)() = nullptr;  // called at registration time
  duckdb::set_option_callback_t set_callback = nullptr;
  duckdb::reset_option_callback_t reset_callback = nullptr;
  duckdb::SetScope scope = duckdb::SetScope::AUTOMATIC;
};

std::string_view GetOriginalName(std::string_view name);

int64_t ParseDurationMillis(std::string_view name, std::string_view text,
                            double unitless_ms);

bool IsUnchangeableSetting(std::string_view name);

bool IsNonTransactionalSetting(std::string_view name);

void RequireSuperuserToSetGlobal(duckdb::ClientContext& ctx,
                                 std::string_view name);

namespace connector {

void RegisterConfigVariables(duckdb::DBConfig& config);

duckdb::Value ValidateSetting(duckdb::ClientContext& context,
                              std::string_view name,
                              const duckdb::Value& value);

}  // namespace connector
}  // namespace sdb
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::IsolationLevel>(sdb::IsolationLevel value) noexcept {
  switch (value) {
    case sdb::IsolationLevel::READ_COMMITTED:
      return "read committed";
    case sdb::IsolationLevel::REPEATABLE_READ:
      return "repeatable read";
    default:
      break;
  }
  return default_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::ByteaOutput>(sdb::ByteaOutput value) noexcept {
  switch (value) {
    case sdb::ByteaOutput::Hex:
      return "hex";
    case sdb::ByteaOutput::Escape:
      return "escape";
  }
  return default_tag;
}

}  // namespace magic_enum
