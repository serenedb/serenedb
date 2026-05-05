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

#include "table_options.h"

#include <absl/algorithm/container.h>
#include <absl/strings/numbers.h>
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/slice.h>

#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <optional>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "app/name_validator.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/result.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sharding_strategy.h"
#include "catalog/table.h"
#include "catalog/types.h"
#include "general_server/server_options_feature.h"
#include "general_server/state.h"

namespace sdb::catalog {
namespace {

// NOLINTBEGIN
struct KeyGeneratorOptions {
  std::string_view type;
  std::optional<uint64_t> lastValue;
  std::optional<uint64_t> offset;
  std::optional<uint64_t> increment;
  bool allowUserKeys{true};

  static KeyGeneratorOptions autoIncrement() {
    return {
      .type = "autoincrement", .lastValue = 0, .offset = 0, .increment = 1};
  }
  static KeyGeneratorOptions uuid() { return {.type = "uuid"}; }
  static KeyGeneratorOptions traditional() {
    return {.type = "traditional", .lastValue = 0};
  }
  static KeyGeneratorOptions padded() {
    return {.type = "padded", .lastValue = 0};
  }
};

// NOLINTEND

std::string GenerateUniqueName(std::string_view prefix,
                               std::span<const std::string> column_names) {
  std::string candidate{prefix};
  size_t suffix = 0;
  bool found_unique = false;

  while (!found_unique) {
    found_unique = true;
    for (const auto& str : column_names) {
      if (str == candidate) [[unlikely]] {
        found_unique = false;
        candidate.erase(prefix.size());
        absl::StrAppend(&candidate, "_", ++suffix);
        break;
      }
    }
  }

  return candidate;
}

}  // namespace

std::string Column::GeneratePKName(std::span<const std::string> column_names) {
  return GenerateUniqueName("sdb_generated_pk", column_names);
}

std::string Column::GenerateScoreName(
  std::span<const std::string> column_names) {
  return GenerateUniqueName(Column::kScoreName, column_names);
}

std::optional<size_t> CheckConstraint::IsNotNull(
  std::span<const Column> columns) const noexcept {
  SDB_ASSERT(expr);
  if (!expr->HasExpr()) {
    return std::nullopt;
  }
  // NOT NULL stored as OPERATOR_IS_NOT_NULL(ColumnRefExpression(col_name)).
  auto& parsed = expr->GetExpr();
  if (parsed.GetExpressionType() !=
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
    return std::nullopt;
  }
  auto& op = parsed.Cast<duckdb::OperatorExpression>();
  if (op.children.size() != 1 || op.children[0]->GetExpressionType() !=
                                   duckdb::ExpressionType::COLUMN_REF) {
    return std::nullopt;
  }
  auto name =
    op.children[0]->Cast<duckdb::ColumnRefExpression>().GetColumnName();
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].name == name) {
      return i;
    }
  }
  return std::nullopt;
}

Result MakeTableOptions(CreateTableRequest&& request, ObjectId database_id,
                        CreateTableOptions& options,
                        uint32_t replication_factor, uint32_t write_concern,
                        bool enforce_replication_factor) {
  if (request.type != std::to_underlying(TableType::RocksDB)) {
    return {ERROR_BAD_PARAMETER, "Invalid collection type: ", request.type};
  }

  if (auto r = TableNameValidator::validateName(request.name); !r.ok()) {
    return r;
  }

  if (ServerState::instance()->IsSingle()) {
    // TODO(gnusi): don't fallback, check values
    if (!request.replicationFactor || *request.replicationFactor != 0) {
      request.replicationFactor = 1;
    }
    request.writeConcern = 1;
    request.numberOfShards = 1;
    request.distributeShardsLike = {};

    // if (request.replicationFactor) {
    //   if (*request.replicationFactor != 1) {
    //     return {
    //       ERROR_BAD_PARAMETER,
    //       "replicationFactor must be set to 1 for single-node deployment"};
    //   }
    // } else {
    //   request.replicationFactor = 1;
    // }

    // if (request.writeConcern) {
    //   if (request.writeConcern != 1) {
    //     return {ERROR_BAD_PARAMETER,
    //             "writeConcern must be set to 1 for single-node deployment"};
    //   }
    // } else {
    //   request.writeConcern = 1;
    // }

    // if (request.numberOfShards) {
    //   if (request.numberOfShards != 1) {
    //     return {ERROR_BAD_PARAMETER,
    //             "numberOfShards must be set to 1 for single-node
    //             deployment"};
    //   }
    // } else {
    //   request.numberOfShards = 1;
    // }

    // if (request.distributeShardsLike &&
    //     !request.distributeShardsLike->empty()) {
    //   return {ERROR_BAD_PARAMETER,
    //           "distributeShardsLike must be empty for single-node
    //           deployment"};
    // }
  } else if (request.distributeShardsLike) {
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    auto snapshot = catalog.GetCatalogSnapshot();
    auto leader = snapshot->GetTable(database_id, StaticStrings::kPublic,
                                     *request.distributeShardsLike);
    if (!leader) {
      return {ERROR_CLUSTER_UNKNOWN_DISTRIBUTESHARDSLIKE,
              "Collection not found: ", *request.distributeShardsLike};
    }

    if (leader->distributeShardsLike().isSet()) {
      auto new_leader = snapshot->GetObject(leader->distributeShardsLike());
      SDB_ENSURE(new_leader, ERROR_CLUSTER_CHAIN_OF_DISTRIBUTESHARDSLIKE,
                 "Collection not found: ", leader->distributeShardsLike());

      return {ERROR_CLUSTER_CHAIN_OF_DISTRIBUTESHARDSLIKE,
              "Cannot distribute shards like '",
              *request.distributeShardsLike,
              "' it is already distributed like '",
              new_leader->GetName(),
              "'"};
    }

    if (auto number_of_shards_leader = leader->numberOfShards();
        request.numberOfShards &&
        *request.numberOfShards != number_of_shards_leader) {
      return {ERROR_BAD_PARAMETER, "Cannot have a different numberOfShards ",
              *request.numberOfShards, ", than the leading collection ",
              number_of_shards_leader};
    } else {
      request.numberOfShards = number_of_shards_leader;
    }

    if (auto replication_factor_leader = leader->replicationFactor();
        request.replicationFactor) {
      if (*request.replicationFactor != replication_factor_leader) {
        return {ERROR_BAD_PARAMETER,
                "Cannot have a different replicationFactor ",
                *request.replicationFactor, ", than the leading collection ",
                replication_factor_leader};
      }
    } else {
      request.replicationFactor = replication_factor_leader;
    }

    if (request.shardingStrategy) {
      if (*request.shardingStrategy != leader->shardingStrategy().name()) {
        return {ERROR_BAD_PARAMETER,
                "Cannot have a different sharding strategy ",
                *request.shardingStrategy, ", than the leading collection ",
                leader->shardingStrategy().name()};
      }
    } else {
      request.shardingStrategy = leader->shardingStrategy().name();
    }

    if (!request.writeConcern) {
      request.writeConcern = leader->writeConcern();
    }

    if (request.shardKeys.size() != leader->shardKeys().size()) {
      return {ERROR_BAD_PARAMETER,
              "Cannot have a different number of shardKeys ",
              request.shardKeys.size(), ", than the leading collection ",
              leader->shardKeys().size()};
    }

    options.distributeShardsLike = leader->GetId();
  } else {
    if (request.shardingStrategy) {
      if (*request.shardingStrategy != "hash") {
        return {ERROR_BAD_PARAMETER, "Invalid sharding strategy ",
                *request.shardingStrategy};
      }
    } else {
      request.shardingStrategy = "hash";
    }

    if (!request.replicationFactor) {
      request.replicationFactor = replication_factor;
    }

    if (!request.numberOfShards) {
      request.numberOfShards = 1;
    }

    if (!request.writeConcern) {
      if (*request.replicationFactor == 0) {
        request.writeConcern = 0;
      } else {
        request.writeConcern = write_concern;
      }
    }
  }

  SDB_ASSERT(request.replicationFactor);
  SDB_ASSERT(request.writeConcern);
  SDB_ASSERT(request.numberOfShards);

  const auto& server_options = GetServerOptions();

  if (enforce_replication_factor) {
    if (auto max_replication_factor =
          server_options.cluster_max_replication_factor;
        max_replication_factor > 0 &&
        *request.replicationFactor > max_replication_factor) {
      return {ERROR_BAD_PARAMETER,
              "replicationFactor must not be higher than "
              "maximum allowed replicationFactor ",
              max_replication_factor};
    }

    if (auto min_replication_factor =
          server_options.cluster_min_replication_factor;
        *request.replicationFactor != 0 &&
        *request.replicationFactor < min_replication_factor) {
      return {ERROR_BAD_PARAMETER,
              "replicationFactor must not be lower than "
              "minimum allowed replicationFactor ",
              min_replication_factor};
    }
  }

  if (auto max_number_of_shards = server_options.cluster_max_number_of_shards;
      max_number_of_shards > 0 &&
      *request.numberOfShards > max_number_of_shards) {
    return {ERROR_CLUSTER_TOO_MANY_SHARDS,
            "Too many shards. maximum number of shards is ",
            max_number_of_shards};
  }

  if (*request.replicationFactor == 0) {
    if (*request.writeConcern > 1) {
      return {ERROR_BAD_PARAMETER,
              "For a satellite collection writeConcern must not be set"};
    }

    if (*request.numberOfShards != 1) {
      return {ERROR_BAD_PARAMETER,
              "A satellite collection can only have 1 shard"};
    }

    // TODO(gnusi): check and error?
    request.writeConcern = 0;
    request.numberOfShards = 1;
  } else if (*request.replicationFactor < *request.writeConcern) {
    return {ERROR_BAD_PARAMETER,
            "writeConcern must not be higher than replicationFactor"};
  } else if (*request.writeConcern == 0) {
    return {ERROR_BAD_PARAMETER,
            "writeConcern can be 0 only for satellite collections"};
  } else if (*request.numberOfShards == 0) {
    return {ERROR_BAD_PARAMETER, "numberOfShards cannot be 0"};
  }

  if (auto r = basics::SafeCall([&] {
        options.keyOptions = KeyGeneratorHelper::createKeyGenerator(
          *request.numberOfShards, request.keyOptions);
        return Result{};
      });
      !r.ok()) {
    return r;
  }
  if (auto r = [&] -> Result {
        if (!request.id.empty()) {
          uint64_t id;
          if (!absl::SimpleAtoi(request.id, &id)) {
            return {ERROR_BAD_PARAMETER, "Invalid collection id: ", request.id};
          }
          options.id = Identifier{id};
        } else {
          options.id = Identifier{catalog::NextId().id()};
        }
        return {};
      }();
      !r.ok()) {
    return r;
  }
  options.shardKeys = std::move(request.shardKeys);
  options.columns = std::move(request.columns);
  options.pkColumns = std::move(request.pkColumns);
  options.checkConstraints = std::move(request.checkConstraints);
  options.avoidServers = std::move(request.avoidServers);
  if (request.shardingStrategy) {
    options.shardingStrategy = std::move(*request.shardingStrategy);
  }
  options.name = request.name;
  if (!request.indexes.isNone()) {
    options.indexes = request.indexes;
  }
  options.numberOfShards = *request.numberOfShards;
  options.replicationFactor = *request.replicationFactor;
  options.writeConcern = *request.writeConcern;
  options.type = request.type;
  options.waitForSync = request.waitForSync;

  return {};
}

}  // namespace sdb::catalog
