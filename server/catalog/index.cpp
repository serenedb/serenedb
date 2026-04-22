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

#include "catalog/index.h"

#include <absl/strings/ascii.h>
#include <vpack/serializer.h>

#include <duckdb/common/exception.hpp>

#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "catalog/types.h"

namespace sdb::catalog {
namespace {

ResultOr<int64_t> GetIntOption(std::string_view column_name,
                               std::string_view key, const duckdb::Value& v) {
  try {
    return v.DefaultCastAs(duckdb::LogicalType::BIGINT).GetValue<int64_t>();
  } catch (const std::exception&) {
    return std::unexpected<Result>{std::in_place,
                                   ERROR_BAD_PARAMETER,
                                   "Column '",
                                   column_name,
                                   "': hnsw option '",
                                   key,
                                   "' must be an integer, got '",
                                   v.ToString(),
                                   "'"};
  }
}

ResultOr<std::string> GetStringOption(std::string_view column_name,
                                      std::string_view key,
                                      const duckdb::Value& v) {
  try {
    return v.DefaultCastAs(duckdb::LogicalType::VARCHAR)
      .GetValue<std::string>();
  } catch (const std::exception&) {
    return std::unexpected<Result>{std::in_place,
                                   ERROR_BAD_PARAMETER,
                                   "Column '",
                                   column_name,
                                   "': hnsw option '",
                                   key,
                                   "' must be a string, got '",
                                   v.ToString(),
                                   "'"};
  }
}

Result ApplyHNSWOptions(
  std::string_view column_name,
  const duckdb::case_insensitive_map_t<duckdb::Value>& opts,
  HNSWColumnConfig& cfg) {
  for (const auto& [key, raw_val] : opts) {
    if (key == "metric") {
      auto str = GetStringOption(column_name, key, raw_val);
      if (!str) {
        return std::move(str).error();
      }
      std::string v = std::move(*str);
      absl::AsciiStrToLower(&v);
      if (v == "l2") {
        cfg.metric = irs::HNSWMetric::L2;
      } else if (v == "l2sqr") {
        cfg.metric = irs::HNSWMetric::L2Sqr;
      } else if (v == "l1") {
        cfg.metric = irs::HNSWMetric::L1;
      } else if (v == "cosine") {
        cfg.metric = irs::HNSWMetric::Cosine;
      } else if (v == "ip" || v == "inner_product") {
        cfg.metric = irs::HNSWMetric::InnerProduct;
      } else {
        return {ERROR_BAD_PARAMETER,
                "Column '",
                column_name,
                "': unknown hnsw metric '",
                v,
                "'. Expected one of: l2, l2sqr, l1, cosine, ip"};
      }
    } else if (key == "m") {
      auto n = GetIntOption(column_name, key, raw_val);
      if (!n) {
        return std::move(n).error();
      }
      if (*n < 2 || *n > 128) {
        return {ERROR_BAD_PARAMETER, "Column '", column_name,
                "': hnsw option 'm' must be in [2, 128], got ", *n};
      }
      cfg.m = static_cast<int>(*n);
    } else if (key == "ef_construction") {
      auto n = GetIntOption(column_name, key, raw_val);
      if (!n) {
        return std::move(n).error();
      }
      if (*n < 1) {
        return {ERROR_BAD_PARAMETER, "Column '", column_name,
                "': hnsw option 'ef_construction' must be positive, got ", *n};
      }
      cfg.ef_construction = static_cast<int>(*n);
    } else {
      return {ERROR_BAD_PARAMETER,
              "Column '",
              column_name,
              "': unknown hnsw option '",
              key,
              "'. Accepted options: metric, m, ef_construction"};
    }
  }
  if (cfg.ef_construction < cfg.m) {
    return {ERROR_BAD_PARAMETER,
            "Column '",
            column_name,
            "': hnsw option 'ef_construction' (",
            cfg.ef_construction,
            ") must be >= 'm' (",
            cfg.m,
            ")"};
  }
  return {};
}

Result ValidateInvertedIndexColumns(
  std::span<CreateIndexColumn> indexed_columns) {
  for (auto c : indexed_columns) {
    SDB_ASSERT(c.catalog_column);
    if (c.catalog_column->type.id() == duckdb::LogicalTypeId::TIMESTAMP ||
        c.catalog_column->type.id() == duckdb::LogicalTypeId::HUGEINT) {
      return {ERROR_BAD_PARAMETER, "Column ", c.name,
              " has unsupported kind and can not be indexed"};
    }
  }
  return {};
}

}  // namespace
namespace {

std::vector<Column::Id> ExtractColumnIds(
  std::span<const CreateIndexColumn> columns) {
  std::vector<Column::Id> ids;
  ids.reserve(columns.size());
  for (const auto& c : columns) {
    SDB_ASSERT(c.catalog_column);
    ids.push_back(c.catalog_column->id);
  }
  return ids;
}

}  // namespace

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique) {
  for (const auto& c : columns) {
    SDB_ASSERT(c.catalog_column);
    // if (c.catalog_column->type->providesCustomComparison()) {
    //   return std::unexpected<Result>{
    //     std::in_place, ERROR_BAD_PARAMETER, "Column ", c.name,
    //     " has type with custom comparison and can not be indexed"};
    // }
    // if (!c.catalog_column->type->isPrimitiveType()) {
    //   return std::unexpected<Result>{
    //     std::in_place, ERROR_BAD_PARAMETER, "Column ", c.name,
    //     " has non primitive type and can not be indexed"};
    // }
  }
  return std::make_shared<SecondaryIndex>(database_id, schema_id, id,
                                          relation_id, std::move(name),
                                          ExtractColumnIds(columns), unique);
}

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot) {
  auto column_validation_res = ValidateInvertedIndexColumns(columns);
  if (column_validation_res.fail()) {
    return std::unexpected<Result>(std::move(column_validation_res));
  }

  InvertedIndex::ColumnOptions inverted_columns;
  for (const auto& c : columns) {
    InvertedIndexColumnInfo index_col;
    if (!c.opclass.empty()) {
      // "hnsw" is a built-in opclass for vector (ARRAY(FLOAT, N)) columns.
      if (c.opclass == "hnsw") {
        const auto& col_type = c.catalog_column->type;
        if (col_type.id() != duckdb::LogicalTypeId::ARRAY) {
          return std::unexpected<Result>{
            std::in_place, ERROR_BAD_PARAMETER, "Column '", c.name,
            "' must be an ARRAY type to use the 'hnsw' opclass"};
        }
        const auto& child_type = duckdb::ArrayType::GetChildType(col_type);
        if (child_type.id() != duckdb::LogicalTypeId::FLOAT) {
          return std::unexpected<Result>{
            std::in_place, ERROR_BAD_PARAMETER, "Column '", c.name,
            "' must be ARRAY(FLOAT, N) to use the 'hnsw' opclass"};
        }
        HNSWColumnConfig cfg{
          .d = static_cast<int>(duckdb::ArrayType::GetSize(col_type)),
        };
        if (auto r = ApplyHNSWOptions(c.name, c.opclass_options, cfg);
            r.fail()) {
          return std::unexpected<Result>(std::move(r));
        }
        index_col.hnsw_config = cfg;
      } else {
        if (!c.opclass_options.empty()) {
          return std::unexpected<Result>{
            std::in_place,
            ERROR_BAD_PARAMETER,
            "Opclass '",
            c.opclass,
            "' does not accept options (used on column '",
            c.name,
            "')"};
        }
        auto object_name = pg::ParseObjectName(c.opclass, schema_name);
        if (object_name.schema != schema_name) {
          // Technically nothing prevents us from allowing so.
          // But that will make schema drop more complicated as we will need to
          // check if any dictionaries are used in the indexes from other
          // schemas and even fail schema drops on this case. For now if we
          // drop text dictionary as a child entity we can be sure that
          // indexes will also be dropped along with tables from same schema.
          return std::unexpected<Result>{
            std::in_place, ERROR_BAD_PARAMETER,
            "Accessing text dictionary from different schema is not supported"};
        }
        auto dict = snapshot->GetTokenizer(database_id, object_name.schema,
                                           object_name.relation);
        if (!dict) {
          return std::unexpected<Result>{std::in_place,
                                         ERROR_BAD_PARAMETER,
                                         "Text search dictionary '",
                                         c.opclass,
                                         "' does not exist.",
                                         " Required by column '",
                                         c.name,
                                         "'"};
        }
        index_col.text_dictionary = dict->GetId();
        index_col.features = dict->GetFeatures();
      }
    }
    inverted_columns.emplace(c.catalog_column->id, std::move(index_col));
  }
  return std::make_shared<InvertedIndex>(
    database_id, schema_id, id, relation_id, std::move(name),
    ExtractColumnIds(columns), std::move(inverted_columns));
}

Index::Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
             ObjectId relation_id, std::string name,
             std::vector<Column::Id> column_ids, ObjectType type)
  : SchemaObject{{}, database_id, schema_id, id, std::move(name), type},
    _relation_id{relation_id},
    _column_ids{std::move(column_ids)} {
  SDB_ASSERT(GetId().isSet());
}

}  // namespace sdb::catalog
