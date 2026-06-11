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

// Golden-fixture test for the on-disk catalog format (catalog/persistence/*).
// Each fixture in resources/tests/catalog/ holds the exact bytes a known sample
// serializes to. The test asserts that (1) the current code still produces
// those bytes and (2) the recorded bytes still deserialize and re-serialize
// identically -- so any change to a persistent struct's layout is caught.
//
// Regenerate fixtures after an intended format change:
//   SDB_REGEN_FIXTURES=1 ./build/bin/serenedb-tests_basics \
//     --gtest_filter='CatalogPersistence.*'

#include <gtest/gtest.h>

#include <cstdlib>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "basics/serializer.h"
#include "catalog/persistence/database.h"
#include "catalog/persistence/index.h"
#include "catalog/persistence/inverted_index.h"
#include "catalog/persistence/role.h"
#include "catalog/persistence/schema.h"
#include "catalog/persistence/scorer_options.h"
#include "catalog/persistence/secondary_index.h"
#include "catalog/persistence/sequence.h"
#include "catalog/persistence/table.h"
#include "catalog/persistence/table_stats.h"
#include "catalog/persistence/tokenizer.h"

namespace sdb::catalog::persistence {
namespace {

namespace fs = std::filesystem;

fs::path FixturePath(std::string_view name) {
  return fs::path{SDB_RESOURCE_DIR} / "tests" / "catalog" / name;
}

template<typename T>
std::string Serialize(const T& value) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  basics::WriteTuple(serializer, value);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

template<typename T>
T Deserialize(std::string_view bytes) {
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  T out{};
  basics::ReadTuple(deserializer, out);
  return out;
}

template<typename T>
void CheckFixture(std::string_view name, const T& sample) {
  const fs::path path = FixturePath(name);
  const std::string bytes = Serialize(sample);

  if (std::getenv("SDB_REGEN_FIXTURES") != nullptr) {
    fs::create_directories(path.parent_path());
    std::ofstream out{path, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    GTEST_SKIP() << "regenerated fixture " << path;
  }

  std::ifstream in{path, std::ios::binary};
  ASSERT_TRUE(in.good()) << "missing fixture " << path
                         << " (run with SDB_REGEN_FIXTURES=1)";
  const std::string golden{std::istreambuf_iterator<char>{in},
                           std::istreambuf_iterator<char>{}};

  // (1) Current code still produces the recorded on-disk bytes.
  EXPECT_EQ(bytes, golden) << "on-disk format for " << name << " changed";
  // (2) The recorded bytes still deserialize, and re-serialize identically.
  EXPECT_EQ(Serialize(Deserialize<T>(golden)), golden)
    << "deserialization of " << name << " is not byte-stable";
}

TEST(CatalogPersistence, secondary_index) {
  CheckFixture("secondary_index.bin",
               SecondaryIndexData{
                 .name = "idx_demo",
                 .column_ids = {ObjectId{1}, ObjectId{2}, ObjectId{3}},
                 .unique = true,
               });
}

TEST(CatalogPersistence, table) {
  CheckFixture("table.bin", TableData{
                              .name = "t",
                              .columns = {Column{ObjectId{}, ObjectId{1}, "a",
                                                 duckdb::LogicalType::INTEGER},
                                          Column{ObjectId{}, ObjectId{2}, "b",
                                                 duckdb::LogicalType::VARCHAR}},
                              .pk_columns = {ObjectId{1}},
                              .check_constraints = {},
                              .generated_pk_seq_id = ObjectId{9},
                            });
}

TEST(CatalogPersistence, tokenizer) {
  CheckFixture("tokenizer.bin", TokenizerData{
                                  .name = "tok",
                                  .config = {},
                                  .features = search::Features{},
                                  .norm_row_group_size = 7,
                                });
}

// Every TokenizerConfig variant arm must serialize and re-serialize stably,
// so a change to any analyzer's Options layout is caught. Arms are iterated by
// variant index and default-constructed; a non-default-constructible arm is
// skipped (its layout is exercised by the analyzer integration tests instead).
using ConfigVariant = decltype(irs::analysis::TokenizerConfig{}.config);

template<std::size_t I>
void CheckTokenizerArm() {
  if constexpr (std::is_default_constructible_v<
                  std::variant_alternative_t<I, ConfigVariant>>) {
    irs::analysis::TokenizerConfig cfg;
    cfg.config.template emplace<I>();
    CheckFixture("tokenizer/config_" + std::to_string(I) + ".bin", cfg);
  }
}

template<std::size_t... Is>
void CheckTokenizerArms(std::index_sequence<Is...>) {
  (CheckTokenizerArm<Is>(), ...);
}

TEST(CatalogPersistence, tokenizer_configs) {
  CheckTokenizerArms(
    std::make_index_sequence<std::variant_size_v<ConfigVariant>>{});
}

TEST(CatalogPersistence, column_serialized) {
  CheckFixture(
    "column_serialized.bin",
    ColumnSerialized{
      .text_dictionary = ObjectId{5},
      .store_values = true,
      .compression = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED,
      .features = search::Features{},
      .hnsw_config = std::nullopt,
      .synthetic_column = irs::field_limits::invalid(),
      .row_group_size = 100,
      .norm_row_group_size = 50,
    });
}

TEST(CatalogPersistence, expression_serialized) {
  CheckFixture("expression_serialized.bin",
               ExpressionSerialized{
                 .serialized_expr = "expr-bytes",
                 .pretty_printed = "a + b",
                 .dependent_columns = {ObjectId{1}, ObjectId{2}},
                 .return_type = duckdb::LogicalType::DOUBLE,
                 .synthetic_column = irs::field_limits::invalid(),
                 .text_dictionary = ObjectId{3},
                 .field_id = 7,
                 .norm_row_group_size = 9,
                 .features = search::Features{},
               });
}

TEST(CatalogPersistence, inverted_index) {
  CheckFixture(
    "inverted_index.bin",
    InvertedIndexData{
      .name = "idx",
      .column_ids = {ObjectId{1}},
      .columns = {{ObjectId{1}, ColumnSerialized{.text_dictionary = ObjectId{5},
                                                 .row_group_size = 100}}},
      .expressions = {ExpressionSerialized{.serialized_expr = "e",
                                           .field_id = 7}},
      .options = InvertedIndexOptions{.row_group_size = 1024},
    });
}

TEST(CatalogPersistence, database_options) {
  CheckFixture("database_options.bin", DatabaseOptions{.name = "db"});
}

TEST(CatalogPersistence, schema_options) {
  CheckFixture("schema_options.bin",
               SchemaOptions{.id = ObjectId{4}, .name = "public"});
}

TEST(CatalogPersistence, sequence_options) {
  CheckFixture("sequence_options.bin", SequenceOptions{
                                         .name = "seq",
                                         .start_value = 10,
                                         .increment = 2,
                                         .min_value = 1,
                                         .max_value = 1000,
                                         .cache = 5,
                                         .owner_table_id = 3,
                                         .cycle = true,
                                       });
}

TEST(CatalogPersistence, role_data) {
  // Single-entry db_access: the map is unordered, so >1 entry would not
  // serialize to stable bytes.
  CheckFixture("role_data.bin", RoleData{
                                  .id = ObjectId{2},
                                  .name = "alice",
                                  .active = true,
                                  .password_method = "scram",
                                  .password_salt = "salt",
                                  .password_hash = "hash",
                                  .db_access = {{"db1", auth::Level::RW}},
                                });
}

TEST(CatalogPersistence, table_stats) {
  CheckFixture("table_stats.bin", TableStats{.num_rows = 42});
}

TEST(CatalogPersistence, inverted_index_options) {
  CheckFixture("inverted_index_options.bin", InvertedIndexOptions{
                                               .row_group_size = 1024,
                                               .norm_row_group_size = 512,
                                               .refresh_interval_ms = 100,
                                               .compaction_interval_ms = 200,
                                               .cleanup_interval_step = 3,
                                               .topk_scorer = std::nullopt,
                                             });
}

TEST(CatalogPersistence, expression_data) {
  CheckFixture("expression_data.bin",
               ExpressionData{
                 .serialized_expr = "expr",
                 .dependent_columns = {ObjectId{1}},
                 .return_type = duckdb::LogicalType::BIGINT,
                 .pretty_printed = "x",
               });
}

TEST(CatalogPersistence, hnsw_column_config) {
  CheckFixture("hnsw_column_config.bin", HNSWColumnConfig{
                                           .d = 128,
                                           .m = 16,
                                           .ef_construction = 200,
                                           .metric = irs::HNSWMetric::L2Sqr,
                                         });
}

TEST(CatalogPersistence, scorer_options) {
  CheckFixture("scorer_options.bin",
               ScorerOptions{.params = ScorerOptions::Bm25{}});
}

}  // namespace
}  // namespace sdb::catalog::persistence
