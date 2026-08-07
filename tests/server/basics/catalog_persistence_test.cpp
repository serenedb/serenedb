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
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
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
#include "catalog/persistence/tokenizer.h"
#include "catalog/store/wal_entry.h"
#include "catalog/table.h"

namespace sdb::catalog::persistence {
namespace {

namespace fs = std::filesystem;

fs::path FixturePath(std::string_view name) {
  return fs::path{SDB_RESOURCE_DIR} / "tests" / "catalog" / name;
}

template<typename T>
std::string Serialize(const T& value) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream, duckdb::VersionStorageOptions()};
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

// The same two checks for a whole wal frame, which is how a definition that is
// a CreateInfo is written: the reflected tuple plus what travels beside it.
void CheckFrameFixture(std::string_view name, const wal::Entry& entry) {
  const fs::path path = FixturePath(name);
  duckdb::MemoryStream stream;
  wal::SerializeEntries({}, {&entry, 1}, stream);
  const std::string bytes{reinterpret_cast<const char*>(stream.GetData()),
                          stream.GetPosition()};

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

  EXPECT_EQ(bytes, golden) << "on-disk format for " << name << " changed";

  auto parsed = wal::ParseEntries(
    {reinterpret_cast<const uint8_t*>(golden.data()), golden.size()});
  ASSERT_EQ(parsed.entries.size(), 1);
  duckdb::MemoryStream again;
  wal::SerializeEntries({}, {&parsed.entries.front().entry, 1}, again);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(again.GetData()),
                        again.GetPosition()),
            golden)
    << "deserialization of " << name << " is not byte-stable";
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
  // columns slot 2 is an expression sentinel (kInvalidColumnId); the
  // interleaving (column, expr, column) is the ART key order and must
  // round-trip.
  CheckFixture("secondary_index.bin",
               SecondaryIndexData{
                 .name = "idx_demo",
                 .unique = true,
                 .columns = {ObjectId{1}, kInvalidColumnId, ObjectId{3}},
                 .expressions =
                   {
                     ExpressionData{
                       .serialized_expr = "expr-bytes",
                       .dependent_columns = {ObjectId{2}},
                       .return_type = duckdb::LogicalType::DOUBLE,
                       .pretty_printed = "a + b",
                     },
                   },
                 .comment = "sk note",
               });
}

// A table's durable form is duckdb's own CreateTableInfo, so the fixture is
// the frame the catalog log writes: the reflected info tuple followed by the
// per-column grants a ColumnDefinition has nowhere to keep. The whole record
// goes through the writer and reader the log uses, because that pairing is
// what a restart depends on.
TEST(CatalogPersistence, table) {
  auto info = std::make_shared<CreateTableInfo>();
  info->SetTableName(duckdb::Identifier{"t"});
  info->SetSchema(duckdb::Identifier{"public"});
  info->SetTableTags(TableEngine::Search,
                     {.refresh_interval_ms = 500,
                      .compaction_interval_ms = 7000,
                      .cleanup_interval_step = 3},
                     ObjectId{9});

  duckdb::ColumnDefinition col_a{duckdb::Identifier{"a"},
                                 duckdb::LogicalType::INTEGER};
  col_a.SetCatalogOid(1);
  col_a.SetCompressionType(duckdb::CompressionType::COMPRESSION_ZSTD);
  info->columns.AddColumn(std::move(col_a));
  duckdb::ColumnDefinition col_b{duckdb::Identifier{"b"},
                                 duckdb::LogicalType::VARCHAR};
  col_b.SetCatalogOid(2);
  info->columns.AddColumn(std::move(col_b));

  auto not_null =
    duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex{0});
  not_null->oid = 3;
  not_null->constraint_name = "t_a_not_null";
  info->constraints.push_back(std::move(not_null));
  auto key = duckdb::make_uniq<duckdb::UniqueConstraint>(
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{"a"}},
    /*is_primary_key=*/true);
  key->oid = 4;
  key->host_index_id = 5;
  key->constraint_name = "t_pkey";
  info->constraints.push_back(std::move(key));

  // Column "a" carries a per-column ACL (pg_attribute.attacl), so the golden
  // bytes exercise column-level GRANT persistence; column "b" stays default.
  CreateTableInfo::ColumnAcls acls;
  acls.emplace(ObjectId{1}, Acl{AclItem{.grantee = ObjectId{7},
                                        .grantor = ObjectId{42},
                                        .privs = AclMode::Select}});
  info->SetColumnAcls(std::move(acls));

  // A non-default owner plus an acl item, so the frame exercises
  // creator-owns persistence rather than just defaults.
  const wal::Entry entry{
    wal::PutTable{.schema_id = ObjectId{11},
                  .id = ObjectId{12},
                  .mode = wal::PutMode::Create,
                  .info = std::move(info),
                  .perm = Permissions{ObjectId{42},
                                      {AclItem{.grantee = ObjectId{7},
                                               .grantor = ObjectId{42},
                                               .privs = AclMode::Select}}}}};
  CheckFrameFixture("table.bin", entry);
}

TEST(CatalogPersistence, tokenizer) {
  // Owner and ACL are not here: a tokenizer's entry is the object, and the
  // record that carries the definition writes the permissions beside it.
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

TEST(CatalogPersistence, entry_config_serialized) {
  CheckFixture(
    "entry_config_serialized.bin",
    EntryConfigSerialized{
      .text_dictionary = ObjectId{5},
      .store_values = true,
      .compression = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED,
      .features = search::Features{},
      .ivf_config = std::nullopt,
      .synthetic_column = irs::field_limits::invalid(),
      .row_group_size = 100,
      .norm_row_group_size = 50,
    });
}

TEST(CatalogPersistence, entry_config_serialized_ivf) {
  CheckFixture(
    "entry_config_serialized_ivf.bin",
    EntryConfigSerialized{
      .text_dictionary = ObjectId{5},
      .store_values = true,
      .compression = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED,
      .features = search::Features{},
      .ivf_config =
        IVFColumnConfig{
          .d = 128,
          .metric = irs::VectorMetric::InnerProduct,
          .quant = irs::VectorQuantization::SQ8,
          .pq_m = 0,
          .rabitq_bits = 0,
          .sample_factor = 0.5f,
          .posting_size = 2048,
          .compression = false,
        },
      .synthetic_column = irs::field_limits::invalid(),
      .row_group_size = 100,
      .norm_row_group_size = 50,
    });
}

TEST(CatalogPersistence, inverted_index) {
  // One column key (field_id == column id 1) and one expression key (allocated
  // field_id 7). `entries` is kept to a single element so the unordered map
  // serializes to stable bytes.
  CheckFixture(
    "inverted_index.bin",
    InvertedIndexData{
      .name = "idx",
      .columns = {ObjectId{1}},
      .expression_keys = {ExpressionKey{
        .data = ExpressionData{.serialized_expr = "e",
                               .return_type = duckdb::LogicalType::DOUBLE,
                               .pretty_printed = "x + 1"},
        .field_id = 7}},
      .entries = {{1, EntryConfigSerialized{.text_dictionary = ObjectId{5},
                                            .row_group_size = 100}}},
      .options = InvertedIndexOptions{.row_group_size = 1024},
      .comment = "inv note",
    });
}

TEST(CatalogPersistence, database_options) {
  // Owner and ACL are not here: a database's entry is the object, and the
  // record that carries the definition writes the permissions beside it.
  CheckFixture("database_options.bin", DatabaseOptions{.name = "db"});
}

TEST(CatalogPersistence, schema_options) {
  // Owner and ACL are not here either: a schema's entry is the object, and the
  // record that carries the definition writes the permissions beside it.
  CheckFixture("schema_options.bin", SchemaOptions{.name = "public"});
}

TEST(CatalogPersistence, sequence_options) {
  // Owner and ACL are not here either: a sequence's entry is the object, and
  // the record that carries the definition writes the permissions beside it.
  CheckFixture("sequence_options.bin", SequenceOptions{
                                         .name = "seq",
                                         .start_value = 10,
                                         .increment = 2,
                                         .min_value = 1,
                                         .max_value = 1000,
                                         .cache = 5,
                                         .owner_table_id = 3,
                                         .cycle = true,
                                         .comment = "seq note",
                                       });
}

TEST(CatalogPersistence, role_data) {
  // Single-entry db_access: the map is unordered, so >1 entry would not
  // serialize to stable bytes.
  CheckFixture(
    "role_data.bin",
    RoleData{
      .name = "alice",
      // RBAC: attribute bitmask + a membership edge with non-default per-edge
      // options, so the golden bytes exercise Membership round-trip.
      .options = 0b0110,  // Login | Inherit
      .member_of = {Membership{.role = ObjectId{5},
                               .admin_option = true,
                               .inherit_option = false,
                               .set_option = true}},
      // RBAC attrs surfaced via pg_authid / pg_roles, plus per-role GUC config
      // and ALTER DEFAULT PRIVILEGES targets. valid_until is opaque micros.
      .conn_limit = 5,
      .valid_until = 946771200000000,  // 2030-01-01 00:00:00+00
      .config = {"search_path=clickclack"},
      .default_acls = {DefaultAcl{.schema = ObjectId{7},
                                  .objtype = 'r',
                                  .acl = {AclItem{.grantee = ObjectId{5},
                                                  .grantor = ObjectId{2},
                                                  .privs = AclMode::Select}}}},
      // rolpassword: a stored SCRAM verifier, so the golden bytes exercise the
      // password round-trip.
      .password_verifier = {"SCRAM-SHA-256$4096:c2FsdHNhbHQ=$"
                            "c3RvcmVka2V5c3RvcmVka2V5c3RvcmVka2V5c3Q=:"
                            "c2VydmVya2V5c2VydmVya2V5c2VydmVya2V5c2U="},
    });
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

TEST(CatalogPersistence, scorer_options) {
  CheckFixture("scorer_options.bin",
               ScorerOptions{.params = ScorerOptions::Bm25{}});
}

}  // namespace
}  // namespace sdb::catalog::persistence
