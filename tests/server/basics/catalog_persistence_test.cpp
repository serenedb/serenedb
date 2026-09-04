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
#include "catalog/database.h"
#include "catalog/persistence/index.h"
#include "catalog/persistence/inverted_index.h"
#include "catalog/persistence/role.h"
#include "catalog/persistence/scorer_options.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"
#include "connector/file_manifest.h"

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

// The same two checks for a definition the catalog log carries. A definition is
// a CreateInfo written by duckdb's own entry record -- property 101 the info,
// property 102 the permissions beside it -- so that pair is what a fixture
// pins, without the WAL framing around it.
void CheckDefinitionFixture(std::string_view name,
                            const duckdb::CreateInfo& info,
                            const Permissions& perm) {
  const fs::path path = FixturePath(name);
  const auto write = [&](const duckdb::CreateInfo& value) {
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer serializer{stream,
                                        duckdb::VersionStorageOptions()};
    serializer.OnObjectBegin();
    serializer.WriteProperty(101, "entry", &value);
    serializer.WriteProperty(102, "permissions", perm);
    serializer.OnObjectEnd();
    return std::string{reinterpret_cast<const char*>(stream.GetData()),
                       stream.GetPosition()};
  };
  const std::string bytes = write(info);

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

  duckdb::MemoryStream read{
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(golden.data())),
    golden.size()};
  duckdb::BinaryDeserializer deserializer{read};
  deserializer.OnObjectBegin();
  auto parsed =
    deserializer.ReadProperty<duckdb::unique_ptr<duckdb::CreateInfo>>(101,
                                                                      "entry");
  deserializer.ReadProperty<duckdb::CatalogPermissions>(102, "permissions");
  deserializer.OnObjectEnd();
  ASSERT_TRUE(parsed);
  EXPECT_EQ(write(*parsed), golden)
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

// The order-insensitive variant for types whose serialization is not byte
// canonical (hash-map members): the recorded bytes must parse to the sample
// (old files stay readable) and a round trip must preserve it.
template<typename T>
void CheckFixtureParsed(std::string_view name, const T& sample) {
  const fs::path path = FixturePath(name);

  if (std::getenv("SDB_REGEN_FIXTURES") != nullptr) {
    const std::string bytes = Serialize(sample);
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

  EXPECT_EQ(Deserialize<T>(golden), sample)
    << "on-disk format for " << name << " changed";
  EXPECT_EQ(Deserialize<T>(Serialize(sample)), sample)
    << "round trip of " << name << " lost data";
}

// A table's durable form is duckdb's own duckdb::CreateTableInfo, so the
// fixture is the frame the catalog log writes: the reflected info tuple
// followed by the per-column grants a ColumnDefinition has nowhere to keep. The
// whole record goes through the writer and reader the log uses, because that
// pairing is what a restart depends on.
TEST(CatalogPersistence, table) {
  auto info = catalog::NewTableInfo();
  info->SetTableName(duckdb::Identifier{"t"});
  info->SetSchema(duckdb::Identifier{"public"});
  catalog::SetTableTags(*info, TableEngine::Search,
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

  // A non-default owner, an acl item, and a per-column ACL on column "a"
  // (pg_attribute.attacl), so the frame exercises creator-owns and
  // column-level GRANT persistence rather than just defaults.
  Permissions perm{ObjectId{42},
                   {AclItem{.grantee = ObjectId{7},
                            .grantor = ObjectId{42},
                            .privs = AclMode::Select}}};
  catalog::SetColumnAcl(perm.column_acl, ObjectId{1},
                        Acl{AclItem{.grantee = ObjectId{7},
                                    .grantor = ObjectId{42},
                                    .privs = AclMode::Select}});

  CheckDefinitionFixture("table.bin", *info, perm);
}

TEST(CatalogPersistence, tokenizer) {
  // Owner and ACL are not here: a tokenizer's entry is the object, and the
  // record that carries the definition writes the permissions beside it.
  auto info = std::make_shared<catalog::CreateTokenizerInfo>(
    ObjectId{11}, ObjectId{12}, "tok", search::Features{},
    irs::analysis::TokenizerConfig{});
  CheckDefinitionFixture("tokenizer.bin", *info, Permissions{ObjectId{42}});
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
      .ann_config = std::nullopt,
      .synthetic_column = irs::field_limits::invalid(),
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
      .ann_config =
        AnnColumnConfig{
          .kind = irs::AnnKind::Ivf,
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
      .entries = {{1, EntryConfigSerialized{.text_dictionary = ObjectId{5}}}},
      .options = InvertedIndexOptions{.row_group_size = 1024},
      .comment = "inv note",
    });
}

TEST(CatalogPersistence, database_options) {
  // Owner and ACL are not here: a database's entry is the object, and the
  // record that carries the definition writes the permissions beside it.
  auto info = std::make_shared<catalog::CreateDatabaseInfo>(
    ObjectId{9}, "db", /*public_schema_id=*/ObjectId{10});
  CheckDefinitionFixture("database_options.bin", *info,
                         Permissions{ObjectId{42}});
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
      .password = {"SCRAM-SHA-256$4096:c2FsdHNhbHQ=$"
                   "c3RvcmVka2V5c3RvcmVka2V5c3RvcmVka2V5c3Q=:"
                   "c2VydmVya2V5c2VydmVya2V5c2VydmVya2V5c2U="},
    });
}

TEST(CatalogPersistence, inverted_index_options) {
  CheckFixture("inverted_index_options.bin", InvertedIndexOptions{
                                               .row_group_size = 1024,
                                               .refresh_interval_ms = 100,
                                               .compaction_interval_ms = 200,
                                               .cleanup_interval_step = 3,
                                               .reindex_interval_ms = 400,
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

// Not a catalog struct, but the same WriteTuple format: the view-index source
// manifest embedded in the iresearch segment-meta payload (FileManifest
// AppendTo/Parse go through the identical serializer). Its `entries` is a
// hash map, so the serialized byte ORDER is not canonical (absl salts
// iteration per container instance) -- the golden is checked SEMANTICALLY:
// the recorded bytes must keep parsing to the expected values and a round
// trip must preserve them; bytes are never compared.
TEST(CatalogPersistence, file_manifest) {
  CheckFixtureParsed("file_manifest.bin",
                     search::FileManifest{
                       .entries = {{0,
                                    {.file_id = 0,
                                     .path = "s3://bucket/data/a.parquet",
                                     .etag = "\"abc123\""}},
                                   {3,
                                    {.file_id = 3,
                                     .path = "/local/b.parquet",
                                     .mtime_micros = 1721900000000000}}},
                     });
}

// Iceberg persists as the version alone (FileManifest::Serialize drops the
// entries when the version is set): the pin is the identity.
TEST(CatalogPersistence, file_manifest_iceberg) {
  CheckFixtureParsed("file_manifest_iceberg.bin",
                     search::FileManifest{.version = 7011998});
}

}  // namespace
}  // namespace sdb::catalog::persistence
