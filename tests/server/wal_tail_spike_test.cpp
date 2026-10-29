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

#include <algorithm>
#include <atomic>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <filesystem>
#include <string>
#include <thread>

#include "engine_context.h"
#include "search/wal_tail_keys.h"

namespace sdb::test {
namespace {

struct Attached {
  std::filesystem::path dir;
  std::string db_path;
  duckdb::shared_ptr<duckdb::AttachedDatabase> db;
};

Attached AttachOnDisk(duckdb::ClientContext& ctx, std::string_view name) {
  Attached out;
  out.dir =
    std::filesystem::temp_directory_path() / ("wal-spike-" + std::string{name});
  std::filesystem::remove_all(out.dir);
  std::filesystem::create_directories(out.dir);
  out.db_path = (out.dir / "spike.db").string();

  duckdb::Connection con{*ctx.db};
  // SereneDB's storage version, because USE_TABLE only carries the table oid
  // the resolver keys on when the file is written in it.
  // A DuckDB-format file: the gtest engine has no serenedb storage extension.
  // USE_TABLE therefore carries no table oid (it is serialized only for
  // SERENEDB_V1), so the collectors below key on "any table" instead.
  auto attach =
    con.Query("ATTACH '" + out.db_path + "' AS " + std::string{name});
  EXPECT_FALSE(attach->HasError()) << attach->GetError();
  auto& manager = duckdb::DatabaseManager::Get(*ctx.db);
  out.db = manager.GetDatabase(duckdb::Identifier{std::string{name}});
  return out;
}

duckdb::idx_t WalSize(duckdb::AttachedDatabase& db) {
  return db.GetStorageManager().GetWALSize();
}

std::string WalPath(duckdb::AttachedDatabase& db) {
  return db.GetStorageManager().GetWAL()->GetPath();
}

// Counts what the reader hands back, standing in for a certifier's consumer on
// a plain table (which has no key columns, so no terms are encoded).
class CountingCollector final : public duckdb::TableLogStorage,
                                public duckdb::TableLogStorageResolver {
 public:
  enum class Accepts : uint8_t {
    Everything,
    Nothing,
  };

  explicit CountingCollector(Accepts accepts) : _accepts{accepts} {}

  duckdb::idx_t staged_rows = 0;
  duckdb::idx_t committed_rows = 0;
  duckdb::idx_t committed_batches = 0;
  duckdb::idx_t dropped_batches = 0;
  duckdb::idx_t other_tables = 0;

  std::vector<duckdb::idx_t> seen_oids;

  duckdb::optional_ptr<duckdb::TableLogStorage> Resolve(
    duckdb::idx_t table_oid) final {
    seen_oids.push_back(table_oid);
    if (_accepts == Accepts::Nothing) {
      ++other_tables;
      return nullptr;
    }
    return this;
  }

  void EndBatch(bool committed) final {
    if (committed) {
      committed_rows += staged_rows;
      if (staged_rows != 0) {
        ++committed_batches;
      }
    } else if (staged_rows != 0) {
      ++dropped_batches;
    }
    staged_rows = 0;
  }

  void ReplayInsert(duckdb::ClientContext&, duckdb::DataChunk& chunk,
                    duckdb::optional_idx) final {
    staged_rows += chunk.size();
  }
  void ReplayDelete(duckdb::ClientContext&, duckdb::DataChunk&) final {}
  void ReplayDeleteRanges(duckdb::ClientContext&,
                          const duckdb::vector<duckdb::row_t>&,
                          const duckdb::vector<duckdb::idx_t>&) final {}
  void ReplayTruncate(duckdb::ClientContext&) final {}
  void ReplayAdoptSegments(duckdb::ClientContext&,
                           duckdb::vector<std::string>) final {}

 private:
  Accepts _accepts;
};

TEST(WalTailSpike, DeliversRowsCommittedAfterTheBoundary) {
  auto& ctx = EngineContext();
  auto attached = AttachOnDisk(ctx, "spikea");
  ASSERT_NE(attached.db, nullptr);

  duckdb::Connection con{*ctx.db};
  con.Query("CREATE TABLE spikea.t(i INTEGER, s VARCHAR)");
  con.Query("CREATE TABLE spikea.other(i INTEGER)");

  // Rows before the boundary stand in for the published tier.
  con.Query("INSERT INTO spikea.t VALUES (-1, 'published')");
  const auto boundary = WalSize(*attached.db);

  constexpr int kTxns = 6;
  constexpr int kRowsPerTxn = 3;
  constexpr int kOtherRows = 2;
  for (int i = 0; i < kTxns; ++i) {
    con.Query("INSERT INTO spikea.t VALUES (" + std::to_string(i) +
              ", 'a'), (" + std::to_string(i) + ", 'b'), (" +
              std::to_string(i) + ", 'c')");
  }
  // A second table's rows must be passed over, not counted, and not fatal.
  con.Query("INSERT INTO spikea.other VALUES (1), (2)");

  const auto end = WalSize(*attached.db);
  ASSERT_GT(end, boundary);

  CountingCollector collector{CountingCollector::Accepts::Everything};
  const auto read = duckdb::ReadWALTail(
    *attached.db, ctx, WalPath(*attached.db), boundary, end, collector);

  EXPECT_EQ(read.error, "");
  EXPECT_FALSE(read.stopped_undecided);
  EXPECT_GE(read.wal_version, 2u);
  EXPECT_EQ(read.boundary, end) << "a clean read must consume the whole range";
  EXPECT_EQ(collector.committed_rows,
            static_cast<duckdb::idx_t>(kTxns * kRowsPerTxn + kOtherRows))
    << "every row committed after the boundary should have been delivered";
  EXPECT_EQ(collector.committed_batches, static_cast<duckdb::idx_t>(kTxns + 1));
  EXPECT_EQ(collector.dropped_batches, 0u);
  EXPECT_FALSE(collector.seen_oids.empty()) << "USE_TABLE never resolved";

  // A resolver that claims nothing passes every entry over rather than
  // treating a table it has no consumer for as a corrupt WAL.
  CountingCollector skipping{CountingCollector::Accepts::Nothing};
  const auto skipped = duckdb::ReadWALTail(
    *attached.db, ctx, WalPath(*attached.db), boundary, end, skipping);
  EXPECT_EQ(skipped.error, "");
  EXPECT_EQ(skipped.boundary, end);
  EXPECT_EQ(skipping.committed_rows, 0u);
  EXPECT_GT(skipping.other_tables, 0u);

  // Reading the same range again is identical: the read mutates nothing.
  CountingCollector again{CountingCollector::Accepts::Everything};
  const auto second = duckdb::ReadWALTail(
    *attached.db, ctx, WalPath(*attached.db), boundary, end, again);
  EXPECT_EQ(second.error, "");
  EXPECT_EQ(again.committed_rows, collector.committed_rows);

  std::filesystem::remove_all(attached.dir);
}

TEST(WalTailSpike, BoundaryComposesAcrossSuccessiveReads) {
  auto& ctx = EngineContext();
  auto attached = AttachOnDisk(ctx, "spikec");
  ASSERT_NE(attached.db, nullptr);

  duckdb::Connection con{*ctx.db};
  con.Query("CREATE TABLE spikec.t(i INTEGER)");
  auto boundary = WalSize(*attached.db);

  duckdb::idx_t total = 0;
  for (int round = 0; round < 5; ++round) {
    for (int i = 0; i < 3; ++i) {
      con.Query("INSERT INTO spikec.t VALUES (" + std::to_string(i) + ")");
    }
    const auto end = WalSize(*attached.db);
    CountingCollector collector{CountingCollector::Accepts::Everything};
    const auto read = duckdb::ReadWALTail(
      *attached.db, ctx, WalPath(*attached.db), boundary, end, collector);
    ASSERT_EQ(read.error, "") << "round " << round;
    EXPECT_EQ(read.boundary, end);
    EXPECT_EQ(collector.committed_rows, 3u) << "round " << round;
    total += collector.committed_rows;
    boundary = read.boundary;
  }
  EXPECT_EQ(total, 15u)
    << "successive tails must partition the log, not overlap";

  std::filesystem::remove_all(attached.dir);
}

TEST(WalTailSpike, ReadsALogThatIsStillBeingWritten) {
  auto& ctx = EngineContext();
  auto attached = AttachOnDisk(ctx, "spikeb");
  ASSERT_NE(attached.db, nullptr);

  duckdb::Connection setup{*ctx.db};
  setup.Query("CREATE TABLE spikeb.t(i INTEGER, s VARCHAR)");
  const auto path = WalPath(*attached.db);

  std::atomic<bool> stop{false};
  std::atomic<int> written{0};
  std::thread writer{[&] {
    duckdb::Connection w{*ctx.db};
    for (int i = 0; !stop.load(std::memory_order_relaxed); ++i) {
      w.Query("INSERT INTO spikeb.t VALUES (" + std::to_string(i) +
              ", 'concurrent" + std::to_string(i) + "')");
      written.fetch_add(1, std::memory_order_relaxed);
    }
  }};

  int reads = 0;
  int failures = 0;
  std::string first_error;
  duckdb::idx_t boundary = 0;
  duckdb::idx_t delivered = 0;
  constexpr int kWantRows = 200;
  while (written.load(std::memory_order_relaxed) < kWantRows) {
    const auto end = WalSize(*attached.db);
    if (end <= boundary) {
      std::this_thread::yield();
      continue;
    }
    CountingCollector collector{CountingCollector::Accepts::Everything};
    const auto read =
      duckdb::ReadWALTail(*attached.db, ctx, path, boundary, end, collector);
    ++reads;
    if (!read.error.empty()) {
      ++failures;
      if (first_error.empty()) {
        first_error = read.error;
      }
      continue;
    }
    EXPECT_GE(read.boundary, boundary) << "the boundary went backwards";
    boundary = read.boundary;
    delivered += collector.committed_rows;
  }

  stop.store(true, std::memory_order_relaxed);
  writer.join();

  EXPECT_GT(written.load(), 0) << "the writer never committed anything";
  EXPECT_GT(reads, 0) << "no read ran";
  EXPECT_EQ(failures, 0) << "reads of a live WAL failed, first: "
                         << first_error;
  EXPECT_GT(delivered, 0u) << "no rows were delivered from the live tail";
  EXPECT_LE(delivered, static_cast<duckdb::idx_t>(written.load()))
    << "more rows delivered than were committed";

  std::filesystem::remove_all(attached.dir);
}

}  // namespace
}  // namespace sdb::test
