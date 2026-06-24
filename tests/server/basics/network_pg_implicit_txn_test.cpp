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

#include <duckdb/main/connection.hpp>

#include "basics/duckdb_engine.h"
#include "network/pg/protocol_state.h"

using namespace sdb::network::pg;

namespace {

// A fresh autocommit connection per test; ImplicitTxnState borrows its
// TransactionContext (the connection is destroyed at scope end, rolling back
// anything still open).
struct Conn {
  decltype(sdb::DuckDBEngine::Instance().CreateConnection()) handle =
    sdb::DuckDBEngine::Instance().CreateConnection();
  duckdb::TransactionContext& txn() { return handle->context->transaction; }
};

}  // namespace

TEST(NetworkPgImplicitTxn, ArmOnAutocommitOpensTheBlock) {
  Conn c;
  ASSERT_TRUE(c.txn().IsAutoCommit());
  ImplicitTxnState state{c.txn()};
  EXPECT_FALSE(state.OpenedByUs());
  state.Arm();
  EXPECT_TRUE(state.OpenedByUs());
  EXPECT_FALSE(state.Explicit());
  EXPECT_FALSE(c.txn().IsAutoCommit());  // SetAutoCommit(false)
  state.Arm();                           // idempotent
  EXPECT_TRUE(state.OpenedByUs());
  EXPECT_FALSE(state.Explicit());
}

TEST(NetworkPgImplicitTxn, ArmYieldsToAUserBegin) {
  Conn c;
  c.txn().SetAutoCommit(false);  // a user BEGIN is already active
  ImplicitTxnState state{c.txn()};
  state.Arm();
  EXPECT_FALSE(state.OpenedByUs());  // we did not open it
  EXPECT_TRUE(state.Explicit());
  EXPECT_FALSE(state.ShouldCommitAtSync());
  EXPECT_FALSE(state.ShouldRollback());
}

TEST(NetworkPgImplicitTxn, CommitAndRollbackGuards) {
  Conn c;
  ImplicitTxnState state{c.txn()};
  EXPECT_FALSE(state.ShouldCommitAtSync());  // nothing open yet
  EXPECT_FALSE(state.ShouldRollback());
  state.Arm();
  EXPECT_TRUE(state.ShouldCommitAtSync());
  EXPECT_TRUE(state.ShouldRollback());
}

TEST(NetworkPgImplicitTxn, TrackTxnStatement) {
  Conn c;
  ImplicitTxnState state{c.txn()};
  c.txn().SetAutoCommit(false);  // a BEGIN left the transaction open
  state.TrackTxnStatement();
  EXPECT_TRUE(state.Explicit());
  EXPECT_FALSE(state.OpenedByUs());
  EXPECT_FALSE(state.Closed());  // a block is open -> portals stay
  c.txn().SetAutoCommit(true);   // a COMMIT/ROLLBACK closed it
  state.TrackTxnStatement();
  EXPECT_FALSE(state.Explicit());
  EXPECT_FALSE(state.OpenedByUs());
  EXPECT_TRUE(state.Closed());  // boundary reached -> portals drop
}

TEST(NetworkPgImplicitTxn, CommitResolvesTheBlock) {
  Conn c;
  ImplicitTxnState state{c.txn()};
  state.Arm();
  c.handle->Query("SELECT 1");  // materialise the deferred transaction
  EXPECT_FALSE(state.Commit().has_value());  // nullopt = success
  EXPECT_FALSE(state.OpenedByUs());          // open bit cleared first
  EXPECT_TRUE(c.txn().IsAutoCommit());       // autocommit restored
}

TEST(NetworkPgImplicitTxn, RollbackResolvesTheBlock) {
  Conn c;
  ImplicitTxnState state{c.txn()};
  state.Arm();
  c.handle->Query("SELECT 1");
  state.Rollback();
  EXPECT_FALSE(state.OpenedByUs());
  EXPECT_TRUE(c.txn().IsAutoCommit());
}
