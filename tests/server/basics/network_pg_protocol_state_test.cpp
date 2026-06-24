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

#include <memory>

#include "network/pg/protocol_state.h"

using namespace sdb::network::pg;

TEST(NetworkPgProtocolState, AnonSlotAndFindOrThrow) {
  ProtocolState proto;
  EXPECT_NO_THROW((void)proto.statements.FindOrThrow(""));  // anon slot, no map
  EXPECT_ANY_THROW((void)proto.statements.FindOrThrow("nope"));
  EXPECT_ANY_THROW((void)proto.portals.FindOrThrow("nope"));
  EXPECT_NE(proto.portals.Find(""), nullptr);  // anon slot
  EXPECT_EQ(proto.portals.Find("nope"), nullptr);
}

TEST(NetworkPgProtocolState, CreateRejectsDuplicate) {
  ProtocolState proto;
  proto.statements.Create("s");
  EXPECT_ANY_THROW((void)proto.statements.Create("s"));
  EXPECT_NO_THROW(
    (void)proto.statements.FindOrThrow("s"));  // the first survived
}

TEST(NetworkPgProtocolState, UndefineAndDropAreSilentOnMissing) {
  ProtocolState proto;
  EXPECT_NO_THROW(proto.statements.Undefine("missing"));
  EXPECT_NO_THROW(proto.portals.Drop("missing"));
  proto.statements.Create("s");
  proto.statements.Undefine("s");
  EXPECT_EQ(proto.statements.Find("s"), nullptr);
}

TEST(NetworkPgProtocolState, CloseStatementKeepsBoundPortals) {
  ProtocolState proto;
  proto.statements.Create("s");
  // Each portal co-owns the plan via its StatementPtr (copied at "Bind").
  proto.portals.Create("p1").stmt = proto.statements.FindOrThrow("s");
  proto.portals.Anon().stmt = proto.statements.FindOrThrow("s");

  proto.statements.Drop("s");  // Close 'S' -- no cascade into portals

  EXPECT_NE(proto.portals.Find("p1"), nullptr);        // portal survives
  EXPECT_NE(proto.portals.Find("p1")->stmt, nullptr);  // its plan is still live
  EXPECT_NE(proto.portals.Anon().stmt, nullptr);
  EXPECT_EQ(proto.statements.Find("s"), nullptr);  // only the entry is dropped
}

TEST(NetworkPgProtocolState, DropTxnPortalsClearsPortalsKeepsStatements) {
  ProtocolState proto;
  proto.statements.Create("s");
  proto.portals.Create("p1").stmt = proto.statements.FindOrThrow("s");
  proto.portals.Anon().stmt = proto.statements.FindOrThrow("s");

  proto.DropTxnPortals();

  EXPECT_EQ(proto.portals.Find("p1"), nullptr);
  EXPECT_EQ(proto.portals.Anon().stmt, nullptr);  // anon portal reset
  EXPECT_NO_THROW((void)proto.statements.FindOrThrow(
    "s"));  // statements survive the boundary
}

TEST(NetworkPgProtocolState, ReParseUnnamedKeepsBoundPortal) {
  ProtocolState proto;
  proto.portals.Anon().stmt = proto.statements.Anon();  // bind the anon portal
  auto* prior = proto.portals.Anon().stmt.get();
  // Re-Parse of the unnamed statement installs a fresh plan in the slot; the
  // bound portal keeps its own.
  proto.statements.Anon() = std::make_shared<Statement>();
  EXPECT_EQ(proto.portals.Anon().stmt.get(), prior);
  EXPECT_NE(proto.statements.Anon().get(), prior);
}
