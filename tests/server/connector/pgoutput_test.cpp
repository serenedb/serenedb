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

#include <exception>
#include <string>
#include <vector>

#include "replication/pgoutput.h"

namespace sdb::replication {
namespace {

struct Builder {
  std::string b;
  void Byte(uint8_t v) { b.push_back(static_cast<char>(v)); }
  void Int16(uint16_t v) {
    Byte(v >> 8);
    Byte(v & 0xff);
  }
  void Int32(uint32_t v) {
    Byte(v >> 24);
    Byte(v >> 16);
    Byte(v >> 8);
    Byte(v);
  }
  void Int64(uint64_t v) {
    Int32(static_cast<uint32_t>(v >> 32));
    Int32(static_cast<uint32_t>(v));
  }
  void Str(const std::string& s) {
    b += s;
    Byte(0);
  }
  void TextCol(const std::string& s) {
    Byte('t');
    Int32(static_cast<uint32_t>(s.size()));
    b += s;
  }
};

std::vector<PgColumn> ReadCols(std::string_view tuple) {
  PgTupleReader reader{tuple};
  std::vector<PgColumn> cols;
  while (reader.HasNext()) {
    cols.push_back(reader.Next());
  }
  return cols;
}

TEST(PgOutput, Begin) {
  Builder x;
  x.Byte('B');
  x.Int64(0x16B3728);  // final_lsn
  x.Int64(700000);     // commit_time
  x.Int32(1234);       // xid
  auto msg = DecodePgOutput(x.b);
  auto* b = std::get_if<BeginMessage>(&msg);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ(b->final_lsn, 0x16B3728u);
  EXPECT_EQ(b->xid, 1234u);
}

TEST(PgOutput, Commit) {
  Builder x;
  x.Byte('C');
  x.Byte(0);           // flags
  x.Int64(0x16B3728);  // commit_lsn
  x.Int64(0x16B3800);  // end_lsn
  x.Int64(700001);     // commit_time
  auto msg = DecodePgOutput(x.b);
  auto* c = std::get_if<CommitMessage>(&msg);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(c->commit_lsn, 0x16B3728u);
  EXPECT_EQ(c->end_lsn, 0x16B3800u);
}

TEST(PgOutput, RelationWithKey) {
  Builder x;
  x.Byte('R');
  x.Int32(16392);   // relid
  x.Str("public");  // namespace
  x.Str("t1");      // relation
  x.Byte('d');      // replica identity default
  x.Int16(2);       // natts
  x.Byte(1);        // flags: key
  x.Str("id");
  x.Int32(23);  // int4
  x.Int32(-1);
  x.Byte(0);  // flags: non-key
  x.Str("name");
  x.Int32(25);  // text
  x.Int32(-1);
  auto msg = DecodePgOutput(x.b);
  auto* r = std::get_if<RelationMessage>(&msg);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(r->relation_id, 16392u);
  EXPECT_EQ(r->namespace_name, "public");
  EXPECT_EQ(r->relation_name, "t1");
  ASSERT_EQ(r->columns.size(), 2u);
  EXPECT_TRUE(r->columns[0].is_key);
  EXPECT_EQ(r->columns[0].name, "id");
  EXPECT_FALSE(r->columns[1].is_key);
  EXPECT_EQ(r->columns[1].name, "name");
}

TEST(PgOutput, RelationEmptyNamespaceIsPgCatalog) {
  Builder x;
  x.Byte('R');
  x.Int32(1);
  x.Str("");  // empty -> pg_catalog on the wire
  x.Str("pg_class");
  x.Byte('d');
  x.Int16(0);
  auto msg = DecodePgOutput(x.b);
  auto* r = std::get_if<RelationMessage>(&msg);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(r->namespace_name, "");
}

TEST(PgOutput, InsertTuple) {
  Builder x;
  x.Byte('I');
  x.Int32(16392);
  x.Byte('N');
  x.Int16(3);      // ncols
  x.TextCol("1");  // id
  x.TextCol("alice");
  x.Byte('n');  // amount NULL
  auto msg = DecodePgOutput(x.b);
  auto* i = std::get_if<InsertMessage>(&msg);
  ASSERT_NE(i, nullptr);
  EXPECT_EQ(i->relation_id, 16392u);
  const auto cols = ReadCols(i->new_tuple);
  ASSERT_EQ(cols.size(), 3u);
  EXPECT_EQ(cols[0].kind, TupleColKind::Text);
  EXPECT_EQ(cols[0].data, "1");
  EXPECT_EQ(cols[1].data, "alice");
  EXPECT_EQ(cols[2].kind, TupleColKind::Null);
}

TEST(PgOutput, UpdateWithOldKey) {
  Builder x;
  x.Byte('U');
  x.Int32(16392);
  x.Byte('K');  // old key follows
  x.Int16(1);
  x.TextCol("1");  // old id
  x.Byte('N');     // new tuple
  x.Int16(1);
  x.TextCol("2");  // new id
  auto msg = DecodePgOutput(x.b);
  auto* u = std::get_if<UpdateMessage>(&msg);
  ASSERT_NE(u, nullptr);
  EXPECT_TRUE(u->has_old);
  EXPECT_TRUE(u->old_is_key);
  const auto old_cols = ReadCols(u->old_tuple);
  ASSERT_EQ(old_cols.size(), 1u);
  EXPECT_EQ(old_cols[0].data, "1");
  const auto new_cols = ReadCols(u->new_tuple);
  ASSERT_EQ(new_cols.size(), 1u);
  EXPECT_EQ(new_cols[0].data, "2");
}

TEST(PgOutput, UpdateNoOld) {
  Builder x;
  x.Byte('U');
  x.Int32(16392);
  x.Byte('N');
  x.Int16(2);
  x.TextCol("1");
  x.Byte('u');  // unchanged toast
  auto msg = DecodePgOutput(x.b);
  auto* u = std::get_if<UpdateMessage>(&msg);
  ASSERT_NE(u, nullptr);
  EXPECT_FALSE(u->has_old);
  const auto new_cols = ReadCols(u->new_tuple);
  ASSERT_EQ(new_cols.size(), 2u);
  EXPECT_EQ(new_cols[1].kind, TupleColKind::Unchanged);
}

TEST(PgOutput, Delete) {
  Builder x;
  x.Byte('D');
  x.Int32(16392);
  x.Byte('K');
  x.Int16(1);
  x.TextCol("7");
  auto msg = DecodePgOutput(x.b);
  auto* d = std::get_if<DeleteMessage>(&msg);
  ASSERT_NE(d, nullptr);
  EXPECT_TRUE(d->old_is_key);
  const auto cols = ReadCols(d->old_tuple);
  ASSERT_EQ(cols.size(), 1u);
  EXPECT_EQ(cols[0].data, "7");
}

TEST(PgOutput, TruncateFlags) {
  Builder x;
  x.Byte('T');
  x.Int32(2);  // nrelids
  x.Byte(3);   // cascade | restart
  x.Int32(10);
  x.Int32(11);
  auto msg = DecodePgOutput(x.b);
  auto* t = std::get_if<TruncateMessage>(&msg);
  ASSERT_NE(t, nullptr);
  EXPECT_TRUE(t->cascade);
  EXPECT_TRUE(t->restart_identity);
  ASSERT_EQ(t->relation_ids.size(), 2u);
  EXPECT_EQ(t->relation_ids[1], 11u);
}

TEST(PgOutput, TruncatedBufferThrows) {
  Builder x;
  x.Byte('I');
  x.Int32(16392);
  x.Byte('N');
  x.Int16(2);      // claims 2 cols
  x.TextCol("1");  // only 1 provided
  EXPECT_THROW(DecodePgOutput(x.b), std::exception);
}

TEST(PgOutput, EmptyThrows) {
  EXPECT_THROW(DecodePgOutput(std::string_view{}), std::exception);
}

TEST(PgOutput, StreamStart) {
  Builder x;
  x.Byte('S');
  x.Int32(771);
  x.Byte(1);
  auto msg = DecodePgOutput(x.b);
  auto* start = std::get_if<StreamStartMessage>(&msg);
  ASSERT_NE(start, nullptr);
  EXPECT_EQ(start->xid, 771u);
  EXPECT_TRUE(start->first_segment);
}

TEST(PgOutput, StreamStop) {
  auto msg = DecodePgOutput("E");
  EXPECT_NE(std::get_if<StreamStopMessage>(&msg), nullptr);
}

TEST(PgOutput, StreamCommit) {
  Builder x;
  x.Byte('c');
  x.Int32(771);
  x.Byte(0);
  x.Int64(0x2000);
  x.Int64(0x2040);
  x.Int64(900000);
  auto msg = DecodePgOutput(x.b);
  auto* commit = std::get_if<StreamCommitMessage>(&msg);
  ASSERT_NE(commit, nullptr);
  EXPECT_EQ(commit->xid, 771u);
  EXPECT_EQ(commit->commit_lsn, 0x2000u);
  EXPECT_EQ(commit->end_lsn, 0x2040u);
  EXPECT_EQ(commit->commit_time, 900000);
}

TEST(PgOutput, StreamAbort) {
  Builder x;
  x.Byte('A');
  x.Int32(771);
  x.Int32(772);
  auto msg = DecodePgOutput(x.b);
  auto* abort = std::get_if<StreamAbortMessage>(&msg);
  ASSERT_NE(abort, nullptr);
  EXPECT_EQ(abort->xid, 771u);
  EXPECT_EQ(abort->subxid, 772u);
}

TEST(PgOutput, StreamedInsertCarriesXid) {
  Builder x;
  x.Byte('I');
  x.Int32(772);
  x.Int32(16392);
  x.Byte('N');
  x.Int16(1);
  x.TextCol("5");
  EXPECT_EQ(StreamedMessageXid(x.b), 772u);
  auto msg = DecodePgOutput(x.b, true);
  auto* insert = std::get_if<InsertMessage>(&msg);
  ASSERT_NE(insert, nullptr);
  EXPECT_EQ(insert->relation_id, 16392u);
  const auto cols = ReadCols(insert->new_tuple);
  ASSERT_EQ(cols.size(), 1u);
  EXPECT_EQ(cols[0].data, "5");
}

TEST(PgOutput, StreamedRelationCarriesXid) {
  Builder x;
  x.Byte('R');
  x.Int32(771);
  x.Int32(16392);
  x.Str("public");
  x.Str("t");
  x.Byte('d');
  x.Int16(1);
  x.Byte(1);
  x.Str("id");
  x.Int32(23);
  x.Int32(0xFFFFFFFF);
  auto msg = DecodePgOutput(x.b, true);
  auto* relation = std::get_if<RelationMessage>(&msg);
  ASSERT_NE(relation, nullptr);
  EXPECT_EQ(relation->relation_id, 16392u);
  EXPECT_EQ(relation->relation_name, "t");
  ASSERT_EQ(relation->columns.size(), 1u);
  EXPECT_TRUE(relation->columns[0].is_key);
}

}  // namespace
}  // namespace sdb::replication
