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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/store/store_op.h"
#include "catalog/table_options.h"

namespace duckdb {

struct CreateSequenceInfo;

class CreateInfo;
class MemoryStream;

}  // namespace duckdb
namespace sdb::catalog::wal {

// One entry per catalog operation, each carrying the fields that operation
// needs and nothing else. There is no universal record: the {op, key,
// sequence_value, def} row the catalog used when it lived in RocksDB tables is
// what a table wants, and comes back only if the catalog moves into a duckdb
// table.
//
// A definition is the CreateInfo its catalog entry is built from, written
// inline and read back by dispatching the entry's own type field, so nothing
// has to be staged as bytes.

// Whether a definition record brings the object into being or supersedes a
// version of it that is already there.
//
// The record says which; the applier does not infer it from whether the id is
// already held. The two are not the same question once a record can be replayed
// against a catalog the statement never saw: a replace whose target a
// concurrently committed transaction has dropped must be refused (40001), and
// an inference reads it as a create and resurrects the object instead.
enum class PutMode : uint8_t {
  Create,
  Replace,
};

// A SERIAL's sequence, created and seeded by the statement that owns it. Only a
// create carries these -- a later version of the table names no sequence, so
// the table's own mode is the one they are performed under.
//
// The definition is the CreateInfo a catalog entry is built from, with the
// owner and ACL beside it, exactly as PutEntry carries a free-standing
// sequence.
struct OwnedSequence {
  ObjectId id;
  std::shared_ptr<const duckdb::CreateSequenceInfo> info;
  Permissions perm;
  uint64_t seed = 0;
};

// Creates or replaces one table: CREATE TABLE / CTAS / search-table create and
// every ALTER that rewrites the definition. The sequences the table owns are
// part of the same operation, so they ride the same entry -- which is the one
// thing that keeps a table off PutEntry.
//
// The definition is duckdb's own duckdb::CreateTableInfo, extended with the
// per-column grants a ColumnDefinition has nowhere to keep; `perm` travels
// beside it exactly as it does on PutEntry.
struct PutTable {
  ObjectId schema_id;
  ObjectId id;
  PutMode mode{PutMode::Create};
  std::shared_ptr<const duckdb::CreateTableInfo> info;
  Permissions perm;
  std::vector<OwnedSequence> sequences;
};

// Creates or replaces one object whose definition is a duckdb CreateInfo --
// the form a catalog entry is built from, so the record and the entry are made
// from the same thing rather than one being derived from the other.
//
// `perm` is the owner and ACL, which duckdb's CreateInfo has nowhere to put --
// the entry carries them, so they travel beside the definition.
//
// An index is the one kind with two ancestors -- its name lives in the
// schema's relation namespace, its rows belong to a relation -- and
// `parent_id` names the schema, so the relation it covers rides on the info
// (CreateIndexInfoBase).
//
struct PutEntry {
  ObjectId parent_id;
  duckdb::CatalogType type{duckdb::CatalogType::INVALID};
  // Which of the two index kinds, for the one CatalogType that covers two.
  bool inverted = false;
  ObjectId id;
  PutMode mode{PutMode::Create};
  std::shared_ptr<const duckdb::CreateInfo> info;
  Permissions perm;
};

// Erases every definition directly under `parent_id`. Each level of a drop
// emits its own, exactly as the drop-task cascade already walks the tree --
// so nothing has to rediscover the subtree at apply time.
struct DropChildren {
  ObjectId parent_id;
};

struct DropObject {
  ObjectId parent_id;
  duckdb::CatalogType type{duckdb::CatalogType::INVALID};
  ObjectId id;
};

// One object inside the subtree a still-open drop has to reclaim. `parent_id`
// is the object it hangs off inside that subtree -- the database for a schema,
// the schema for a table, the table for its indexes and owned sequences.
// `engine` is meaningful for a table and says which artifacts its reclamation
// removes.
struct DropNode {
  ObjectId parent_id;
  ObjectId id;
  duckdb::CatalogType type{duckdb::CatalogType::INVALID};
  TableEngine engine{TableEngine::Transactional};
  // Meaningful for an index: whether its reclamation removes an iresearch
  // directory. `engine` plays the same role for a table.
  bool inverted = false;
};

// Opens a drop, for the root of the dropped subtree only: everything
// structurally under it goes with it, so a cascade costs one entry, not one per
// object. It also marks the id spent before anything else can claim it.
//
// It applies the moment it lands: making the object invisible is the whole
// point of it, and a client's response depends on that having happened. What it
// defers is the reclamation, not the visibility.
//
// Objects a cascade removed from outside the subtree (a sequence owned from
// another schema, a dependent elsewhere in the tree) are not covered by the
// root and keep their own DropObject.
//
// The record carries the reclamation itself: `database_id`/`schema_id` place
// the root, and `subtree` lists everything under it whose artifacts still have
// to be swept. A boot that finds the drop open rebuilds the async task tree
// from this alone -- it never reads back the definitions the drop removed,
// which is what lets the log stop keeping them.
struct DropPrepare {
  ObjectId parent_id;
  duckdb::CatalogType type{duckdb::CatalogType::INVALID};
  bool inverted = false;
  ObjectId id;
  ObjectId database_id;
  ObjectId schema_id;
  std::shared_ptr<const std::vector<DropNode>> subtree;
};

// Ordered, authoritative assign: setval, creation seed, compaction snapshot.
struct SetSequence {
  ObjectId id;
  uint64_t value = 0;
};

// Monotonic horizon bump, replayed as a max-merge: these run outside the
// sequence lock and group-commit freely, so appends may land out of order.
struct BumpSequence {
  ObjectId id;
  uint64_t value = 0;
};

// Drops the counter. Pairs with the DropObject that drops the definition: a
// sequence is the one object with state outside its definition.
struct DropSequence {
  ObjectId id;
};

// Closes the drop opened by the DropPrepare naming `id`: the artifacts are
// gone, so the subtree can be reclaimed.
//
// It names the operation rather than relying on position: a cascade reclaim can
// run for a long time, so several drops can be open at once and "the one before
// this" is not an answer.
struct PrepareCommit {
  ObjectId id;
};

// The store half of a frame is invariant 3b made concrete: the catalog commits
// first and the database's duckdb commit follows, carrying the store change and
// the new log position atomically, so a database whose committed position is
// behind the log tail replays exactly the frames in between. That works only if
// every record reconstructs into its store operation, so the operation travels
// in the frame beside the records -- `ALTER COLUMN TYPE` is the case that
// proves the point, because the resulting definition says where the column
// lands but not the USING expression that moves the data. One entry per
// operation: an op already names the database whose file holds the rows.
using Entry = std::variant<PutTable, PutEntry, DropObject, DropChildren,
                           DropPrepare, SetSequence, BumpSequence, DropSequence,
                           PrepareCommit, store_op::Targeted>;

// Written ahead of each entry.
enum class Tag : uint8_t {
  PutTable,
  PutEntry,
  DropObject,
  DropChildren,
  DropPrepare,
  SetSequence,
  BumpSequence,
  DropSequence,
  PrepareCommit,
  StoreOp,
};

// The object an entry names.
std::optional<ObjectId> IdOf(const Entry& entry) noexcept;

struct TaggedEntry {
  Tag tag{Tag::PrepareCommit};
  Entry entry;
};

// The catalog log is a sequence of frames, one per batch, each atomic. A
// frame's position is its index in that sequence, counting from one: it is what
// a database records when it commits the data half of the batch, and the unit
// the boot gap is measured in.
struct FrameHeader {
  uint64_t position = 0;
  // Every object id up to this one is spent, whether or not any record here
  // names it: an id names artifacts that outlive the transaction meant to
  // record them -- an iresearch directory, a search WAL shard -- so reissuing
  // one after a crash would collide with what is already on disk. Written
  // ahead of duckdb's allocator and max-merged at replay, so a DDL pays an
  // append only rarely, and a frame with no records at all is one of those
  // bumps and nothing else.
  uint64_t oid_horizon = 0;
  // Written by compaction: the resident state as of `position`, not a run of
  // log records. It carries the position rather than advancing it.
  bool snapshot = false;
};

struct ParsedFrame {
  FrameHeader header;
  std::vector<TaggedEntry> entries;
};

// A frame is [u8 version][u8 flags][u64 position][u64 oid_horizon][u32 count]
// [entry...], each entry [u8 tag][fields].
void SerializeEntries(FrameHeader header, std::span<const Entry> entries,
                      duckdb::MemoryStream& stream);
ParsedFrame ParseEntries(std::span<const uint8_t> frame);

}  // namespace sdb::catalog::wal
