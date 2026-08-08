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

#include "catalog/store/wal_entry.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "catalog/create_info_serde.h"
#include "catalog/database.h"
#include "catalog/duckdb_dependency.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"
#include "catalog/view.h"

namespace sdb::catalog::wal {
namespace {

// 3 added the resolved dependency list that follows every definition.
// 4 gave store columns the user's own names, which reshaped the store-op
// record: DropColumn carries the column id and RenameColumn is a new op.
// 5 made DropPrepare carry its own reclamation subtree, so an open drop no
// longer needs the definitions it removed to be readable after a restart.
// 6 gave every definition record its create-or-replace mode, so the applier
// stops inferring it from whether the id is already held.
// 7 added PutEntry, the record whose definition is the duckdb CreateInfo a
// catalog entry is built from -- roles moved to it first.
// 8 moved databases onto PutEntry, which also took the owner and ACL out of a
// database's own definition -- they now travel beside it like a role's.
// 9 moved schemas onto PutEntry, and their owner and ACL beside it.
// 10 moved text-search dictionaries onto PutEntry, taking the owner and ACL out
// of their own definition too.
// 11 moved user-defined types onto PutEntry: the record now carries duckdb's
// own CreateTypeInfo, with the ids and the owner and ACL beside it.
// 12 moved SQL functions onto PutEntry, carrying duckdb's CreateMacroInfo.
// 13 moved views onto PutEntry, carrying duckdb's CreateViewInfo -- and gave
// PutEntry the resolved dependency list a view's or a function's body needs,
// which a rename makes unrepeatable at boot.
// 14 moved foreign servers onto PutEntry, taking the owner and ACL out of their
// own definition too.
// 15 moved sequences onto PutEntry, carrying a duckdb::CreateSequenceInfo --
// duckdb's own plus the CACHE, the owning table and the stable id it has no
// room for -- with the owner and ACL beside it. A table's owned sequences carry
// the same definition inline on PutTable. 16 moved both index kinds onto
// PutEntry, carrying the CreateIndexInfo a catalog entry is built from, and
// deleted PutIndex. The relation an index covers is written ahead of the info:
// PutEntry names one parent and an index has two ancestors. 17 deleted
// PutObject: with every kind but a table on PutEntry, the record whose payload
// was a definition object was reachable only for a table, which has PutTable of
// its own. 18 taught PutEntry to carry a table: duckdb's own
// duckdb::CreateTableInfo, with the ids on the column and constraint structures
// it already has room for, and the per-column grants written after it -- a
// column has no entry of its own to keep them on, and duckdb's ColumnDefinition
// has nowhere to put them. 19 moved PutTable onto the same payload: every
// record's definition is now a CreateInfo. It keeps a record of its own only
// because the sequences a table's SERIAL columns own ride it. 20 was the last
// version keyed on a SereneDB object-type enum. 21 keys every record on
// duckdb::CatalogType instead -- different byte values, and one CatalogType
// covers both index kinds, so a record naming an index carries which of the two
// it is beside the type. 22 names the relation of a store op by id. It was
// written as the store table's name and parsed back to the id on the way out, a
// round trip through a spelling no reader wanted.
constexpr uint8_t kEntryVersion = 31;

constexpr uint8_t kFrameSnapshot = 1U << 0U;

void Write(duckdb::MemoryStream& s, ObjectId id) { s.Write<uint64_t>(id.id()); }

ObjectId ReadId(duckdb::MemoryStream& s) {
  return ObjectId{s.Read<uint64_t>()};
}

void WriteDependencies(const duckdb::LogicalDependencyList& deps,
                       duckdb::MemoryStream& stream) {
  stream.Write<uint32_t>(static_cast<uint32_t>(deps.Set().size()));
  std::vector<ObjectId> ids;
  ids.reserve(deps.Set().size());
  for (const auto& dep : deps.Set()) {
    ids.push_back(catalog::DependencyInfoId(dep.entry));
  }
  // The set is unordered, so a frame is only byte-stable once they are sorted.
  std::ranges::sort(ids);
  for (const auto id : ids) {
    Write(stream, id);
  }
}

duckdb::LogicalDependencyList ReadDependencies(duckdb::MemoryStream& stream) {
  const auto count = stream.Read<uint32_t>();
  std::vector<ObjectId> ids;
  ids.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    ids.push_back(ReadId(stream));
  }
  return catalog::DependencyList(ids);
}

void WriteAcl(AclView acl, duckdb::MemoryStream& stream) {
  stream.Write<uint32_t>(static_cast<uint32_t>(acl.size()));
  for (const auto& item : acl) {
    stream.Write<uint64_t>(item.grantee);
    stream.Write<uint64_t>(item.grantor);
    stream.Write<uint64_t>(std::to_underlying(item.privs));
    stream.Write<uint64_t>(std::to_underlying(item.grant_option));
  }
}

Acl ReadAcl(duckdb::MemoryStream& stream) {
  Acl acl;
  const auto count = stream.Read<uint32_t>();
  acl.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    auto& item = acl.emplace_back();
    item.grantee = stream.Read<uint64_t>();
    item.grantor = stream.Read<uint64_t>();
    item.privs = static_cast<AclMode>(stream.Read<uint64_t>());
    item.grant_option = static_cast<AclMode>(stream.Read<uint64_t>());
  }
  return acl;
}

// A CreateInfo goes into the frame inline, with the reader picking the concrete
// kind from the record's own type field.
//
// `parent_id`/`id` are what the record already states beside the payload, and
// ReadInfoInline stamps them back onto whatever it reads. Stamping them here
// too is what makes a frame byte-stable: an info that reached this writer from
// a live definition carries none, one that came back off the log carries both,
// and a compaction that rewrites the second must produce the bytes the first
// did.
void WriteInfoInline(duckdb::CatalogType type, ObjectId parent_id, ObjectId id,
                     const duckdb::CreateInfo& info,
                     duckdb::MemoryStream& stream) {
  auto& stamped = const_cast<duckdb::CreateInfo&>(info);
  stamped.oid = id.id();
  stamped.parent_oid = parent_id.id();
  if (type == duckdb::CatalogType::INDEX_ENTRY) {
    // Ahead of the payload: the record names the schema, and an index's other
    // ancestor -- the relation its rows belong to -- has nowhere else to go.
    Write(stream,
          basics::downCast<const CreateIndexInfoBase>(info).GetRelationId());
    // And what its expression keys resolved to. Every other kind's payload is
    // duckdb's own CreateInfo::Serialize, which carries `dependencies`; an
    // index serializes its own and would drop them.
    WriteDependencies(info.dependencies, stream);
  }
  duckdb::BinarySerializer serializer{stream, duckdb::VersionStorageOptions()};
  switch (type) {
    case duckdb::CatalogType::INDEX_ENTRY:
      // An index writes a reflected tuple and opens nothing of its own.
      info.Serialize(serializer);
      return;
    case duckdb::CatalogType::ROLE_ENTRY:
    case duckdb::CatalogType::DATABASE_ENTRY:
    case duckdb::CatalogType::TOKENIZER_ENTRY:
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
    case duckdb::CatalogType::SCHEMA_ENTRY:
    case duckdb::CatalogType::SEQUENCE_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY:
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::VIEW_ENTRY:
      basics::WriteTuple(serializer, CreateInfoRef<duckdb::CreateInfo>{&info});
      return;
    case duckdb::CatalogType::TABLE_ENTRY:
      basics::WriteTuple(serializer, CreateInfoRef<duckdb::CreateInfo>{&info});
      return;
    default:
      SDB_FATAL(STARTUP, "catalog wal: object type ", static_cast<int>(type),
                " has no create info");
  }
}

std::shared_ptr<const duckdb::CreateInfo> ReadInfoPayload(
  duckdb::CatalogType type, bool inverted, ObjectId parent_id, ObjectId id,
  duckdb::MemoryStream& stream);

std::shared_ptr<const duckdb::CreateInfo> ReadInfoInline(
  duckdb::CatalogType type, bool inverted, ObjectId parent_id, ObjectId id,
  duckdb::MemoryStream& stream) {
  auto info = ReadInfoPayload(type, inverted, parent_id, id, stream);
  // The record names the identity beside the payload rather than inside it, and
  // the info is where every reader looks for it -- so this is the one place it
  // is stamped back on.
  auto& mutable_info = const_cast<duckdb::CreateInfo&>(*info);
  mutable_info.oid = id.id();
  mutable_info.parent_oid = parent_id.id();
  return info;
}

std::shared_ptr<const duckdb::CreateInfo> ReadInfoPayload(
  duckdb::CatalogType type, bool inverted, ObjectId parent_id, ObjectId id,
  duckdb::MemoryStream& stream) {
  const auto is_index = type == duckdb::CatalogType::INDEX_ENTRY;
  const auto relation_id = is_index ? ReadId(stream) : ObjectId{};
  auto dependencies =
    is_index ? ReadDependencies(stream) : duckdb::LogicalDependencyList{};
  duckdb::BinaryDeserializer src{stream};
  switch (type) {
    case duckdb::CatalogType::INDEX_ENTRY: {
      std::shared_ptr<duckdb::CreateInfo> index =
        inverted ? std::shared_ptr<
                     duckdb::CreateInfo>{CreateInvertedIndexInfo::Deserialize(
                     src, parent_id, id, relation_id)}
                 : std::shared_ptr<duckdb::CreateInfo>{
                     CreateSecondaryIndexInfo::Deserialize(src, parent_id, id,
                                                           relation_id)};
      index->dependencies = std::move(dependencies);
      return index;
    }
    case duckdb::CatalogType::ROLE_ENTRY:
    case duckdb::CatalogType::DATABASE_ENTRY:
    case duckdb::CatalogType::TOKENIZER_ENTRY:
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
    case duckdb::CatalogType::SCHEMA_ENTRY:
    case duckdb::CatalogType::SEQUENCE_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY:
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::VIEW_ENTRY: {
      // Read back through duckdb's own dispatch: no kind needs a reader of
      // ours, because the record already names the ids beside it.
      CreateInfoOwned<duckdb::CreateInfo> data;
      basics::ReadTuple(src, data);
      return std::shared_ptr<const duckdb::CreateInfo>{data.info.release()};
    }
    case duckdb::CatalogType::TABLE_ENTRY: {
      // The same dispatch, put back into our own info: a table's runtime state
      // and its per-column grants have nowhere to live on duckdb's.
      CreateInfoOwned<duckdb::CreateTableInfo> data;
      basics::ReadTuple(src, data);
      // duckdb's own dispatch already built the definition; the record adds
      // nothing to it but the identity ReadInfoInline stamps back.
      return std::shared_ptr<const duckdb::CreateTableInfo>{
        data.info.release()};
    }
    default:
      SDB_FATAL(STARTUP, "catalog wal: object type ", static_cast<int>(type),
                " has no create info");
  }
}

// Owner and ACL travel beside the CreateInfo rather than inside it: duckdb's
// CreateInfo has nowhere to put them, and the entry keeps them on itself.
void WritePermissions(const Permissions& perm, duckdb::MemoryStream& stream) {
  stream.Write<uint64_t>(perm.owner);
  WriteAcl(perm.acl, stream);
  // The per-column grants are part of the same permissions; the list is kept
  // in column order, so one catalog state writes one frame.
  stream.Write<uint32_t>(static_cast<uint32_t>(perm.column_acl.size()));
  for (const auto& column : perm.column_acl) {
    stream.Write<uint64_t>(column.catalog_oid);
    WriteAcl(column.acl, stream);
  }
}

Permissions ReadPermissions(duckdb::MemoryStream& stream) {
  Permissions perm;
  perm.owner = stream.Read<uint64_t>();
  perm.acl = ReadAcl(stream);
  const auto columns = stream.Read<uint32_t>();
  perm.column_acl.reserve(columns);
  for (uint32_t i = 0; i < columns; ++i) {
    auto& column = perm.column_acl.emplace_back();
    column.catalog_oid = stream.Read<uint64_t>();
    column.acl = ReadAcl(stream);
  }
  return perm;
}

}  // namespace
namespace {

// The tag `entry` writes under.
Tag TagOf(const Entry& entry) noexcept {
  return std::visit(
    [](const auto& e) {
      using T = std::decay_t<decltype(e)>;
      if constexpr (std::is_same_v<T, wal::PutTable>) {
        return Tag::PutTable;
      } else if constexpr (std::is_same_v<T, wal::PutEntry>) {
        return Tag::PutEntry;
      } else if constexpr (std::is_same_v<T, wal::DropObject>) {
        return Tag::DropObject;
      } else if constexpr (std::is_same_v<T, wal::DropChildren>) {
        return Tag::DropChildren;
      } else if constexpr (std::is_same_v<T, wal::DropPrepare>) {
        return Tag::DropPrepare;
      } else if constexpr (std::is_same_v<T, wal::SetSequence>) {
        return Tag::SetSequence;
      } else if constexpr (std::is_same_v<T, wal::BumpSequence>) {
        return Tag::BumpSequence;
      } else if constexpr (std::is_same_v<T, wal::DropSequence>) {
        return Tag::DropSequence;
      } else if constexpr (std::is_same_v<T, wal::PrepareCommit>) {
        return Tag::PrepareCommit;
      } else if constexpr (std::is_same_v<T, store_op::Targeted>) {
        return Tag::StoreOp;
      } else {
        // No catch-all: a new entry type falling through here would be
        // silently tagged as something else.
        static_assert(false, "entry type has no tag");
      }
    },
    entry);
}

}  // namespace

void SerializeEntries(FrameHeader header, std::span<const Entry> entries,
                      duckdb::MemoryStream& stream) {
  stream.Write<uint8_t>(kEntryVersion);
  stream.Write<uint8_t>(header.snapshot ? kFrameSnapshot : uint8_t{0});
  stream.Write<uint64_t>(header.position);
  stream.Write<uint64_t>(header.oid_horizon);
  stream.Write<uint32_t>(static_cast<uint32_t>(entries.size()));
  for (const auto& entry : entries) {
    const auto tag = TagOf(entry);
    stream.Write<uint8_t>(static_cast<uint8_t>(tag));
    std::visit(
      [&](const auto& e) {
        using T = std::decay_t<decltype(e)>;
        if constexpr (std::is_same_v<T, wal::PutTable>) {
          Write(stream, e.schema_id);
          Write(stream, e.id);
          stream.Write<uint8_t>(static_cast<uint8_t>(e.mode));
          WriteInfoInline(duckdb::CatalogType::TABLE_ENTRY, e.schema_id, e.id,
                          *e.info, stream);
          WritePermissions(e.perm, stream);
          stream.Write<uint32_t>(static_cast<uint32_t>(e.sequences.size()));
          for (const auto& seq : e.sequences) {
            Write(stream, seq.id);
            stream.Write<uint64_t>(seq.seed);
            WriteInfoInline(duckdb::CatalogType::SEQUENCE_ENTRY, e.schema_id,
                            seq.id, *seq.info, stream);
            WritePermissions(seq.perm, stream);
          }
        } else if constexpr (std::is_same_v<T, wal::PutEntry>) {
          Write(stream, e.parent_id);
          stream.Write<uint8_t>(static_cast<uint8_t>(e.type));
          stream.Write<uint8_t>(static_cast<uint8_t>(e.inverted));
          Write(stream, e.id);
          stream.Write<uint8_t>(static_cast<uint8_t>(e.mode));
          WriteInfoInline(e.type, e.parent_id, e.id, *e.info, stream);
          WritePermissions(e.perm, stream);
        } else if constexpr (std::is_same_v<T, wal::DropObject>) {
          Write(stream, e.parent_id);
          stream.Write<uint8_t>(static_cast<uint8_t>(e.type));
          Write(stream, e.id);
        } else if constexpr (std::is_same_v<T, wal::DropChildren>) {
          Write(stream, e.parent_id);
        } else if constexpr (std::is_same_v<T, wal::DropPrepare>) {
          Write(stream, e.parent_id);
          stream.Write<uint8_t>(static_cast<uint8_t>(e.type));
          stream.Write<uint8_t>(static_cast<uint8_t>(e.inverted));
          Write(stream, e.id);
          Write(stream, e.database_id);
          Write(stream, e.schema_id);
          const auto nodes = e.subtree ? std::span{*e.subtree}
                                       : std::span<const wal::DropNode>{};
          stream.Write<uint32_t>(static_cast<uint32_t>(nodes.size()));
          for (const auto& node : nodes) {
            Write(stream, node.parent_id);
            Write(stream, node.id);
            stream.Write<uint8_t>(static_cast<uint8_t>(node.type));
            stream.Write<uint8_t>(static_cast<uint8_t>(node.engine));
            stream.Write<uint8_t>(static_cast<uint8_t>(node.inverted));
          }
        } else if constexpr (std::is_same_v<T, wal::SetSequence> ||
                             std::is_same_v<T, wal::BumpSequence>) {
          Write(stream, e.id);
          stream.Write<uint64_t>(e.value);
        } else if constexpr (std::is_same_v<T, wal::DropSequence> ||
                             std::is_same_v<T, wal::PrepareCommit>) {
          Write(stream, e.id);
        } else if constexpr (std::is_same_v<T, store_op::Targeted>) {
          store_op::SerializeOp(e, stream);
        } else {
          static_assert(false, "entry type is not serialized");
        }
      },
      entry);
  }
}

ParsedFrame ParseEntries(std::span<const uint8_t> frame) {
  duckdb::MemoryStream stream{const_cast<duckdb::data_t*>(frame.data()),
                              frame.size()};
  const auto version = stream.Read<uint8_t>();
  SDB_ENSURE(version == kEntryVersion, "catalog wal: unknown entry version ",
             version);
  ParsedFrame parsed;
  const auto flags = stream.Read<uint8_t>();
  parsed.header.snapshot = (flags & kFrameSnapshot) != 0;
  parsed.header.position = stream.Read<uint64_t>();
  parsed.header.oid_horizon = stream.Read<uint64_t>();
  const auto count = stream.Read<uint32_t>();
  // Every entry costs at least its tag byte, so a count past the frame is
  // corruption -- catch it before reserving for it.
  SDB_ENSURE(count <= frame.size(), "catalog wal: entry count ", count,
             " exceeds frame size");
  auto& entries = parsed.entries;
  entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    const auto tag = static_cast<Tag>(stream.Read<uint8_t>());
    switch (tag) {
      case Tag::PutTable: {
        wal::PutTable e;
        e.schema_id = ReadId(stream);
        e.id = ReadId(stream);
        e.mode = static_cast<PutMode>(stream.Read<uint8_t>());
        e.info = std::static_pointer_cast<const duckdb::CreateTableInfo>(
          ReadInfoInline(duckdb::CatalogType::TABLE_ENTRY, false, e.schema_id,
                         e.id, stream));
        e.perm = ReadPermissions(stream);
        const auto seqs = stream.Read<uint32_t>();
        e.sequences.reserve(seqs);
        for (uint32_t s = 0; s < seqs; ++s) {
          auto& seq = e.sequences.emplace_back();
          seq.id = ReadId(stream);
          seq.seed = stream.Read<uint64_t>();
          seq.info = std::static_pointer_cast<const duckdb::CreateSequenceInfo>(
            ReadInfoInline(duckdb::CatalogType::SEQUENCE_ENTRY, false,
                           e.schema_id, seq.id, stream));
          seq.perm = ReadPermissions(stream);
        }
        entries.push_back({tag, std::move(e)});
        break;
      }
      case Tag::PutEntry: {
        wal::PutEntry e;
        e.parent_id = ReadId(stream);
        e.type = static_cast<duckdb::CatalogType>(stream.Read<uint8_t>());
        e.inverted = stream.Read<uint8_t>() != 0;
        e.id = ReadId(stream);
        e.mode = static_cast<PutMode>(stream.Read<uint8_t>());
        e.info = ReadInfoInline(e.type, e.inverted, e.parent_id, e.id, stream);
        e.perm = ReadPermissions(stream);
        entries.push_back({tag, std::move(e)});
        break;
      }
      case Tag::DropObject: {
        wal::DropObject e;
        e.parent_id = ReadId(stream);
        e.type = static_cast<duckdb::CatalogType>(stream.Read<uint8_t>());
        e.id = ReadId(stream);
        entries.push_back({tag, e});
        break;
      }
      case Tag::DropChildren: {
        wal::DropChildren e;
        e.parent_id = ReadId(stream);
        entries.push_back({tag, e});
        break;
      }
      case Tag::DropPrepare: {
        wal::DropPrepare e;
        e.parent_id = ReadId(stream);
        e.type = static_cast<duckdb::CatalogType>(stream.Read<uint8_t>());
        e.inverted = stream.Read<uint8_t>() != 0;
        e.id = ReadId(stream);
        e.database_id = ReadId(stream);
        e.schema_id = ReadId(stream);
        const auto nodes = stream.Read<uint32_t>();
        if (nodes != 0) {
          auto subtree = std::make_shared<std::vector<wal::DropNode>>();
          subtree->reserve(nodes);
          for (uint32_t n = 0; n < nodes; ++n) {
            auto& node = subtree->emplace_back();
            node.parent_id = ReadId(stream);
            node.id = ReadId(stream);
            node.type =
              static_cast<duckdb::CatalogType>(stream.Read<uint8_t>());
            node.engine = static_cast<TableEngine>(stream.Read<uint8_t>());
            node.inverted = stream.Read<uint8_t>() != 0;
          }
          e.subtree = std::move(subtree);
        }
        entries.push_back({tag, std::move(e)});
        break;
      }
      case Tag::SetSequence: {
        wal::SetSequence e;
        e.id = ReadId(stream);
        e.value = stream.Read<uint64_t>();
        entries.push_back({tag, e});
        break;
      }
      case Tag::BumpSequence: {
        wal::BumpSequence e;
        e.id = ReadId(stream);
        e.value = stream.Read<uint64_t>();
        entries.push_back({tag, e});
        break;
      }
      case Tag::DropSequence: {
        wal::DropSequence e;
        e.id = ReadId(stream);
        entries.push_back({tag, e});
        break;
      }
      case Tag::PrepareCommit: {
        wal::PrepareCommit e;
        e.id = ReadId(stream);
        entries.push_back({tag, e});
        break;
      }
      case Tag::StoreOp:
        entries.push_back({tag, store_op::DeserializeOp(stream)});
        break;
      default:
        SDB_FATAL(STARTUP, "catalog wal: unknown entry tag ",
                  static_cast<int>(tag));
    }
  }
  return parsed;
}

std::optional<ObjectId> IdOf(const Entry& entry) noexcept {
  return std::visit(
    [](const auto& e) -> std::optional<ObjectId> {
      if constexpr (requires { e.id; }) {
        return e.id;
      } else {
        return std::nullopt;
      }
    },
    entry);
}

}  // namespace sdb::catalog::wal
