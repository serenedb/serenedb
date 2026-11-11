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

#include "search/table_keys.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "catalog/base_catalog.h"
#include "catalog/dependency.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/constraint_keys.h"
#include "search/store.h"

namespace sdb::search {
namespace {

irs::bytes_view Bytes(std::string_view key) noexcept {
  return {reinterpret_cast<const irs::byte_type*>(key.data()), key.size()};
}

std::string_view Chars(irs::bytes_view term) noexcept {
  return {reinterpret_cast<const char*>(term.data()), term.size()};
}

const irs::TermReader* KeyField(const irs::SubReader& sub,
                                irs::field_id field) {
  for (const auto* reader : sub.fields(field)) {
    if (reader->meta().dict == connector::kConstraintKeyDict) {
      return reader;
    }
  }
  return nullptr;
}

std::string DefaultName(const catalog::SearchTableEntry& table,
                        std::span<const std::string> names,
                        std::string_view suffix) {
  return absl::StrCat(table.name.GetIdentifierName(), "_",
                      absl::StrJoin(names, "_"), "_", suffix);
}

const catalog::SearchTableEntry* TableByOid(
  const catalog::SearchTableEntry& table, duckdb::idx_t oid,
  duckdb::CatalogTransaction transaction) {
  if (!oid) {
    return nullptr;
  }
  const auto entry =
    table.catalog.Cast<catalog::BaseCatalog>().Oids().Find(transaction, oid);
  if (!entry || !catalog::IsSearchTable(*entry)) {
    return nullptr;
  }
  return &entry->Cast<catalog::SearchTableEntry>();
}

const catalog::SearchTableEntry* ReferencedTable(
  const catalog::SearchTableEntry& table,
  const duckdb::ForeignKeyConstraint& fk,
  duckdb::CatalogTransaction transaction) {
  if (fk.info.type == duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
    return &table;
  }
  return TableByOid(table, catalog::ReferencedTableOid(table, fk), transaction);
}

const duckdb::Constraint* ReferencedKey(
  const catalog::SearchTableEntry& parent,
  std::span<const duckdb::Identifier> columns) {
  const auto& parent_columns = parent.GetColumns();
  for (const auto& constraint : parent.GetConstraints()) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto indexes =
      constraint->Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(
        parent_columns);
    if (indexes.size() == columns.size() &&
        absl::c_all_of(columns, [&](const duckdb::Identifier& name) {
          return parent_columns.ColumnExists(name) &&
                 absl::c_linear_search(
                   indexes, parent_columns.GetColumn(name).Logical());
        })) {
      return constraint.get();
    }
  }
  return nullptr;
}

}  // namespace

TableKeys ResolveTableKeys(const catalog::SearchTableEntry& table,
                           duckdb::CatalogTransaction transaction) {
  TableKeys keys;
  const auto& columns = table.GetColumns();
  const auto add_column = [&](KeySpec& spec,
                              const duckdb::ColumnDefinition& column) {
    spec.key.columns.push_back(connector::TableColumnId(column));
    spec.names.push_back(column.Name().GetIdentifierName());
    spec.types.push_back(column.Type());
  };
  for (const auto& constraint : table.GetConstraints()) {
    if (constraint->type == duckdb::ConstraintType::UNIQUE) {
      const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
      KeySpec spec{.kind = KeyKind::Unique,
                   .key = {.field = constraint->oid},
                   .constraint = constraint->constraint_name};
      for (const auto index : unique.GetLogicalIndexes(columns)) {
        add_column(spec, columns.GetColumn(index));
      }
      if (spec.constraint.empty()) {
        spec.constraint =
          unique.IsPrimaryKey()
            ? absl::StrCat(table.name.GetIdentifierName(), "_pkey")
            : DefaultName(table, spec.names, "key");
      }
      keys.specs.push_back(std::move(spec));
      continue;
    }
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    if (!fk.info.IsAppendConstraint()) {
      continue;
    }
    KeySpec spec{.kind = KeyKind::Reference,
                 .key = {.field = constraint->oid},
                 .constraint = constraint->constraint_name,
                 .referenced = fk.info.table};
    if (spec.constraint.empty()) {
      std::vector<std::string> names;
      for (const auto& name : fk.fk_columns) {
        names.push_back(name.GetIdentifierName());
      }
      spec.constraint = DefaultName(table, names, "fkey");
    }
    const auto* parent = ReferencedTable(table, fk, transaction);
    const auto* referenced =
      parent ? ReferencedKey(*parent, fk.pk_columns) : nullptr;
    if (parent && !referenced) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_FOREIGN_KEY),
        ERR_MSG("there is no unique constraint matching given keys for "
                "referenced table \"",
                parent->name.GetIdentifierName(), "\""));
    }
    if (!referenced) {
      for (const auto& name : fk.fk_columns) {
        add_column(spec, columns.GetColumn(name));
      }
      keys.specs.push_back(std::move(spec));
      continue;
    }
    spec.self_reference =
      fk.info.type == duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
    spec.referenced_table = parent->oid;
    spec.referenced_field = referenced->oid;
    const auto& parent_columns = parent->GetColumns();
    std::vector<duckdb::LogicalType> own_types;
    for (const auto index :
         referenced->Cast<duckdb::UniqueConstraint>().GetLogicalIndexes(
           parent_columns)) {
      const auto& parent_column = parent_columns.GetColumn(index);
      const auto at =
        absl::c_find_if(fk.pk_columns,
                        [&](const auto& name) {
                          return parent_columns.GetColumn(name).Logical() ==
                                 index;
                        }) -
        fk.pk_columns.begin();
      const auto& column = columns.GetColumn(fk.fk_columns[at]);
      spec.key.columns.push_back(connector::TableColumnId(column));
      spec.names.push_back(column.Name().GetIdentifierName());
      spec.types.push_back(parent_column.Type());
      own_types.push_back(column.Type());
    }
    if (own_types != spec.types) {
      spec.key.types = spec.types;
    }
    auto referencing = std::make_shared<const Referencing>(
      Referencing{.table = table.Storage()->GetId(),
                  .field = spec.key.field,
                  .referenced = spec.referenced_field});
    parent->Storage()->AddReferencing(referencing);
    keys.referencing.push_back(std::move(referencing));
    keys.specs.push_back(std::move(spec));
  }
  return keys;
}

const catalog::SearchTableEntry* ReferencedTable(
  const catalog::SearchTableEntry& table, const KeySpec& spec,
  duckdb::CatalogTransaction transaction) {
  return TableByOid(table,
                    spec.self_reference ? table.oid : spec.referenced_table,
                    transaction);
}

std::vector<ReferencingKey> LiveReferencing(
  const catalog::SearchTableEntry& table,
  std::span<const std::shared_ptr<const Referencing>> referencing,
  irs::field_id field, duckdb::CatalogTransaction transaction) {
  std::vector<ReferencingKey> live;
  auto& oids = table.catalog.Cast<catalog::BaseCatalog>().Oids();
  for (const auto& reference : referencing) {
    if (reference->referenced != field ||
        absl::c_any_of(live, [&](const ReferencingKey& key) {
          return key.table->oid == reference->table &&
                 key.spec->key.field == reference->field;
        })) {
      continue;
    }
    auto entry = oids.Find(transaction, reference->table);
    if (!entry || !catalog::IsSearchTable(*entry)) {
      continue;
    }
    auto& child = entry->Cast<catalog::SearchTableEntry>();
    const auto& specs = child.KeySpecs();
    const auto spec = absl::c_find_if(specs, [&](const KeySpec& candidate) {
      return candidate.kind == KeyKind::Reference &&
             candidate.key.field == reference->field &&
             candidate.referenced_field == field;
    });
    if (spec == specs.end()) {
      continue;
    }
    const auto* referenced = ReferencedTable(child, *spec, transaction);
    if (referenced && referenced->Storage() == table.Storage()) {
      live.push_back({.table = &child, .spec = &*spec});
    }
  }
  return live;
}

KeySpec UniqueIndexKey(const catalog::SearchTableEntry& table,
                       irs::field_id field, std::string name,
                       std::span<const UniqueIndexPart> parts) {
  KeySpec spec{.kind = KeyKind::Unique,
               .key = {.field = field},
               .constraint = std::move(name)};
  for (const auto& part : parts) {
    if (!part.expression.empty()) {
      spec.key.columns.push_back(irs::field_limits::invalid());
      spec.names.push_back(part.text);
      spec.types.push_back(part.type);
      continue;
    }
    const auto& column = table.GetColumns().GetColumn(part.column);
    spec.key.columns.push_back(connector::TableColumnId(column));
    spec.names.push_back(column.Name().GetIdentifierName());
    spec.types.push_back(column.Type());
  }
  if (absl::c_any_of(parts, [](const UniqueIndexPart& part) {
        return !part.expression.empty();
      })) {
    for (const auto& part : parts) {
      spec.key.expressions.push_back(part.expression);
    }
  }
  return spec;
}

std::optional<KeySpec> IndexKeySpec(const catalog::SearchTableEntry& table,
                                    const catalog::ConstraintKey& key,
                                    duckdb::CatalogTransaction transaction) {
  transaction.view.visibility_bound =
    duckdb::VisibilityBound::IncludingUncommitted();
  const auto entry = table.catalog.Cast<catalog::BaseCatalog>().Oids().Find(
    transaction, key.field);
  if (!entry || entry->type != duckdb::CatalogType::INDEX_ENTRY) {
    return std::nullopt;
  }
  KeySpec spec{.kind = KeyKind::Unique,
               .key = key,
               .constraint = entry->name.GetIdentifierName()};
  const auto& config = entry->Cast<catalog::InvertedIndexEntry>().Config();
  for (size_t i = 0; i != key.columns.size(); ++i) {
    if (i < key.expressions.size() && !key.expressions[i].empty()) {
      const auto found =
        absl::c_find_if(config->keys, [&](const catalog::InvertedIndexKey& k) {
          return k.normalized_expression == key.expressions[i];
        });
      SDB_ASSERT(found != config->keys.end());
      spec.names.push_back(found->expression_text);
      spec.types.push_back(found->type);
      continue;
    }
    for (const auto& column : table.GetColumns().Logical()) {
      if (connector::TableColumnId(column) == key.columns[i]) {
        spec.names.push_back(column.Name().GetIdentifierName());
        spec.types.push_back(column.Type());
      }
    }
  }
  return spec;
}

void VerifyUniqueKey(duckdb::ClientContext& context,
                     const irs::DirectoryReader& reader, const KeySpec& spec) {
  std::vector<KeyProbe::Segment> segments;
  for (const auto& segment : reader) {
    segments.push_back({.sub = &segment});
  }
  ForEachSegmentKey(
    segments, spec.key.field, [&](std::string_view key, size_t rows) {
      if (rows > 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
          ERR_MSG("could not create unique index \"", spec.constraint, "\""),
          ERR_DETAIL(
            "Key ",
            connector::ConstraintKeyText(context, key, spec.names, spec.types),
            " is duplicated."));
      }
      return true;
    });
}

void KeyProbe::Add(const Segment& segment) {
  if (const auto* field = KeyField(*segment.sub, _field)) {
    _segments.push_back({.unit = segment.unit,
                         .field = field,
                         .masks = {.deleted = segment.sub->docs_mask(),
                                   .removed = segment.removed}});
  }
}

void KeyProbe::AddLive(const LiveUnits& live, const LiveUnits* since) {
  for (const auto& unit : live.Units()) {
    if (since && since->Find(unit.unit->id)) {
      continue;
    }
    Add(Segment{.unit = unit.unit->id,
                .sub = unit.unit->reader.get(),
                .removed = unit.removed});
  }
}

void KeyProbe::ForEach(std::string_view key,
                       absl::FunctionRef<bool(const KeyHolder&)> visit) const {
  bool more = true;
  for (const auto& source : _segments) {
    source.field->ReadDocs(Bytes(key), [&](irs::doc_id_t doc) {
      if (source.masks.Contains(doc)) {
        return true;
      }
      more = visit({.unit = source.unit, .doc = doc});
      return more;
    });
    if (!more) {
      return;
    }
  }
  for (const auto& hot : _hots) {
    hot.keys->ForEach(_field, key, [&](irs::doc_id_t doc) {
      const auto row = doc - irs::doc_limits::min();
      if (row >= hot.docs || (hot.removed && hot.removed->Contains(doc))) {
        return true;
      }
      more = visit({.unit = hot.unit, .doc = doc});
      return more;
    });
    if (!more) {
      return;
    }
  }
}

bool KeyProbe::Exists(std::string_view key) const {
  bool found = false;
  ForEach(key, [&](const KeyHolder&) {
    found = true;
    return false;
  });
  return found;
}

void ForEachSegmentKey(
  std::span<const KeyProbe::Segment> segments, irs::field_id field,
  absl::FunctionRef<bool(std::string_view key, size_t rows)> visit,
  KeyRange range) {
  struct Cursor {
    std::unique_ptr<const irs::DocumentMask> merged;
    const irs::DocumentMask* gone = nullptr;
    irs::SeekTermIterator::ptr terms;
    irs::TermPostings::ptr postings;
  };
  std::vector<Cursor> cursors;
  cursors.reserve(segments.size());
  std::vector<size_t> heap;
  heap.reserve(segments.size());
  for (const auto& segment : segments) {
    const auto* reader = KeyField(*segment.sub, field);
    if (!reader) {
      continue;
    }
    auto terms = reader->iterator();
    if (!terms) {
      continue;
    }
    const bool positioned =
      range.from.empty()
        ? terms->next()
        : terms->seek_ge(Bytes(range.from)) != irs::SeekResult::End;
    if (positioned) {
      const auto* deleted = segment.sub->docs_mask();
      std::unique_ptr<const irs::DocumentMask> merged;
      if (deleted && segment.removed) {
        irs::DocumentMaskBuilder gone{*deleted};
        gone.Merge(*segment.removed);
        merged =
          std::make_unique<const irs::DocumentMask>(std::move(gone).Finish());
      }
      const auto* gone =
        merged ? merged.get() : (deleted ? deleted : segment.removed);
      heap.push_back(cursors.size());
      cursors.push_back(
        {.merged = std::move(merged), .gone = gone, .terms = std::move(terms)});
    }
  }
  const auto later = [&](size_t lhs, size_t rhs) {
    return cursors[lhs].terms->value() > cursors[rhs].terms->value();
  };
  absl::c_make_heap(heap, later);
  std::vector<size_t> at;
  at.reserve(cursors.size());
  irs::doc_id_t docs[irs::doc_limits::kBlockSize];
  while (!heap.empty()) {
    const auto least = cursors[heap.front()].terms->value();
    if (!range.to.empty() && Chars(least) >= range.to) {
      return;
    }
    at.clear();
    do {
      absl::c_pop_heap(heap, later);
      at.push_back(heap.back());
      heap.pop_back();
    } while (!heap.empty() && cursors[heap.front()].terms->value() == least);
    size_t rows = 0;
    for (const auto i : at) {
      auto& cursor = cursors[i];
      cursor.postings = cursor.terms->ReusePostings(irs::IndexFeatures::None,
                                                    std::move(cursor.postings));
      auto gone = irs::MakeGenericDocsMask(cursor.gone);
      while (const auto n = cursor.postings->NextDocs(docs, nullptr)) {
        rows += n - gone.CountMasked(docs, n);
      }
    }
    if (rows != 0 && !visit(Chars(least), rows)) {
      return;
    }
    for (const auto i : at) {
      if (cursors[i].terms->next()) {
        heap.push_back(i);
        absl::c_push_heap(heap, later);
      }
    }
  }
}

uint64_t SegmentKeyCount(std::span<const KeyProbe::Segment> segments,
                         irs::field_id field) {
  uint64_t count = 0;
  for (const auto& segment : segments) {
    if (const auto* reader = KeyField(*segment.sub, field)) {
      count += reader->size();
    }
  }
  return count;
}

std::vector<std::string> SplitSegmentKeys(
  std::span<const KeyProbe::Segment> segments, irs::field_id field,
  std::span<const std::string> keys, size_t parts) {
  constexpr uint64_t kSamplesPerPart = 64;
  constexpr uint64_t kSampleRunBytes = 256;
  const auto total = SegmentKeyCount(segments, field) + keys.size();
  const auto wanted = parts * kSamplesPerPart;
  std::vector<std::string> samples;
  samples.reserve(wanted * 2);
  for (const auto& segment : segments) {
    const auto* reader = KeyField(*segment.sub, field);
    if (!reader || reader->size() == 0) {
      continue;
    }
    const auto share = std::max<uint64_t>(1, wanted * reader->size() / total);
    reader->SampleTerms(share * kSampleRunBytes,
                        [&](std::span<const irs::bytes_view> run) {
                          for (const auto term : run) {
                            samples.emplace_back(Chars(term));
                          }
                        });
  }
  if (!keys.empty()) {
    const auto share = std::max<uint64_t>(1, wanted * keys.size() / total);
    const auto stride = std::max<uint64_t>(1, keys.size() / share);
    for (size_t i = 0; i < keys.size(); i += stride) {
      samples.push_back(keys[i]);
    }
  }
  absl::c_sort(samples);
  samples.erase(std::ranges::unique(samples).begin(), samples.end());
  std::vector<std::string> bounds;
  if (samples.empty()) {
    return bounds;
  }
  for (size_t part = 1; part < parts; ++part) {
    auto& bound = samples[part * samples.size() / parts];
    if (!bound.empty() && (bounds.empty() || bounds.back() < bound)) {
      bounds.push_back(std::move(bound));
    }
  }
  return bounds;
}

}  // namespace sdb::search
