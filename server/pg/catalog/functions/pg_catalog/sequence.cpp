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

#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <optional>

#include "auth/role_closure.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/connection_context.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

using duckdb::AclMode;
using duckdb::CatalogType;

struct SequenceProgress {
  int64_t last_value;
  bool is_called;
};

SequenceProgress ProgressOf(const duckdb::SequenceData& data) {
  if (data.last_value && data.Successor(*data.last_value) == data.counter) {
    return {.last_value = *data.last_value, .is_called = true};
  }
  return {.last_value = data.counter, .is_called = false};
}

bool Allows(const std::shared_ptr<const auth::RoleClosure>& closure,
            const duckdb::Permissions& permissions, CatalogType type,
            AclMode modes) {
  return !closure || closure->CanAny(type, permissions, modes);
}

const duckdb::SequenceCatalogEntry* ReadableSequence(
  const Session& session,
  const std::shared_ptr<const auth::RoleClosure>& closure, int64_t oid,
  AclMode modes) {
  const auto entry = EntryByOid(session, static_cast<uint64_t>(oid));
  if (!entry || entry->type != CatalogType::SEQUENCE_ENTRY ||
      !Allows(closure, entry->permissions, CatalogType::SEQUENCE_ENTRY,
              modes)) {
    return nullptr;
  }
  return &entry->Cast<duckdb::SequenceCatalogEntry>();
}

void PgSequenceLastValueFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  const auto access = auth::SessionClosure(state.GetContext());
  const auto oids = args.data[0].Values<int64_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    if (!oid.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto entry =
      EntryByOid(session, static_cast<uint64_t>(oid.GetValue()));
    if (!entry) {
      writer.WriteNull();
      continue;
    }
    if (entry->type != CatalogType::SEQUENCE_ENTRY) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
        ERR_MSG("\"", entry->name.GetIdentifierName(), "\" is not a sequence"));
    }
    if (!Allows(access, entry->permissions, CatalogType::SEQUENCE_ENTRY,
                AclMode::Select | AclMode::Usage)) {
      writer.WriteNull();
      continue;
    }
    const auto progress =
      ProgressOf(entry->Cast<duckdb::SequenceCatalogEntry>().GetData());
    if (progress.is_called) {
      writer.WriteValue(progress.last_value);
    } else {
      writer.WriteNull();
    }
  }
}

void PgGetSequenceDataFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  const auto access = auth::SessionClosure(state.GetContext());
  const auto oids = args.data[0].Values<int64_t>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorStructType<int64_t, bool>>(
      result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    if (!oid.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto* sequence =
      ReadableSequence(session, access, oid.GetValue(), AclMode::Select);
    writer.WriteValue([&](auto& last_value, auto& is_called) {
      if (!sequence) {
        last_value.WriteNull();
        is_called.WriteNull();
        return;
      }
      const auto progress = ProgressOf(sequence->GetData());
      last_value.WriteValue(progress.last_value);
      is_called.WriteValue(progress.is_called);
    });
  }
}

void PgSequenceParametersFunction(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  const auto access = auth::SessionClosure(state.GetContext());
  const auto oids = args.data[0].Values<int64_t>();
  auto writer = duckdb::FlatVector::Writer<duckdb::VectorStructType<
    int64_t, int64_t, int64_t, int64_t, bool, int64_t, int64_t>>(result,
                                                                 args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    if (!oid.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto relation =
      RelationByOid(session, static_cast<uint64_t>(oid.GetValue()));
    if (!relation) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_TABLE),
        ERR_MSG("relation with OID ", oid.GetValue(), " does not exist"));
    }
    const auto type =
      relation->entry ? relation->entry->type : CatalogType::TABLE_ENTRY;
    if (!Allows(access, *relation->permissions, type,
                AclMode::Select | AclMode::Update | AclMode::Usage)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for sequence ",
                              NameOf(*session.transaction, *relation).name));
    }
    if (relation->key || type != CatalogType::SEQUENCE_ENTRY) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("cache lookup failed for sequence ", oid.GetValue()));
    }
    const auto data =
      relation->entry->Cast<duckdb::SequenceCatalogEntry>().GetData();
    writer.WriteValue([&](auto& start, auto& minimum, auto& maximum,
                          auto& increment, auto& cycle, auto& cache,
                          auto& data_type) {
      start.WriteValue(data.start_value);
      minimum.WriteValue(data.min_value);
      maximum.WriteValue(data.max_value);
      increment.WriteValue(data.increment);
      cycle.WriteValue(data.cycle);
      cache.WriteValue(static_cast<int64_t>(data.cache));
      data_type.WriteValue(int64_t{kInt8});
    });
  }
}

struct LastValue {
  duckdb::idx_t sequence;
  int64_t value;
};

void LastvalFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& sequences = duckdb::SequenceSession::Get(context);
  std::optional<LastValue> last;
  {
    absl::MutexLock lock{&sequences.lock};
    if (sequences.last_used.IsValid()) {
      const auto it = sequences.values.find(sequences.last_used.GetIndex());
      if (it != sequences.values.end() && it->second.last) {
        last = LastValue{.sequence = it->first, .value = *it->second.last};
      }
    }
  }
  duckdb::optional_ptr<duckdb::CatalogEntry> entry;
  if (last) {
    entry = EntryByOid(SessionOf(state), last->sequence);
  }
  if (!entry) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("lastval is not yet defined in this session"));
  }
  if (!Allows(auth::SessionClosure(context), entry->permissions,
              CatalogType::SEQUENCE_ENTRY, AclMode::Select | AclMode::Usage)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for sequence ",
                            entry->name.GetIdentifierName()));
  }
  result.Reference(duckdb::Value::BIGINT(last->value),
                   duckdb::count_t(args.size()));
}

}  // namespace

void RegisterSequenceFunctions(duckdb::ExtensionLoader& loader) {
  using enum duckdb::FunctionStability;
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  const auto int8 = duckdb::LogicalType::BIGINT;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  duckdb::ScalarFunction last_value{
    {REGCLASS()}, int8, PgSequenceLastValueFunction};
  last_value.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_sequence_last_value", VOLATILE,
                 InitSession, {last_value});
  duckdb::ScalarFunction lastval{{}, int8, LastvalFunction};
  lastval.SetFallible();
  RegisterScalar(loader, pg_catalog, "lastval", VOLATILE, InitSession,
                 {lastval});
  duckdb::ScalarFunction sequence_data{
    {REGCLASS()},
    duckdb::LogicalType::STRUCT({{"last_value", int8}, {"is_called", boolean}}),
    PgGetSequenceDataFunction};
  sequence_data.GetSignature().SetRecordResult();
  RegisterScalar(loader, pg_catalog, "pg_get_sequence_data", VOLATILE,
                 InitSession, {sequence_data});
  duckdb::ScalarFunction parameters{{OID()},
                                    duckdb::LogicalType::STRUCT({
                                      {"start_value", int8},
                                      {"minimum_value", int8},
                                      {"maximum_value", int8},
                                      {"increment", int8},
                                      {"cycle_option", boolean},
                                      {"cache_size", int8},
                                      {"data_type", OID()},
                                    }),
                                    PgSequenceParametersFunction};
  parameters.SetFallible();
  parameters.GetSignature().SetRecordResult();
  RegisterScalar(loader, pg_catalog, "pg_sequence_parameters",
                 CONSISTENT_WITHIN_QUERY, InitSession, {parameters});
}

}  // namespace sdb::pg
