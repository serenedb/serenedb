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

#include "pg/types/domain.h"

#include <absl/strings/str_format.h>

#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/operator/cast_operators.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/column_list.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <utility>

namespace sdb::pg {
namespace {

struct DomainCastData : duckdb::BoundCastData {
  DomainCastData(duckdb::BoundCastInfo base_cast_p,
                 duckdb::LogicalType base_type_p, std::string domain_name_p,
                 bool not_null_p)
    : base_cast(std::move(base_cast_p)),
      base_type(std::move(base_type_p)),
      domain_name(std::move(domain_name_p)),
      not_null(not_null_p) {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const override {
    auto result = duckdb::make_uniq<DomainCastData>(base_cast.Copy(), base_type,
                                                    domain_name, not_null);
    result->check_names = check_names;
    for (const auto& check : checks) {
      result->checks.push_back(check->Copy());
    }
    return result;
  }

  duckdb::BoundCastInfo base_cast;
  duckdb::LogicalType base_type;
  std::string domain_name;
  bool not_null;
  duckdb::vector<std::string> check_names;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> checks;
};

struct DomainCastLocalState : duckdb::FunctionLocalState {
  duckdb::unique_ptr<duckdb::FunctionLocalState> base_state;
  duckdb::unique_ptr<duckdb::ExpressionExecutor> executor;
  duckdb::DataChunk chunk;
  duckdb::Vector check_result{duckdb::LogicalType::BOOLEAN};
};

duckdb::unique_ptr<DomainCastLocalState> CreateDomainCastLocalState(
  const DomainCastData& data, duckdb::optional_ptr<duckdb::ClientContext> context) {
  auto state = duckdb::make_uniq<DomainCastLocalState>();
  if (data.base_cast.HasInitLocalState()) {
    duckdb::CastLocalStateParameters base_parameters(
      context, data.base_cast.GetCastData());
    state->base_state = data.base_cast.InitLocalState(base_parameters);
  }
  if (!data.checks.empty()) {
    if (!context) {
      throw duckdb::InternalException(
        "CHECK constraints of domain %s need a client context",
        data.domain_name);
    }
    state->executor =
      duckdb::make_uniq<duckdb::ExpressionExecutor>(*context, data.checks);
    state->chunk.InitializeEmpty({data.base_type});
  }
  return state;
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitDomainCastLocalState(
  duckdb::CastLocalStateParameters& parameters) {
  return CreateDomainCastLocalState(
    parameters.cast_data->Cast<DomainCastData>(), parameters.context);
}

class DomainViolations {
 public:
  DomainViolations(duckdb::Vector& result, duckdb::idx_t count,
                   duckdb::CastParameters& parameters)
    : _result{result}, _count{count}, _parameters{parameters} {}

  template<typename Message>
  void Report(duckdb::idx_t row, int sqlstate, Message&& message) {
    if (!_parameters.error_message) {
      THROW_SQL_ERROR(ERR_CODE(sqlstate), ERR_MSG(message()));
    }
    duckdb::HandleCastError::AssignError(message(), _parameters);
    if (!_flat) {
      _result.Flatten(_count);
      _flat = true;
    }
    duckdb::FlatVector::SetNull(_result, row, true);
    _all_ok = false;
  }

  bool AllOk() const { return _all_ok; }

 private:
  duckdb::Vector& _result;
  duckdb::idx_t _count;
  duckdb::CastParameters& _parameters;
  bool _flat = false;
  bool _all_ok = true;
};

void CheckNotNull(const DomainCastData& data, duckdb::Vector& result,
                  duckdb::idx_t count, DomainViolations& violations) {
  const auto validity = result.Validity();
  if (!validity.CanHaveNull()) {
    return;
  }
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (validity.IsValid(row)) {
      continue;
    }
    violations.Report(row, ERRCODE_NOT_NULL_VIOLATION, [&] {
      return absl::StrFormat("domain %s does not allow null values",
                             data.domain_name);
    });
  }
}

void CheckConstraints(const DomainCastData& data, DomainCastLocalState& state,
                      duckdb::Vector& value, duckdb::idx_t count,
                      DomainViolations& violations) {
  auto& chunk = state.chunk;
  chunk.data[0].Reference(value);
  chunk.SetCardinality(count);
  state.executor->SetChunk(chunk);
  for (duckdb::idx_t check = 0; check < data.checks.size(); ++check) {
    state.executor->ExecuteExpression(check, state.check_result);
    duckdb::UnifiedVectorFormat format;
    state.check_result.ToUnifiedFormat(count, format);
    const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(format);
    for (duckdb::idx_t row = 0; row < count; ++row) {
      const auto idx = format.sel->get_index(row);
      if (!format.validity.RowIsValid(idx) || values[idx]) {
        continue;
      }
      violations.Report(row, ERRCODE_CHECK_VIOLATION, [&] {
        return absl::StrFormat(
          "value for domain %s violates check constraint \"%s\"",
          data.domain_name, data.check_names[check]);
      });
    }
  }
}

bool DomainCast(duckdb::Vector& source, duckdb::Vector& result,
                duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto& data = parameters.cast_data->Cast<DomainCastData>();
  duckdb::unique_ptr<DomainCastLocalState> owned_state;
  DomainCastLocalState* state = nullptr;
  if (parameters.local_state) {
    state = &parameters.local_state->Cast<DomainCastLocalState>();
  } else {
    owned_state = CreateDomainCastLocalState(data, parameters.context);
    state = owned_state.get();
  }

  duckdb::CastParameters base_parameters(
    parameters, data.base_cast.GetCastData(), state->base_state.get());
  duckdb::Vector value(data.base_type, count);
  const bool base_ok =
    data.base_cast.Cast(source, value, count, base_parameters);
  result.Reinterpret(value);

  DomainViolations violations(result, count, parameters);
  if (data.not_null) {
    CheckNotNull(data, result, count, violations);
  }
  if (!data.checks.empty()) {
    CheckConstraints(data, *state, value, count, violations);
  }
  return base_ok && violations.AllOk();
}

void BindDomainChecks(duckdb::ClientContext& context,
                      const duckdb::TypeCatalogEntry& domain,
                      DomainCastData& data) {
  const auto& catalog_name = domain.ParentCatalog().GetName();
  const auto schema_name = domain.ParentSchemaName();
  duckdb::vector<duckdb::CatalogSearchEntry> search_path;
  search_path.emplace_back(catalog_name, schema_name);
  if (schema_name != duckdb::Identifier::DefaultSchema()) {
    search_path.emplace_back(catalog_name, duckdb::Identifier::DefaultSchema());
  }
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->EntryRetriever().SetSearchPath(std::move(search_path));

  duckdb::ColumnList columns;
  columns.AddColumn(
    duckdb::ColumnDefinition(duckdb::Identifier("value"), data.base_type));
  duckdb::physical_index_set_t bound_columns;
  duckdb::CheckBinder check_binder(*binder, context, domain.name, columns,
                                   bound_columns);
  check_binder.target_type = duckdb::LogicalType::BOOLEAN;

  for (const auto& constraint : domain.domain_info->constraints) {
    if (constraint.type != duckdb::ConstraintType::CHECK) {
      continue;
    }
    auto expression = constraint.expression->Copy();
    data.checks.push_back(check_binder.Bind(expression));
    data.check_names.push_back(constraint.name);
  }
}

duckdb::BoundCastInfo BindDomainCast(duckdb::BindCastInput& input,
                                     const duckdb::LogicalType& source,
                                     const duckdb::LogicalType& target) {
  if (!duckdb::ExtensionTypeInfo::IsDomain(target.GetExtensionInfo()) ||
      !input.context) {
    return duckdb::BoundCastInfo(nullptr);
  }
  auto& context = *input.context;
  auto domain = duckdb::TypeCatalogEntry::TryGetDomain(context, target);
  if (!domain) {
    throw duckdb::InternalException("domain of type %s is not found",
                                    target.ToString());
  }
  const auto& domain_info = *domain->domain_info;
  if (domain_info.base_type.id() == duckdb::LogicalTypeId::INVALID) {
    throw duckdb::InternalException("domain %s has no base type",
                                    domain->name.GetIdentifierName());
  }

  auto base_cast = input.GetCastFunction(source, domain_info.base_type);
  auto data = duckdb::make_uniq<DomainCastData>(
    std::move(base_cast), domain_info.base_type,
    domain->name.GetIdentifierName(), domain_info.IsNotNull());
  BindDomainChecks(context, *domain, *data);

  duckdb::BoundCastInfo result(DomainCast, std::move(data),
                               InitDomainCastLocalState);
  result.checks_domain = true;
  return result;
}

}  // namespace

void RegisterDomain(duckdb::CastFunctionSet& casts) {
  casts.RegisterBindCastFunction(BindDomainCast);
}

}  // namespace sdb::pg
