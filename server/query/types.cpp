////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "types.h"

#include <velox/expression/CastExpr.h>
#include <velox/type/StringView.h>
#include <velox/type/Type.h>

#include <memory>

#include "basics/assert.h"
#include "basics/logger/logger.h"

namespace sdb::aql {

// TODO: make separate type for this and inherit from velox::Type
velox::TypePtr COLLECTION() {
  static const velox::TypePtr kCollectionType =
    velox::ROW({"<aql_collection_internal_type>"}, velox::UNKNOWN());
  return kCollectionType;
}

bool IsCollection(const velox::TypePtr& type) {
  return type && *type == *COLLECTION();
}

}  // namespace sdb::aql

namespace sdb::pg {

// TODO: make separate PseudoType (inherited from velox::Type)
// and inherit VOID from it (pseudo-type is a PG special terminology)
velox::TypePtr VOID() { return velox::UNKNOWN(); }

bool IsVoid(const velox::TypePtr& type) { return type && type->isUnKnown(); }

velox::TypePtr PROCEDURE() {
  static const velox::TypePtr kProcedureType =
    velox::ROW({"<pg_procedure_internal_type>"}, velox::UNKNOWN());
  return kProcedureType;
}

bool IsProcedure(const velox::TypePtr& type) {
  return type && *type == *PROCEDURE();
}

class IntervalType final : public velox::HugeintType {
  IntervalType() = default;

 public:
  static constexpr std::shared_ptr<const IntervalType> get() {
    static constexpr IntervalType kInstance;
    return {std::shared_ptr<const IntervalType>{}, &kInstance};
  }

  static constexpr std::string_view kIntervalTypeName = "PG_INTERVAL";

  const char* name() const final { return kIntervalTypeName.data(); }

  std::string toString() const final { return name(); }

  folly::dynamic serialize() const final {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "IntervalType";
    obj["type"] = name();
    return obj;
  }

  static velox::TypePtr deserialize(const folly::dynamic& /*obj*/) {
    return IntervalType::get();
  }
};

velox::TypePtr INTERVAL() { return IntervalType::get(); }

bool IsInterval(const velox::TypePtr& type) { return type == INTERVAL(); }

bool IsInterval(const velox::Type& type) { return &type == INTERVAL().get(); }

class IntervalTypeFactory : public velox::CustomTypeFactory {
 public:
  IntervalTypeFactory() = default;

  velox::TypePtr getType(
    const std::vector<velox::TypeParameter>& parameters) const final {
    VELOX_CHECK(parameters.empty(), "INTERVAL type does not take parameters");
    return INTERVAL();
  }

  velox::exec::CastOperatorPtr getCastOperator() const final {
    VELOX_CHECK(false, "Casting for INTERVAL type is not implemented");
  }

  velox::AbstractInputGeneratorPtr getInputGenerator(
    const velox::InputGeneratorConfig& /*config*/) const final {
    VELOX_CHECK(false, "Input generation for INTERVAL type is not implemented");
  }
};

class UnknownType final : public velox::VarcharType {
  UnknownType() = default;

 public:
  static std::shared_ptr<const UnknownType> get() {
    VELOX_CONSTEXPR_SINGLETON UnknownType kInstance;
    return {std::shared_ptr<const UnknownType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override { return "PG_UNKNOWN"; }

  std::string toString() const override { return name(); }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  bool isOrderable() const override { return false; }
};

velox::TypePtr UNKNOWN() { return UnknownType::get(); }

bool IsUnknown(const velox::TypePtr& type) { return type == UNKNOWN(); }

bool IsUnknown(const velox::Type& type) { return &type == UNKNOWN().get(); }

class UnknownTypeCastOperator : public velox::exec::CastOperator {
  bool isSupportedFromType(const velox::TypePtr& other) const final {
    return other == velox::VARCHAR();
  }

  bool isSupportedToType(const velox::TypePtr& other) const final {
    return other == velox::VARCHAR();
  }

  void castTo(const velox::BaseVector& input, velox::exec::EvalCtx& context,
              const velox::SelectivityVector& rows,
              const velox::TypePtr& resultType,  // NOLINT
              velox::VectorPtr& result) const final {
    SDB_ASSERT(resultType);
    // SDB_PRINT("Result type = ", resultType->name());
    // TODO find out why self in resultType
    SDB_ASSERT(resultType == velox::VARCHAR() || resultType == pg::UNKNOWN());
    context.ensureWritable(rows, resultType, result);
    auto* flat_result =
      result->asChecked<velox::FlatVector<velox::StringView>>();
    flat_result->copy(&input, rows, nullptr);
  }

  void castFrom(const velox::BaseVector& input, velox::exec::EvalCtx& context,
                const velox::SelectivityVector& rows,
                const velox::TypePtr& resultType,  // NOLINT
                velox::VectorPtr& result) const final {
    SDB_ASSERT(resultType == velox::VARCHAR());
    context.ensureWritable(rows, resultType, result);
    auto* flat_result =
      result->asChecked<velox::FlatVector<velox::StringView>>();
    flat_result->copy(&input, rows, nullptr);
  }
};

class UnknownTypeFactory : public velox::CustomTypeFactory {
 public:
  UnknownTypeFactory() = default;

  velox::TypePtr getType(
    const std::vector<velox::TypeParameter>& parameters) const final {
    VELOX_CHECK(parameters.empty(), "UNKNOWN type does not take parameters");
    return UNKNOWN();
  }

  velox::exec::CastOperatorPtr getCastOperator() const final {
    // TODO it is not true, varchars should be able to be casted into UNKNOWN
    return std::make_shared<UnknownTypeCastOperator>();
  }

  velox::AbstractInputGeneratorPtr getInputGenerator(
    const velox::InputGeneratorConfig& /*config*/) const final {
    VELOX_CHECK(false, "Input generation for UNKNOWN type is not implemented");
  }
};

void RegisterTypes() {
  velox::registerCustomType(IntervalTrait::typeName,
                            std::make_unique<const IntervalTypeFactory>());
  velox::registerCustomType(UnknownTrait::typeName,
                            std::make_unique<const UnknownTypeFactory>());
}

}  // namespace sdb::pg
