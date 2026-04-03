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

#include <velox/type/SimpleFunctionApi.h>

#include <memory>

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
    VELOX_FAIL("Casting for INTERVAL type is not implemented");
  }

  velox::AbstractInputGeneratorPtr getInputGenerator(
    const velox::InputGeneratorConfig& /*config*/) const final {
    VELOX_FAIL("Input generation for INTERVAL type is not implemented");
  }
};

#define SDB_DEFINE_PG_TYPE(base, UPPER, CamelName, PG_NAME)                   \
  class CamelName##Type final : public base {                                 \
    CamelName##Type() = default;                                              \
                                                                              \
   public:                                                                    \
    static constexpr std::shared_ptr<const CamelName##Type> get() {           \
      static constexpr CamelName##Type kInstance;                             \
      return {std::shared_ptr<const CamelName##Type>{}, &kInstance};          \
    }                                                                         \
    bool equivalent(const Type& other) const final { return this == &other; } \
    static constexpr std::string_view kTypeName = PG_NAME;                    \
    const char* name() const final { return kTypeName.data(); }               \
    std::string toString() const final { return name(); }                     \
    folly::dynamic serialize() const final {                                  \
      folly::dynamic obj = folly::dynamic::object;                            \
      obj["name"] = #CamelName "Type";                                        \
      obj["type"] = name();                                                   \
      return obj;                                                             \
    }                                                                         \
    static velox::TypePtr deserialize(const folly::dynamic&) {                \
      return CamelName##Type::get();                                          \
    }                                                                         \
  };                                                                          \
  velox::TypePtr UPPER() { return CamelName##Type::get(); }                   \
  bool Is##CamelName(const velox::TypePtr& type) { return type == UPPER(); }  \
  bool Is##CamelName(const velox::Type& type) {                               \
    return &type == UPPER().get();                                            \
  }                                                                           \
  class CamelName##TypeFactory : public velox::CustomTypeFactory {            \
   public:                                                                    \
    velox::TypePtr getType(                                                   \
      const std::vector<velox::TypeParameter>& parameters) const final {      \
      VELOX_CHECK(parameters.empty(),                                         \
                  PG_NAME " type does not take parameters");                  \
      return UPPER();                                                         \
    }                                                                         \
    velox::exec::CastOperatorPtr getCastOperator() const final {              \
      return nullptr;                                                         \
    }                                                                         \
    velox::AbstractInputGeneratorPtr getInputGenerator(                       \
      const velox::InputGeneratorConfig&) const final {                       \
      VELOX_FAIL("Input generation for " PG_NAME " type is not implemented"); \
    }                                                                         \
  }

SDB_DEFINE_PG_TYPE(velox::VarcharType, PGUNKNOWN, Unknown, "PG_UNKNOWN");
SDB_DEFINE_PG_TYPE(velox::VarcharType, PGNAME, Name, "PG_NAME");

SDB_DEFINE_PG_TYPE(velox::BigintType, PGOID, Oid, "PG_OID");

SDB_DEFINE_PG_TYPE(velox::BigintType, REGPROC, Regproc, "PG_REGPROC");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGCLASS, Regclass, "PG_REGCLASS");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGTYPE, Regtype, "PG_REGTYPE");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGNAMESPACE, Regnamespace,
                   "PG_REGNAMESPACE");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGOPER, Regoper, "PG_REGOPER");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGOPERATOR, Regoperator,
                   "PG_REGOPERATOR");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGPROCEDURE, Regprocedure,
                   "PG_REGPROCEDURE");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGROLE, Regrole, "PG_REGROLE");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGCONFIG, Regconfig, "PG_REGCONFIG");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGDICTIONARY, Regdictionary,
                   "PG_REGDICTIONARY");
SDB_DEFINE_PG_TYPE(velox::BigintType, REGCOLLATION, Regcollation,
                   "PG_REGCOLLATION");

SDB_DEFINE_PG_TYPE(velox::BigintType, PGTID, Tid, "PG_TID");
SDB_DEFINE_PG_TYPE(velox::BigintType, PGCID, Cid, "PG_CID");
SDB_DEFINE_PG_TYPE(velox::BigintType, PGXID, Xid, "PG_XID");
SDB_DEFINE_PG_TYPE(velox::BigintType, PGXID8, Xid8, "PG_XID8");

#undef SDB_DEFINE_PG_TYPE

void RegisterTypes() {
  velox::registerCustomType(IntervalTrait::typeName,
                            std::make_unique<const IntervalTypeFactory>());

  velox::registerCustomType(UnknownTrait::typeName,
                            std::make_unique<const UnknownTypeFactory>());
  velox::registerCustomType(NameTrait::typeName,
                            std::make_unique<const NameTypeFactory>());

  velox::registerCustomType(OidTrait::typeName,
                            std::make_unique<const OidTypeFactory>());

  velox::registerCustomType(RegprocTrait::typeName,
                            std::make_unique<const RegprocTypeFactory>());
  velox::registerCustomType(RegclassTrait::typeName,
                            std::make_unique<const RegclassTypeFactory>());
  velox::registerCustomType(RegtypeTrait::typeName,
                            std::make_unique<const RegtypeTypeFactory>());
  velox::registerCustomType(RegnamespaceTrait::typeName,
                            std::make_unique<const RegnamespaceTypeFactory>());
  velox::registerCustomType(RegoperTrait::typeName,
                            std::make_unique<const RegoperTypeFactory>());
  velox::registerCustomType(RegoperatorTrait::typeName,
                            std::make_unique<const RegoperatorTypeFactory>());
  velox::registerCustomType(RegprocedureTrait::typeName,
                            std::make_unique<const RegprocedureTypeFactory>());
  velox::registerCustomType(RegroleTrait::typeName,
                            std::make_unique<const RegroleTypeFactory>());
  velox::registerCustomType(RegconfigTrait::typeName,
                            std::make_unique<const RegconfigTypeFactory>());
  velox::registerCustomType(RegdictionaryTrait::typeName,
                            std::make_unique<const RegdictionaryTypeFactory>());
  velox::registerCustomType(RegcollationTrait::typeName,
                            std::make_unique<const RegcollationTypeFactory>());

  velox::registerCustomType(TidTrait::typeName,
                            std::make_unique<const TidTypeFactory>());
  velox::registerCustomType(CidTrait::typeName,
                            std::make_unique<const CidTypeFactory>());
  velox::registerCustomType(XidTrait::typeName,
                            std::make_unique<const XidTypeFactory>());
  velox::registerCustomType(Xid8Trait::typeName,
                            std::make_unique<const Xid8TypeFactory>());
}

}  // namespace sdb::pg
