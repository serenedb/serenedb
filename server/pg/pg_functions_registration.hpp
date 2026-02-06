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

#pragma once
#include "basics/fwd.h"
#include "query/types.h"
#include <axiom/optimizer/FunctionRegistry.h>
#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/dwio/parquet/RegisterParquetWriter.h>
#include <velox/dwio/text/RegisterTextReader.h>
#include <velox/dwio/text/RegisterTextWriter.h>
#include <velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <velox/functions/prestosql/registration/RegistrationFunctions.h>
#include <velox/functions/prestosql/types/HyperLogLogRegistration.h>
#include <velox/functions/prestosql/types/IPAddressRegistration.h>
#include <velox/functions/prestosql/types/IPPrefixRegistration.h>
#include <velox/functions/prestosql/types/JsonRegistration.h>
#include <velox/functions/prestosql/types/QDigestRegistration.h>
#include <velox/functions/prestosql/types/SfmSketchRegistration.h>
#include <velox/functions/prestosql/types/TDigestRegistration.h>
#include <velox/functions/prestosql/types/TimeWithTimezoneRegistration.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h>
#include <velox/functions/prestosql/types/UuidRegistration.h>
#include <velox/functions/prestosql/window/WindowFunctionsRegistration.h>
#include <velox/functions/sparksql/aggregates/Register.h>
#include <velox/functions/sparksql/registration/Register.h>
#include <velox/functions/sparksql/window/WindowFunctionsRegistration.h>
#include <velox/type/Timestamp.h>
#include <velox/type/Type.h>
#include <velox/type/TypeCoercer.h>
#include "pg/functions.h"

namespace sdb::pg {

inline velox::AllowedCoercions AllowedCoercions() {
  velox::AllowedCoercions coercions;
  static constexpr velox::CallableCost kNullCoercionCost = 1;

  auto add = [&](const velox::TypePtr& from,
                 const std::vector<velox::TypePtr>& to) {
    velox::CallableCost cost = kNullCoercionCost;
    for (const auto& to_type : to) {
      coercions.emplace(
        std::make_pair<std::string, std::string>(from->name(), to_type->name()),
        velox::Coercion{.type = to_type, .cost = ++cost});
    }
  };

  auto add_same_cost = [&](const velox::TypePtr& from,
                           const std::vector<velox::TypePtr>& to,
                           velox::CallableCost cost) {
    for (const auto& to_type : to) {
      coercions.emplace(
        std::make_pair<std::string, std::string>(from->name(), to_type->name()),
        velox::Coercion{.type = to_type, .cost = cost});
    }
  };

  add(velox::TINYINT(), {velox::SMALLINT(), velox::INTEGER(), velox::BIGINT(),
                         velox::HUGEINT(), velox::REAL(), velox::DOUBLE()});
  add(velox::SMALLINT(), {velox::INTEGER(), velox::BIGINT(), velox::HUGEINT(),
                          velox::REAL(), velox::DOUBLE()});
  add(velox::INTEGER(),
      {velox::BIGINT(), velox::HUGEINT(), velox::REAL(), velox::DOUBLE()});
  add(velox::BIGINT(), {velox::HUGEINT(), velox::REAL(), velox::DOUBLE()});
  add(velox::REAL(), {velox::DOUBLE()});
  add(velox::DATE(), {velox::TIMESTAMP()});

  add_same_cost(
    PG_UNKNOWN(),
    {velox::VARCHAR(), velox::TINYINT(), velox::SMALLINT(), velox::INTEGER(),
     velox::BIGINT(), velox::HUGEINT(), velox::REAL(), velox::DOUBLE(),
     velox::BOOLEAN(), velox::TIMESTAMP(), velox::DATE(), INTERVAL()},
    kNullCoercionCost + 1);

  return coercions;
}

inline void RegisterVeloxFunctionsAndTypes() {
  velox::Type::registerSerDe();

  velox::text::registerTextReaderFactory();
  velox::text::registerTextWriterFactory();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();

  // TODO(mbkkt) velox::registerGeometryType();
  velox::registerHyperLogLogType();
  velox::registerIPAddressType();
  velox::registerIPPrefixType();
  velox::registerJsonType();
  velox::registerQDigestType();
  velox::registerSfmSketchType();
  velox::registerTDigestType();
  velox::registerTimeWithTimezoneType();
  velox::registerTimestampWithTimeZoneType();
  velox::registerUuidType();

  velox::functions::sparksql::registerFunctions("spark_");
  velox::functions::aggregate::sparksql::registerAggregateFunctions("spark_");
  velox::functions::window::sparksql::registerWindowFunctions("spark_");

  // Make Presto functions override Spark functions if both are registered
  // as Presto is more SQL compliant.
  velox::functions::prestosql::registerAllScalarFunctions("presto_");
  velox::aggregate::prestosql::registerAllAggregateFunctions("presto_");
  velox::window::prestosql::registerAllWindowFunctions("presto_");
  axiom::optimizer::FunctionRegistry::registerPrestoFunctions("presto_");

  pg::RegisterTypes();
  pg::functions::registerFunctions("pg_");
  velox::TypeCoercer::registerCoercions(AllowedCoercions());
}

} // namespace sdb::pg
