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

#include "pg/functions.h"

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <velox/expression/DecodedArgs.h>
#include <velox/expression/FunctionMetadata.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/vector/ComplexVector.h>

#include "pg/functions/array_extra.h"
#include "pg/functions/datetime_extra.h"
#include "pg/functions/extract.h"
#include "pg/functions/generate.h"
#include "pg/functions/inout.h"
#include "pg/functions/json.h"
#include "pg/functions/math_extra.h"
#include "pg/functions/regexp.h"
#include "pg/functions/size.h"
#include "pg/functions/string_extra.h"
#include "pg/functions/stub.h"
#include "pg/functions/system.h"
#include "pg/functions/util.h"
#include "pg/sdb_functions/vector.h"
#include "search/functions.hpp"

namespace sdb::pg::functions {

void RegisterFunctions(const std::string& prefix) {
  RegisterInOutFunctions(prefix);
  RegisterJsonFunctions(prefix);
  RegisterSizeFunctions(prefix);
  RegisterRegexpFunctions(prefix);
  RegisterStringExtraFunctions(prefix);
  RegisterMathExtraFunctions(prefix);
  RegisterDatetimeExtraFunctions(prefix);
  RegisterArrayExtraFunctions(prefix);
  RegisterExtractFunctions(prefix);
  RegisterStubFunctions(prefix);
  RegisterSystemFunctions(prefix);
  RegisterGenerateFunctions(prefix);
  RegisterUtilFunctions(prefix);
}

void RegisterSdbFunctions(const std::string& prefix) {
  RegisterVectorFunctions(prefix);
  // search functions have sdb_ prefix
  search::functions::RegisterSearchFunctions();
}

}  // namespace sdb::pg::functions
