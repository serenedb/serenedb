////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "validators.h"

#include <vpack/validation.h>
#include <vpack/vpack_helper.h>

#include <array>
#include <string_view>
#include <tao/json/contrib/schema.hpp>
#include <tao/json/jaxn/to_string.hpp>
#include <tao/json/to_string.hpp>

#include "basics/debugging.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"

namespace sdb {

std::string_view ToString(ValidationLevel level) noexcept {
  switch (level) {
    case ValidationLevel::None:
      return StaticStrings::kValidationLevelNone;
    case ValidationLevel::New:
      return StaticStrings::kValidationLevelNew;
    case ValidationLevel::Moderate:
      return StaticStrings::kValidationLevelModerate;
    case ValidationLevel::Strict:
      return StaticStrings::kValidationLevelStrict;
  }
}

ValidatorBase::ValidatorBase(vpack::Slice params) : ValidatorBase() {
  // parse message
  auto msg_slice = params.get(StaticStrings::kValidationParameterMessage);
  if (msg_slice.isString()) {
    this->_message = msg_slice.stringView();
  }

  // parse level
  auto level_slice = params.get(StaticStrings::kValidationParameterLevel);
  if (!level_slice.isNone() && level_slice.isString()) {
    if (level_slice.stringView() == StaticStrings::kValidationLevelNone) {
      this->_level = ValidationLevel::None;
    } else if (level_slice.stringView() == StaticStrings::kValidationLevelNew) {
      this->_level = ValidationLevel::New;
    } else if (level_slice.stringView() ==
               StaticStrings::kValidationLevelModerate) {
      this->_level = ValidationLevel::Moderate;
    } else if (level_slice.stringView() ==
               StaticStrings::kValidationLevelStrict) {
      this->_level = ValidationLevel::Strict;
    } else {
      SDB_THROW(ERROR_VALIDATION_BAD_PARAMETER, "Valid validation levels are: ",
                StaticStrings::kValidationLevelNone, ", ",
                StaticStrings::kValidationLevelNew, ", ",
                StaticStrings::kValidationLevelModerate, ", ",
                StaticStrings::kValidationLevelStrict);
    }
  }
}

bool ValidatorBase::isSame(vpack::Slice validator1, vpack::Slice validator2) {
  if (validator1.isObject() && validator2.isObject()) {
    // type "json" is default if no "type" attribute is specified
    std::string_view type1{"json"};
    std::string_view type2{"json"};

    if (auto s = validator1.get(StaticStrings::kValidationParameterType);
        s.isString()) {
      type1 = s.stringView();
    }
    if (auto s = validator2.get(StaticStrings::kValidationParameterType);
        s.isString()) {
      type2 = s.stringView();
    }

    if (type1 != type2) {
      // different types
      return false;
    }

    // compare "message" and "level"
    std::array<std::string_view, 3> fields = {
      StaticStrings::kValidationParameterMessage,
      StaticStrings::kValidationParameterLevel,
      StaticStrings::kValidationParameterRule};
    for (const auto& f : fields) {
      if (!basics::VPackHelper::equals(validator1.get(f), validator2.get(f))) {
        return false;
      }
    }

    // all attributes equal
    return true;
  }

  if (validator1.isObject() || validator2.isObject()) {
    SDB_ASSERT(validator1.isObject() != validator2.isObject());
    // validator1 is an object, but validator2 isn't (or vice versa),
    // so they must be different
    return false;
  }

  // both validators are non-objects
  SDB_ASSERT(validator1.isNone() || validator1.isNull());
  SDB_ASSERT(validator2.isNone() || validator2.isNull());
  return true;
}

Result ValidatorBase::validate(vpack::Slice new_doc, vpack::Slice old_doc,
                               bool is_insert,
                               const vpack::Options* options) const {
  // This function performs the validation depending on operation (Insert /
  // Update / Replace) and requested validation level (None / Insert / New /
  // Strict / Moderate).

  if (this->_level == ValidationLevel::None) {
    return {};
  }

  if (is_insert) {
    return this->validateOne(new_doc, options);
  }

  /* update replace case */
  if (this->_level == ValidationLevel::New) {
    // Level NEW is for insert only.
    return {};
  }

  if (this->_level == ValidationLevel::Strict) {
    // Changed document must be good!
    return validateOne(new_doc, options);
  }

  SDB_ASSERT(this->_level == ValidationLevel::Moderate);
  // Changed document must be good IIF the unmodified
  // document passed validation.

  auto res_new = this->validateOne(new_doc, options);
  if (res_new.ok()) {
    return {};
  }

  if (this->validateOne(old_doc, options).fail()) {
    return {};
  }

  return res_new;
}

void ValidatorBase::toVPack(vpack::Builder& b) const {
  vpack::ObjectBuilder guard(&b);
  b.add(StaticStrings::kValidationParameterMessage, _message);
  b.add(StaticStrings::kValidationParameterLevel, ToString(this->_level));
  b.add(StaticStrings::kValidationParameterType, this->type());
  this->toVPackDerived(b);
}

ValidatorJsonSchema::ValidatorJsonSchema(vpack::Slice params)
  : ValidatorBase(params) {
  auto rule = params.get(StaticStrings::kValidationParameterRule);
  if (!rule.isObject()) {
    SDB_THROW(
      ERROR_VALIDATION_BAD_PARAMETER,
      "No valid schema in rule attribute given (no object): ", params.toJson());
  }
  auto tao_rule_value = vpack::validation::ToValue(rule);
  try {
    _schema = std::make_shared<tao::json::schema>(tao_rule_value);
    _builder.add(rule);
  } catch (const std::exception& ex) {
    SDB_THROW(ERROR_VALIDATION_BAD_PARAMETER, "invalid object",
              tao::json::to_string(tao_rule_value, 4),
              "exception: ", ex.what());
  }
}

Result ValidatorJsonSchema::validateOne(vpack::Slice slice,
                                        const vpack::Options* options) const {
  auto res = vpack::validation::Validate(*_schema, _special, slice, options);
  if (res) {
    return {};
  }
  return {ERROR_VALIDATION_FAILED, _message};
}
void ValidatorJsonSchema::toVPackDerived(vpack::Builder& b) const {
  SDB_ASSERT(!_builder.slice().isNone());
  b.add(StaticStrings::kValidationParameterRule, _builder.slice());
}

ResultOr<std::shared_ptr<ValidatorJsonSchema>>
ValidatorJsonSchema::buildInstance(vpack::Slice schema) {
  if (schema.isNone() || schema.isNull() || schema.isEmptyObject()) {
    return nullptr;
  }
  if (!schema.isObject()) {
    return std::unexpected<Result>{std::in_place,
                                   ERROR_VALIDATION_BAD_PARAMETER,
                                   "Schema description is not an object."};
  }

  try {
    return std::make_shared<ValidatorJsonSchema>(schema);
  } catch (const std::exception& ex) {
    return std::unexpected<Result>{
      std::in_place, ERROR_VALIDATION_BAD_PARAMETER,
      absl::StrCat("Error when building schema: ", ex.what())};
  }
}

}  // namespace sdb
