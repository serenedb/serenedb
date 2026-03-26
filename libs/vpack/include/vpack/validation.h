#pragma once

#include <memory>
#include <tao/json/from_file.hpp>

#include "vpack/builder.h"
#include "vpack/common.h"
#include "vpack/slice.h"
#include "vpack/validation_types.h"

namespace tao::json {

template<template<typename...> typename Traits>
class basic_schema;  // NOLINT

}  // namespace tao::json
namespace vpack::validation {

[[nodiscard]] bool Validate(
  const tao::json::basic_schema<tao::json::traits>& schema,
  SpecialProperties special, const vpack::Slice doc, const vpack::Options*);

[[nodiscard]] tao::json::value ToValue(
  vpack::Slice doc, SpecialProperties special = SpecialProperties::None,
  const vpack::Options* options = &vpack::Options::gDefaults,
  const vpack::Slice* = nullptr);

}  // namespace vpack::validation
