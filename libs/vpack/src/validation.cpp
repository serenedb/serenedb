#include "vpack/validation.h"

#include <tao/json/contrib/schema.hpp>

#include "vpack/events_from_slice.h"
#include "vpack/iterator.h"
#include "vpack/validation_types.h"

namespace vpack::validation {
namespace {

[[nodiscard]] tao::json::value SliceObjectToValue(vpack::Slice slice,
                                                  SpecialProperties special,
                                                  const vpack::Options* options,
                                                  const vpack::Slice* base) {
  SDB_ASSERT(slice.isObject());
  tao::json::value rv;
  rv.prepare_object();

  vpack::ObjectIterator it{slice, true};
  while (it.valid()) {
    auto [k, v] = *it;
    rv.try_emplace(k.copyString(), ToValue(v, special, options, &slice));
    it.next();
  }

  return rv;
}

[[nodiscard]] tao::json::value SliceArrayToValue(vpack::Slice slice,
                                                 SpecialProperties special,
                                                 const vpack::Options* options,
                                                 const vpack::Slice* base) {
  SDB_ASSERT(slice.isArray());
  vpack::ArrayIterator it(slice);
  tao::json::value rv;
  auto a = rv.prepare_array();
  a.resize(it.size());

  while (it.valid()) {
    auto v = ToValue(it.value(), special, options, &slice);
    rv.push_back(std::move(v));
    it.next();
  }

  return rv;
}

template<template<typename...> typename Traits>
[[nodiscard]] bool ValidateImpl(
  const tao::json::basic_schema<Traits>& schema, SpecialProperties special,
  vpack::Slice v, const vpack::Options* options = &vpack::Options::gDefaults) {
  const auto c = schema.consumer();
  tao::json::events::from_value<Traits>(*c, v, options, nullptr, special);
  return c->finalize();
}

// use basics strings
namespace strings {

constexpr std::string_view kKey = "_key";
constexpr std::string_view kRev = "_rev";
constexpr std::string_view kId = "_id";
constexpr std::string_view kFrom = "_from";
constexpr std::string_view kTo = "_to";

}  // namespace strings
}  // namespace

SpecialProperties StrToSpecial(std::string_view str) {
  if (str == strings::kKey) {
    return SpecialProperties::Key;
  } else if (str == strings::kId) {
    return SpecialProperties::Id;
  } else if (str == strings::kRev) {
    return SpecialProperties::Rev;
  } else if (str == strings::kFrom) {
    return SpecialProperties::From;
  } else if (str == strings::kTo) {
    return SpecialProperties::To;
  } else {
    return SpecialProperties::None;
  }
}

bool SkipSpecial(std::string_view str, SpecialProperties special) {
  const auto len = str.size();
  if (len < 3 || 5 < len) {
    return false;
  }

  auto search = StrToSpecial(str);
  if (search == SpecialProperties::None) {
    return false;
  }

  if (std::to_underlying(special) & std::to_underlying(search)) {
    return false;
  }

  return true;
}

bool Validate(const tao::json::schema& schema, SpecialProperties special,
              const vpack::Slice doc, const vpack::Options* options) {
  return ValidateImpl(schema, special, doc, options);
}

tao::json::value ToValue(vpack::Slice slice, SpecialProperties special,
                         const vpack::Options* options,
                         const vpack::Slice* base) {
  tao::json::value rv;
  switch (slice.type()) {
    case vpack::ValueType::Null:
      rv.set_null();
      return rv;
    case vpack::ValueType::Bool:
      rv.set_boolean(slice.isTrue());
      return rv;
    case vpack::ValueType::Double:
      rv.set_double(slice.getDoubleUnchecked());
      return rv;
    case vpack::ValueType::SmallInt:
    case vpack::ValueType::Int:
      rv.set_signed(slice.getIntUnchecked());
      return rv;
    case vpack::ValueType::UInt:
      rv.set_unsigned(slice.getUIntUnchecked());
      return rv;
    case vpack::ValueType::String:
      rv.set_string(slice.copyString());
      return rv;
    case vpack::ValueType::Array:
      return SliceArrayToValue(slice, special, options, base);
    case vpack::ValueType::Object:
      return SliceObjectToValue(slice, special, options, base);
    default:
      return rv;
  }
}

}  // namespace vpack::validation
