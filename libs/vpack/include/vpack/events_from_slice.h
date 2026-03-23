// Copyright (c) 2016-2020 Daniel Frey and Jan Christoph Uhde
// Please see LICENSE for license or visit https://github.com/taocpp/json/

#pragma once

#include <stdexcept>
#include <tao/json/internal/format.hpp>

#include "vpack/common.h"
#include "vpack/iterator.h"
#include "vpack/options.h"
#include "vpack/slice.h"
#include "vpack/validation_types.h"

namespace tao::json::events {

// Events producer to generate events from a vpack::Slice Value.

template<auto Recurse, template<typename...> typename Traits, typename Consumer>
void from_value(Consumer& consumer, vpack::Slice slice,
                const vpack::Options* options, const vpack::Slice* base,
                vpack::validation::SpecialProperties special) {
  switch (slice.type()) {
    case vpack::ValueType::Null:
      consumer.null();
      return;

    case vpack::ValueType::Bool:
      consumer.boolean(slice.isTrue());
      return;

    case vpack::ValueType::Int:
    case vpack::ValueType::SmallInt:
      consumer.number(slice.getIntUnchecked());
      return;

    case vpack::ValueType::UInt:
      consumer.number(slice.getUIntUnchecked());
      return;

    case vpack::ValueType::Double:
      consumer.number(slice.getDoubleUnchecked());
      return;

    case vpack::ValueType::String:
      consumer.string(slice.stringViewUnchecked());
      return;

    case vpack::ValueType::Array: {
      vpack::ArrayIterator it{slice};
      consumer.begin_array(it.size());
      for (const auto& element : it) {
        Recurse(consumer, element, options, &slice, special);
        consumer.element();
      }
      consumer.end_array(it.size());
      return;
    }

    case vpack::ValueType::Object: {
      // TODO(mbkkt) sequential iterator?
      vpack::ObjectIterator it{slice};
      consumer.begin_object(it.size());
      for (const auto& element : it) {
        const auto key = element.key.stringViewUnchecked();
        if (SkipSpecial(key, special)) {
          continue;
        }
        consumer.key(key);
        Recurse(consumer, element.value(), options, &slice, special);
        consumer.member();
      }
      consumer.end_object(it.size());
      return;
    }

    case vpack::ValueType::None:
      throw std::runtime_error(std::string{"unsupported type: "} +
                               std::string{slice.typeName()});
  }
  throw std::logic_error(internal::format("invalid value '",
                                          static_cast<uint8_t>(slice.type()),
                                          "' for vpack::ValueType"));
}

template<template<typename...> typename Traits, typename Consumer>
void from_value(Consumer& consumer, vpack::Slice slice,
                const vpack::Options* options, const vpack::Slice* base,
                vpack::validation::SpecialProperties special) {
  using FromValue =
    void (*)(Consumer&, vpack::Slice, const vpack::Options*,
             const vpack::Slice*, vpack::validation::SpecialProperties);
  from_value<static_cast<FromValue>(&from_value<Traits, Consumer>), Traits,
             Consumer>(consumer, slice, options, base, special);
}

}  // namespace tao::json::events
