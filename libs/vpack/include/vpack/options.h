////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include "basics/string_utils.h"

namespace vpack {

class Slice;

struct Options : sdb::basics::string_utils::EscapeJsonOptions {
  // Behavior to be applied when dumping VPack values that cannot be
  // expressed in JSON without data loss
  enum UnsupportedTypeBehavior {
    // convert any non-JSON-representable value to null
    kNullifyUnsupportedType,
    // emit a JSON string "(non-representable type ...)"
    kConvertUnsupportedType,
    // throw an exception for any non-JSON-representable value
    kFailOnUnsupportedType
  };

  // Behavior to be applied when building VPack Array/Object values
  // with a Builder
  enum PaddingBehavior {
    // use padding - fill unused head bytes with zero-bytes (ASCII NUL) in
    // order to avoid a later memmove
    kUsePadding,
    // don't pad and do not fill any gaps with zero-bytes (ASCII NUL).
    // instead, memmove data down so there is no gap between the head bytes
    // and the payload
    kNoPadding,
    // pad in cases the Builder considers it useful, and don't pad in other
    // cases when the Builder doesn't consider it useful
    kFlexible
  };

  // Dumper behavior when a VPack value is serialized to JSON that
  // has no JSON equivalent
  UnsupportedTypeBehavior unsupported_type_behavior = kFailOnUnsupportedType;

  // Builder behavior w.r.t. padding or memmoving data
  PaddingBehavior padding_behavior = PaddingBehavior::kFlexible;

  // allow building Arrays without index table?
  bool build_unindexed_arrays = false;

  // allow building Objects without index table?
  bool build_unindexed_objects = false;

  // pretty-print JSON output when dumping with Dumper
  bool pretty_print = false;

  // pretty-print JSON output when dumping with Dumper, but don't add any
  // newlines
  bool single_line_pretty_print = false;

  // keep top-level object/array open when building objects with the Parser
  bool keep_top_level_open = false;

  // clear builder before starting to parse in Parser
  bool clear_builder_before_parse = true;

  // validate UTF-8 strings when JSON-parsing with Parser or validating with
  // Validator
  bool validate_utf8_strings = false;

  // validate that attribute names in Object values are actually
  // unique when creating objects via Builder. This also includes
  // creation of Object values via a Parser
  bool check_attribute_uniqueness = false;

  // dump Object attributes in index order (true) or in "undefined"
  // order (false). undefined order may be faster but not deterministic
  bool dump_attributes_in_index_order = true;

  // dump NaN as "NaN", Infinity as "Infinity"
  bool unsupported_doubles_as_string = false;

  // max recursion level for object/array nesting. checked by Parser and
  // Validator.
  uint32_t nesting_limit = std::numeric_limits<uint32_t>::max();

  // default options with the above settings
  static Options gDefaults;
};

}  // namespace vpack
