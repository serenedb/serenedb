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

#include <cstdint>

#include "vpack/common.h"
#include "vpack/options.h"

namespace vpack {

class Slice;

class Validator {
  // This class can validate a binary VPack value.

 public:
  explicit Validator(const Options* options = &Options::gDefaults);
  ~Validator() = default;

  // validates a VPack Slice value starting at ptr, with length bytes
  // length throws if the data is invalid
  bool validate(const char* ptr, size_t length, bool is_sub_part = false) {
    return validate(reinterpret_cast<const uint8_t*>(ptr), length, is_sub_part);
  }

  // validates a VPack Slice value starting at ptr, with length bytes
  // length throws if the data is invalid
  bool validate(const uint8_t* ptr, size_t length, bool is_sub_part = false);

 private:
  void validatePart(const uint8_t* ptr, size_t length, bool is_sub_part);

  void validateArray(const uint8_t* ptr, size_t length);
  void validateCompactArray(const uint8_t* ptr, size_t length);
  void validateUnindexedArray(const uint8_t* ptr, size_t length);
  void validateIndexedArray(const uint8_t* ptr, size_t length);
  void validateObject(const uint8_t* ptr, size_t length);
  void validateCompactObject(const uint8_t* ptr, size_t length);
  void validateIndexedObject(const uint8_t* ptr, size_t length);
  void validateBufferLength(size_t expected, size_t actual, bool is_sub_part);
  void validateSliceLength(const uint8_t* ptr, size_t length, bool is_sub_part);
  ValueLength readByteSize(const uint8_t*& ptr, const uint8_t* end);

 public:
  const Options* options;

 private:
  uint32_t _nesting;
};

}  // namespace vpack
