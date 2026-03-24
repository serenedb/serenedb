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

#include "key_generator.h"

#include <vpack/slice.h>
#include <vpack/vpack_helper.h>

#include <array>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <limits>
#include <string>
#include <string_view>

#include "app/app_server.h"
#include "app/name_validator.h"
#include "basics/containers/trivial_map.h"
#include "basics/debugging.h"
#include "basics/endian.h"
#include "basics/exceptions.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "catalog/table.h"
#include "database/ticks.h"
#include "general_server/state.h"

namespace sdb {
namespace {

// lookup table for key checks
// in case this table is changed, the regex in file
// js/common/modules/@serenedb/common.js for function isValidDocumentKey
// should be adjusted too
constexpr std::array<bool, 256> kKeyCharLookupTable = {
  {/* 0x00 . */ false, /* 0x01 . */ false,
   /* 0x02 . */ false, /* 0x03 . */ false,
   /* 0x04 . */ false, /* 0x05 . */ false,
   /* 0x06 . */ false, /* 0x07 . */ false,
   /* 0x08 . */ false, /* 0x09 . */ false,
   /* 0x0a . */ false, /* 0x0b . */ false,
   /* 0x0c . */ false, /* 0x0d . */ false,
   /* 0x0e . */ false, /* 0x0f . */ false,
   /* 0x10 . */ false, /* 0x11 . */ false,
   /* 0x12 . */ false, /* 0x13 . */ false,
   /* 0x14 . */ false, /* 0x15 . */ false,
   /* 0x16 . */ false, /* 0x17 . */ false,
   /* 0x18 . */ false, /* 0x19 . */ false,
   /* 0x1a . */ false, /* 0x1b . */ false,
   /* 0x1c . */ false, /* 0x1d . */ false,
   /* 0x1e . */ false, /* 0x1f . */ false,
   /* 0x20   */ false, /* 0x21 ! */ true,
   /* 0x22 " */ false, /* 0x23 # */ false,
   /* 0x24 $ */ true,  /* 0x25 % */ true,
   /* 0x26 & */ false, /* 0x27 ' */ true,
   /* 0x28 ( */ true,  /* 0x29 ) */ true,
   /* 0x2a * */ true,  /* 0x2b + */ true,
   /* 0x2c , */ true,  /* 0x2d - */ true,
   /* 0x2e . */ true,  /* 0x2f / */ false,
   /* 0x30 0 */ true,  /* 0x31 1 */ true,
   /* 0x32 2 */ true,  /* 0x33 3 */ true,
   /* 0x34 4 */ true,  /* 0x35 5 */ true,
   /* 0x36 6 */ true,  /* 0x37 7 */ true,
   /* 0x38 8 */ true,  /* 0x39 9 */ true,
   /* 0x3a : */ true,  /* 0x3b ; */ true,
   /* 0x3c < */ false, /* 0x3d = */ true,
   /* 0x3e > */ false, /* 0x3f ? */ false,
   /* 0x40 @ */ true,  /* 0x41 A */ true,
   /* 0x42 B */ true,  /* 0x43 C */ true,
   /* 0x44 D */ true,  /* 0x45 E */ true,
   /* 0x46 F */ true,  /* 0x47 G */ true,
   /* 0x48 H */ true,  /* 0x49 I */ true,
   /* 0x4a J */ true,  /* 0x4b K */ true,
   /* 0x4c L */ true,  /* 0x4d M */ true,
   /* 0x4e N */ true,  /* 0x4f O */ true,
   /* 0x50 P */ true,  /* 0x51 Q */ true,
   /* 0x52 R */ true,  /* 0x53 S */ true,
   /* 0x54 T */ true,  /* 0x55 U */ true,
   /* 0x56 V */ true,  /* 0x57 W */ true,
   /* 0x58 X */ true,  /* 0x59 Y */ true,
   /* 0x5a Z */ true,  /* 0x5b [ */ false,
   /* 0x5c \ */ false, /* 0x5d ] */ false,
   /* 0x5e ^ */ false, /* 0x5f _ */ true,
   /* 0x60 ` */ false, /* 0x61 a */ true,
   /* 0x62 b */ true,  /* 0x63 c */ true,
   /* 0x64 d */ true,  /* 0x65 e */ true,
   /* 0x66 f */ true,  /* 0x67 g */ true,
   /* 0x68 h */ true,  /* 0x69 i */ true,
   /* 0x6a j */ true,  /* 0x6b k */ true,
   /* 0x6c l */ true,  /* 0x6d m */ true,
   /* 0x6e n */ true,  /* 0x6f o */ true,
   /* 0x70 p */ true,  /* 0x71 q */ true,
   /* 0x72 r */ true,  /* 0x73 s */ true,
   /* 0x74 t */ true,  /* 0x75 u */ true,
   /* 0x76 v */ true,  /* 0x77 w */ true,
   /* 0x78 x */ true,  /* 0x79 y */ true,
   /* 0x7a z */ true,  /* 0x7b { */ false,
   /* 0x7c | */ false, /* 0x7d } */ false,
   /* 0x7e ~ */ false, /* 0x7f   */ false,
   /* 0x80 . */ false, /* 0x81 . */ false,
   /* 0x82 . */ false, /* 0x83 . */ false,
   /* 0x84 . */ false, /* 0x85 . */ false,
   /* 0x86 . */ false, /* 0x87 . */ false,
   /* 0x88 . */ false, /* 0x89 . */ false,
   /* 0x8a . */ false, /* 0x8b . */ false,
   /* 0x8c . */ false, /* 0x8d . */ false,
   /* 0x8e . */ false, /* 0x8f . */ false,
   /* 0x90 . */ false, /* 0x91 . */ false,
   /* 0x92 . */ false, /* 0x93 . */ false,
   /* 0x94 . */ false, /* 0x95 . */ false,
   /* 0x96 . */ false, /* 0x97 . */ false,
   /* 0x98 . */ false, /* 0x99 . */ false,
   /* 0x9a . */ false, /* 0x9b . */ false,
   /* 0x9c . */ false, /* 0x9d . */ false,
   /* 0x9e . */ false, /* 0x9f . */ false,
   /* 0xa0 . */ false, /* 0xa1 . */ false,
   /* 0xa2 . */ false, /* 0xa3 . */ false,
   /* 0xa4 . */ false, /* 0xa5 . */ false,
   /* 0xa6 . */ false, /* 0xa7 . */ false,
   /* 0xa8 . */ false, /* 0xa9 . */ false,
   /* 0xaa . */ false, /* 0xab . */ false,
   /* 0xac . */ false, /* 0xad . */ false,
   /* 0xae . */ false, /* 0xaf . */ false,
   /* 0xb0 . */ false, /* 0xb1 . */ false,
   /* 0xb2 . */ false, /* 0xb3 . */ false,
   /* 0xb4 . */ false, /* 0xb5 . */ false,
   /* 0xb6 . */ false, /* 0xb7 . */ false,
   /* 0xb8 . */ false, /* 0xb9 . */ false,
   /* 0xba . */ false, /* 0xbb . */ false,
   /* 0xbc . */ false, /* 0xbd . */ false,
   /* 0xbe . */ false, /* 0xbf . */ false,
   /* 0xc0 . */ false, /* 0xc1 . */ false,
   /* 0xc2 . */ false, /* 0xc3 . */ false,
   /* 0xc4 . */ false, /* 0xc5 . */ false,
   /* 0xc6 . */ false, /* 0xc7 . */ false,
   /* 0xc8 . */ false, /* 0xc9 . */ false,
   /* 0xca . */ false, /* 0xcb . */ false,
   /* 0xcc . */ false, /* 0xcd . */ false,
   /* 0xce . */ false, /* 0xcf . */ false,
   /* 0xd0 . */ false, /* 0xd1 . */ false,
   /* 0xd2 . */ false, /* 0xd3 . */ false,
   /* 0xd4 . */ false, /* 0xd5 . */ false,
   /* 0xd6 . */ false, /* 0xd7 . */ false,
   /* 0xd8 . */ false, /* 0xd9 . */ false,
   /* 0xda . */ false, /* 0xdb . */ false,
   /* 0xdc . */ false, /* 0xdd . */ false,
   /* 0xde . */ false, /* 0xdf . */ false,
   /* 0xe0 . */ false, /* 0xe1 . */ false,
   /* 0xe2 . */ false, /* 0xe3 . */ false,
   /* 0xe4 . */ false, /* 0xe5 . */ false,
   /* 0xe6 . */ false, /* 0xe7 . */ false,
   /* 0xe8 . */ false, /* 0xe9 . */ false,
   /* 0xea . */ false, /* 0xeb . */ false,
   /* 0xec . */ false, /* 0xed . */ false,
   /* 0xee . */ false, /* 0xef . */ false,
   /* 0xf0 . */ false, /* 0xf1 . */ false,
   /* 0xf2 . */ false, /* 0xf3 . */ false,
   /* 0xf4 . */ false, /* 0xf5 . */ false,
   /* 0xf6 . */ false, /* 0xf7 . */ false,
   /* 0xf8 . */ false, /* 0xf9 . */ false,
   /* 0xfa . */ false, /* 0xfb . */ false,
   /* 0xfc . */ false, /* 0xfd . */ false,
   /* 0xfe . */ false, /* 0xff . */ false}};

uint64_t ReadLastValue(vpack::Slice options) {
  uint64_t last_value = 0;

  if (auto last_value_slice = options.get(StaticStrings::kLastValue);
      last_value_slice.isNumber()) {
    double v = last_value_slice.getNumber<double>();
    if (v < 0.0) {
      // negative lastValue is not allowed
      SDB_THROW(ERROR_SERVER_INVALID_KEY_GENERATOR,
                "'lastValue' value must be greater than zero");
    }

    last_value = last_value_slice.getNumber<uint64_t>();
  }
  return last_value;
}

class TraditionalKeyGenerator : public KeyGenerator {
 public:
  explicit TraditionalKeyGenerator(bool allow_user_keys)
    : KeyGenerator{allow_user_keys} {}

  bool hasDynamicState() const noexcept final { return true; }

  std::string generate(vpack::Slice /*input*/) final {
    uint64_t tick = GenerateValue();

    if (tick == 0) [[unlikely]] {
      // returning an empty string will trigger an error on the call site
      return {};
    }

    return basics::string_utils::Itoa(tick);
  }

  ErrorCode validate(std::string_view key, vpack::Slice input,
                     bool is_restore) noexcept override {
    auto res = KeyGenerator::validate(key, input, is_restore);

    if (res == ERROR_OK) {
      track(key);
    }

    return res;
  }

  void track(std::string_view key) noexcept override {
    const char* p = key.data();
    size_t length = key.size();
    // check the numeric key part
    if (length > 0 && p[0] >= '0' && p[0] <= '9') {
      // potentially numeric key
      uint64_t value = number_utils::AtoiZero<uint64_t>(p, p + length);

      if (value > 0) {
        TrackValue(value);
      }
    }
  }

  void toVPack(vpack::Builder& builder) const override {
    KeyGenerator::toVPack(builder);
    builder.add("type", "traditional");
  }

 protected:
  virtual uint64_t GenerateValue() = 0;

  virtual void TrackValue(uint64_t value) noexcept = 0;
};

class TraditionalKeyGeneratorSingle final : public TraditionalKeyGenerator {
 public:
  explicit TraditionalKeyGeneratorSingle(bool allow_user_keys,
                                         uint64_t last_value)
    : TraditionalKeyGenerator{allow_user_keys}, _last_value{last_value} {
    SDB_ASSERT(!ServerState::instance()->IsCoordinator());
  }

  void toVPack(vpack::Builder& builder) const override {
    TraditionalKeyGenerator::toVPack(builder);
    builder.add(StaticStrings::kLastValue,
                _last_value.load(std::memory_order_relaxed));
  }

 private:
  uint64_t GenerateValue() override {
    SDB_IF_FAILURE("KeyGenerator::generateOnSingleServer") {
      // for testing purposes only
      SDB_THROW(ERROR_DEBUG);
    }

    uint64_t tick = NewTickServer();

    if (tick == UINT64_MAX) [[unlikely]] {
      // out of keys
      return 0;
    }

    // keep track of last assigned value, and make sure the value
    // we hand out is always higher than it
    auto last_value = _last_value.load(std::memory_order_relaxed);
    if (last_value >= UINT64_MAX - 1ULL) [[unlikely]] {
      // oops, out of keys!
      return 0;
    }

    do {
      if (tick <= last_value) {
        tick = _last_value.fetch_add(1, std::memory_order_relaxed) + 1;
        break;
      }
    } while (!_last_value.compare_exchange_weak(last_value, tick,
                                                std::memory_order_relaxed));

    return tick;
  }

  /// track a key value (internal)
  void TrackValue(uint64_t value) noexcept override {
    auto last_value = _last_value.load(std::memory_order_relaxed);
    while (value > last_value) {
      // and update our last value
      if (_last_value.compare_exchange_weak(last_value, value,
                                            std::memory_order_relaxed)) {
        break;
      }
    }
  }

 private:
  std::atomic<uint64_t> _last_value;
};

class PaddedKeyGenerator : public KeyGenerator {
 public:
  explicit PaddedKeyGenerator(bool allow_user_keys, uint64_t last_value)
    : KeyGenerator{allow_user_keys}, _last_value(last_value) {}

  bool hasDynamicState() const noexcept final { return true; }

  std::string generate(vpack::Slice /*input*/) final {
    uint64_t tick = GenerateValue();

    if (tick == 0 || tick == UINT64_MAX) [[unlikely]] {
      // unlikely case we have run out of keys
      // returning an empty string will trigger an error on the call site
      return {};
    }

    auto last_value = _last_value.load(std::memory_order_relaxed);
    if (last_value >= UINT64_MAX - 1ULL) [[unlikely]] {
      // oops, out of keys!
      return {};
    }

    do {
      if (tick <= last_value) {
        tick = _last_value.fetch_add(1, std::memory_order_relaxed) + 1;
        break;
      }
    } while (!_last_value.compare_exchange_weak(last_value, tick,
                                                std::memory_order_relaxed));

    return KeyGeneratorHelper::encodePadded(tick);
  }

  ErrorCode validate(std::string_view key, vpack::Slice input,
                     bool is_restore) noexcept override {
    auto res = KeyGenerator::validate(key, input, is_restore);

    if (res == ERROR_OK) {
      track(key);
    }

    return res;
  }

  void track(std::string_view key) noexcept final {
    // check the numeric key part
    uint64_t value = KeyGeneratorHelper::decodePadded(key.data(), key.size());
    if (value > 0) {
      auto last_value = _last_value.load(std::memory_order_relaxed);
      while (value > last_value) {
        // and update our last value
        if (_last_value.compare_exchange_weak(last_value, value,
                                              std::memory_order_relaxed)) {
          break;
        }
      }
    }
  }

  void toVPack(vpack::Builder& builder) const final {
    KeyGenerator::toVPack(builder);
    builder.add("type", "padded");
    builder.add(StaticStrings::kLastValue,
                _last_value.load(std::memory_order_relaxed));
  }

  void initState(vpack::Slice state) override {
    SDB_ASSERT(state.isObject());
    // special case here: we read a numeric, UNENCODED lastValue attribute from
    // the state object, but we need to pass an ENCODED value to the track()
    // method.
    track(KeyGeneratorHelper::encodePadded(ReadLastValue(state)));
  }

 protected:
  virtual uint64_t GenerateValue() = 0;

 private:
  std::atomic<uint64_t> _last_value;
};

class PaddedKeyGeneratorSingle final : public PaddedKeyGenerator {
 public:
  explicit PaddedKeyGeneratorSingle(bool allow_user_keys, uint64_t last_value)
    : PaddedKeyGenerator{allow_user_keys, last_value} {
    SDB_ASSERT(!ServerState::instance()->IsCoordinator());
  }

 private:
  uint64_t GenerateValue() override {
    SDB_IF_FAILURE("KeyGenerator::generateOnSingleServer") {
      // for testing purposes only
      SDB_THROW(ERROR_DEBUG);
    }
    return NewTickServer();
  }
};

class AutoIncrementKeyGenerator final : public KeyGenerator {
 public:
  AutoIncrementKeyGenerator(bool allow_user_keys, uint64_t last_value,
                            uint64_t offset, uint64_t increment)
    : KeyGenerator{allow_user_keys},
      _last_value{last_value},
      _offset{offset},
      _increment{increment} {}

  bool hasDynamicState() const noexcept override { return true; }

  std::string generate(vpack::Slice /*input*/) override {
    uint64_t key_value;

    auto last_value = _last_value.load(std::memory_order_relaxed);
    do {
      // user has not specified a key, generate one based on algorithm
      if (last_value < _offset) {
        key_value = _offset;
      } else {
        key_value =
          last_value + _increment - ((last_value - _offset) % _increment);
      }

      // bounds and validity checks
      if (key_value == UINT64_MAX || key_value < last_value) {
        return "";
      }

      SDB_ASSERT(key_value > last_value);
      // update our last value
    } while (!_last_value.compare_exchange_weak(last_value, key_value,
                                                std::memory_order_relaxed));

    return basics::string_utils::Itoa(key_value);
  }

  ErrorCode validate(std::string_view key, vpack::Slice input,
                     bool is_restore) noexcept override {
    auto res = KeyGenerator::validate(key, input, is_restore);

    if (res == ERROR_OK) {
      const char* s = key.data();
      const char* e = s + key.size();
      SDB_ASSERT(s != e);
      do {
        if (*s < '0' || *s > '9') {
          return ERROR_SERVER_DOCUMENT_KEY_BAD;
        }
      } while (++s < e);

      track(key);
    }

    return res;
  }

  void track(std::string_view key) noexcept override {
    const char* p = key.data();
    size_t length = key.size();
    // check the numeric key part
    if (length > 0 && p[0] >= '0' && p[0] <= '9') {
      uint64_t value = number_utils::AtoiZero<uint64_t>(p, p + length);

      if (value > 0) {
        auto last_value = _last_value.load(std::memory_order_relaxed);
        while (value > last_value) {
          // and update our last value
          if (_last_value.compare_exchange_weak(last_value, value,
                                                std::memory_order_relaxed)) {
            break;
          }
        }
      }
    }
  }

  void toVPack(vpack::Builder& builder) const override {
    KeyGenerator::toVPack(builder);
    builder.add("type", "autoincrement");
    builder.add("offset", _offset);
    builder.add("increment", _increment);
    builder.add(StaticStrings::kLastValue,
                _last_value.load(std::memory_order_relaxed));
  }

 private:
  std::atomic<uint64_t> _last_value;  // last value assigned
  const uint64_t _offset;             // start value
  const uint64_t _increment;          // increment value
};

class UuidKeyGenerator final : public KeyGenerator {
 public:
  explicit UuidKeyGenerator(bool allow_user_keys)
    : KeyGenerator{allow_user_keys} {}

  bool hasDynamicState() const noexcept override { return false; }

  std::string generate(vpack::Slice /*input*/) override {
    std::lock_guard locker{_lock};
    return boost::uuids::to_string(_uuid());
  }

  void track(std::string_view /*key*/) noexcept override {}

  void toVPack(vpack::Builder& builder) const override {
    KeyGenerator::toVPack(builder);
    builder.add("type", "uuid");
  }

 private:
  absl::Mutex _lock;

  boost::uuids::random_generator _uuid;
};

using FactoryFn = std::shared_ptr<KeyGenerator> (*)(uint32_t, vpack::Slice);

constexpr containers::TrivialBiMap kFactories = [](auto selector) {
  return selector()
    .Case("traditional",
          FactoryFn{[](uint32_t,
                       vpack::Slice options) -> std::shared_ptr<KeyGenerator> {
            bool allow_user_keys = basics::VPackHelper::getBool(
              options, StaticStrings::kAllowUserKeys, true);

            return std::make_shared<TraditionalKeyGeneratorSingle>(
              allow_user_keys, ReadLastValue(options));
          }})
    .Case("autoincrement",
          FactoryFn{[](uint32_t number_of_shards,
                       vpack::Slice options) -> std::shared_ptr<KeyGenerator> {
            if (!ServerState::instance()->IsSingle() && number_of_shards > 1) {
              SDB_THROW(ERROR_CLUSTER_UNSUPPORTED,
                        "the specified key generator is not "
                        "supported for collections with more than one shard");
            }

            uint64_t offset = 0;
            uint64_t increment = 1;

            if (auto increment_slice = options.get("increment");
                increment_slice.isNumber()) {
              double v = increment_slice.getNumber<double>();
              if (v <= 0.0) {
                // negative or 0 increment is not allowed
                SDB_THROW(ERROR_SERVER_INVALID_KEY_GENERATOR,
                          "increment value must be greater than zero");
              }

              increment = increment_slice.getNumber<uint64_t>();

              if (increment == 0 || increment >= (1ULL << 16)) {
                SDB_THROW(
                  ERROR_SERVER_INVALID_KEY_GENERATOR,
                  "increment value must be greater than zero and smaller than "
                  "65536");
              }
            }

            if (auto offset_slice = options.get("offset");
                offset_slice.isNumber()) {
              double v = offset_slice.getNumber<double>();
              if (v < 0.0) {
                // negative or 0 offset is not allowed
                SDB_THROW(ERROR_SERVER_INVALID_KEY_GENERATOR,
                          "offset value must be zero or greater");
              }

              offset = offset_slice.getNumber<uint64_t>();

              if (offset >= UINT64_MAX) {
                SDB_THROW(ERROR_SERVER_INVALID_KEY_GENERATOR,
                          "offset value is too high");
              }
            }

            bool allow_user_keys = basics::VPackHelper::getBool(
              options, StaticStrings::kAllowUserKeys, true);

            return std::make_shared<AutoIncrementKeyGenerator>(
              allow_user_keys, ReadLastValue(options), offset, increment);
          }})
    .Case("uuid",
          FactoryFn{[](uint32_t,
                       vpack::Slice options) -> std::shared_ptr<KeyGenerator> {
            bool allow_user_keys = basics::VPackHelper::getBool(
              options, StaticStrings::kAllowUserKeys, true);

            return std::make_shared<UuidKeyGenerator>(allow_user_keys);
          }})
    .Case("padded",
          FactoryFn{[](uint32_t,
                       vpack::Slice options) -> std::shared_ptr<KeyGenerator> {
            bool allow_user_keys = basics::VPackHelper::getBool(
              options, StaticStrings::kAllowUserKeys, true);

            return std::make_shared<PaddedKeyGeneratorSingle>(
              allow_user_keys, ReadLastValue(options));
          }});
};

}  // namespace

const std::string KeyGeneratorHelper::kLowestKey;
const std::string KeyGeneratorHelper::kHighestKey(
  KeyGeneratorHelper::kMaxKeyLength,
  std::numeric_limits<std::string::value_type>::max());

std::vector<std::string_view> KeyGeneratorHelper::generatorNames() {
  std::vector<std::string_view> names;
  names.reserve(kFactories.size());
  for (const auto& [name, _] : kFactories) {
    names.emplace_back(name);
  }
  return names;
}

uint64_t KeyGeneratorHelper::decodePadded(const char* data,
                                          size_t length) noexcept {
  uint64_t result = 0;

  if (length != sizeof(uint64_t) * 2) {
    return result;
  }

  const char* p = data;
  const char* e = p + length;
  while (p < e) {
    uint64_t high, low;
    uint8_t c = (uint8_t)(*p++);
    if (c >= 'a' && c <= 'f') {
      high = (c - 'a') + 10;
    } else if (c >= '0' && c <= '9') {
      high = (c - '0');
    } else {
      return 0;
    }
    c = (uint8_t)(*p++);
    if (c >= 'a' && c <= 'f') {
      low = (c - 'a') + 10;
    } else if (c >= '0' && c <= '9') {
      low = (c - '0');
    } else {
      return 0;
    }
    result += ((high << 4) | low) << (uint64_t(8) * ((e - p) / 2));
  }

  return result;
}

std::string KeyGeneratorHelper::encodePadded(uint64_t value) {
  uint64_t big = basics::HostToBig(value);

  const uint8_t* p = reinterpret_cast<const uint8_t*>(&big);
  const uint8_t* e = p + sizeof(value);

  char buffer[16];
  uint8_t* out = reinterpret_cast<uint8_t*>(&buffer[0]);
  while (p < e) {
    uint8_t c = (uint8_t)(*p++);
    uint8_t n1 = c >> 4;
    uint8_t n2 = c & 0x0F;
    *out++ = ((n1 < 10) ? ('0' + n1) : ('a' + n1 - 10));
    *out++ = ((n2 < 10) ? ('0' + n2) : ('a' + n2 - 10));
  }

  return std::string(&buffer[0], sizeof(uint64_t) * 2);
}

bool KeyGeneratorHelper::validateKey(const char* key, size_t len) noexcept {
  if (len == 0 || len > kMaxKeyLength) {
    return false;
  }

  const unsigned char* p = reinterpret_cast<const unsigned char*>(key);
  const unsigned char* e = p + len;
  SDB_ASSERT(p != e);
  do {
    if (!kKeyCharLookupTable[*p]) {
      return false;
    }
  } while (++p < e);

  SDB_ASSERT(std::string_view(key, len) > kLowestKey);
  SDB_ASSERT(std::string_view(key, len) <= kHighestKey);
  return true;
}

/// validate a document id (collection name + / + document key)
bool KeyGeneratorHelper::validateId(const char* key, size_t len,
                                    size_t& split) noexcept {
  // look for split character
  const char* found = static_cast<const char*>(memchr(key, '/', len));
  if (found == nullptr) {
    split = 0;
    return false;
  }

  split = found - key;
  if (split == 0 || split + 1 == len) {
    return false;
  }

  SDB_ASSERT(found != nullptr && split > 0);
  // check for numeric collection id
  const char* p = key;
  const char* e = key + split;
  SDB_ASSERT(p != e);
  while (p < e && *p >= '0' && *p <= '9') {
    ++p;
  }
  if (p == key) {
    // non-numeric id
    try {
      if (TableNameValidator::validateName(std::string_view(key, split))
            .fail()) {
        return false;
      }
    } catch (...) {
      return false;
    }
  } else {
    // numeric id. now check if it is all-numeric
    if (p != key + split) {
      return false;
    }
  }

  // validate document key
  return KeyGeneratorHelper::validateKey(key + split + 1, len - split - 1);
}

std::shared_ptr<KeyGenerator> KeyGeneratorHelper::createKeyGenerator(
  uint32_t number_of_shards, vpack::Slice options) {
  if (!options.isObject()) {
    options = vpack::Slice::emptyObjectSlice();
  }

  const auto type =
    basics::VPackHelper::getString(options, "type", "traditional");
  const auto it = kFactories.TryFindICaseByFirst(type);

  if (!it) {
    SDB_THROW(ERROR_SERVER_INVALID_KEY_GENERATOR, "invalid key generator type");
  }

  return (*it)(number_of_shards, options);
}

std::shared_ptr<KeyGenerator> KeyGeneratorHelper::createTraditional() {
  constexpr auto kIt = kFactories.TryFindByFirst("traditional");
  static_assert(kIt);
  return (*kIt)(0, vpack::Slice::emptyObjectSlice());
}

void KeyGenerator::toVPack(vpack::Builder& builder) const {
  SDB_ASSERT(!builder.isClosed());
  builder.add(StaticStrings::kAllowUserKeys, _allow_user_keys);
}

void KeyGenerator::initState(vpack::Slice state) {
  SDB_ASSERT(state.isObject());
  // default implementation is to simply read the lastValue attribute
  // as a number, and track its stringified version
  track(std::to_string(ReadLastValue(state)));
}

ErrorCode KeyGenerator::validate(std::string_view key, vpack::Slice /*input*/,
                                 bool is_restore) noexcept {
  return globalCheck(key.data(), key.size(), is_restore);
}

ErrorCode KeyGenerator::globalCheck(const char* p, size_t length,
                                    bool is_restore) const noexcept {
  // user has specified a key
  if (length > 0 && !_allow_user_keys && !is_restore &&
      !ServerState::instance()->IsDBServer()) {
    // we do not allow user-generated keys
    // note: on a DB server the coordinator will already have generated the key
    return ERROR_SERVER_DOCUMENT_KEY_UNEXPECTED;
  }

  if (length == 0 || length > KeyGeneratorHelper::kMaxKeyLength) {
    // user key is empty or user key is too long
    return ERROR_SERVER_DOCUMENT_KEY_BAD;
  }

  // validate user-supplied key
  if (!KeyGeneratorHelper::validateKey(p, length)) {
    return ERROR_SERVER_DOCUMENT_KEY_BAD;
  }

  return ERROR_OK;
}

}  // namespace sdb
