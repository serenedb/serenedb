////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Julia Puget
/// @author Copyright 2022, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/container/flat_hash_set.h>
#include <absl/random/random.h>

#include <atomic>
#include <charconv>
#include <cmath>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "vpack/exception_macros.h"
#include "vpack/vpack.h"

using namespace vpack;

struct VPackFormat {};

struct JSONFormat {};

enum FuzzBytes {
  kFirstByte = 0,
  kRandomByte,
  kLastByte,
  kNumOptionsBytes,
};

enum FuzzActions {
  kRemoveByte = 0,
  kReplaceByte,
  kInsertByte,
  kNumOptionsActions
};

enum RandomBuilderAdditions {
  kAddArray = 0,
  kAddObject,
  kAddBoolean,
  kAddString,
  kAddNull,
  kAddUinT64,
  kAddInT64,
  kAddDouble,  // here starts values that are only for vpack
  kAddMaxVPackValue,
};

struct KnownLimitValues {
  static constexpr uint8_t kUtf81ByteFirstLowerBound = 0x00;
  static constexpr uint8_t kUtf81ByteFirstUpperBound = 0x7F;
  static constexpr uint8_t kUtf82BytesFirstLowerBound = 0xC2;
  static constexpr uint8_t kUtf82BytesFirstUpperBound = 0xDF;
  static constexpr uint8_t kUtf83BytesFirstLowerBound = 0xE0;
  static constexpr uint8_t kUtf83BytesFirstUpperBound = 0xEF;
  static constexpr uint8_t kUtf83BytesE0ValidatorLowerBound = 0xA0;
  static constexpr uint8_t kUtf83BytesEdValidatorUpperBound = 0x9F;
  static constexpr uint8_t kUtf84BytesFirstLowerBound = 0xF0;
  static constexpr uint8_t kUtf84BytesFirstUpperBound = 0xF4;
  static constexpr uint8_t kUtf84BytesF0ValidatorLowerBound = 0x90;
  static constexpr uint8_t kUtf84BytesF4ValidatorUpperBound = 0x8F;
  static constexpr uint8_t kUtf8CommonLowerBound = 0x80;
  static constexpr uint8_t kUtf8CommonUpperBound = 0xBF;
  static constexpr uint32_t kMinUtf8RandStringLength = 0;
  static constexpr uint32_t kMaxUtf8RandStringLength = 1000;
  static constexpr uint32_t kMaxBinaryRandLength = 1000;
  static constexpr uint32_t kMaxDepth = 10;
  static constexpr uint32_t kObjNumMembers = 10;
  static constexpr uint32_t kArrayNumMembers = 10;
  static constexpr uint32_t kRandBytePosOffset = 8;
  static constexpr uint32_t kNumFuzzIterations = 10;
};

struct RandomGenerator {
  RandomGenerator(uint64_t seed) : mt64(seed) {}

  std::mt19937_64 mt64;  // this is 64 bits for generating uint64_t in
                         // ADD_UINT64 for vpack
};

struct BuilderContext {
  Builder builder;
  RandomGenerator random_generator;
  std::string temp_string;
  std::vector<absl::flat_hash_set<std::string>> temp_object_keys;
  uint32_t recursion_depth = 0;
  bool is_evil;

  BuilderContext() = delete;
  BuilderContext(const BuilderContext&) = delete;
  BuilderContext& operator=(const BuilderContext&) = delete;

  BuilderContext(const Options* options, uint64_t seed, bool is_evil)
    : builder(options), random_generator(seed), is_evil(is_evil) {}
};

Slice gNullSlice(Slice::nullSlice());

static constinit absl::Mutex gMtx{absl::kConstInit};

static void Usage(const char* argv[]) {
  std::cout << "Usage: " << argv[0] << " options" << std::endl;
  std::cout << "This program creates random VPack or JSON structures and "
               "validates them. (Default: VPack)"
            << std::endl;
  std::cout << "Available format options are:" << std::endl;
  std::cout << " --vpack                create VPack" << std::endl;
  std::cout << " --json                 create JSON" << std::endl;
  std::cout << " --evil                 purposefully fuzz generated VPack, to "
               "test for crashes"
            << std::endl;
  std::cout
    << " --iterations <number>  number of iterations (number > 0). Default: 1"
    << std::endl;
  std::cout
    << " --threads <number>     number of threads (number > 0). Default: 1"
    << std::endl;
  std::cout << " --seed <number>        number that will be used as seed for "
               "random generation (number >= 0). Default: random value"
            << std::endl;
  std::cout << std::endl;
}

static inline bool IsOption(const char* arg, const char* expected) {
  return (strcmp(arg, expected) == 0);
}

template<typename T>
static T GenerateRandWithinRange(T min, T max,
                                 RandomGenerator& random_generator) {
  return min + (random_generator.mt64() % (max + 1 - min));
}

static size_t GetBytePos(BuilderContext& ctx, const FuzzBytes& fuzz_byte,
                         const std::string& input) {
  size_t input_size = input.size();
  if (!input_size) {
    return 0;
  }
  using Limits = KnownLimitValues;
  size_t offset = input_size > (3 * Limits::kRandBytePosOffset)
                    ? Limits::kRandBytePosOffset
                    : 0;
  size_t byte_pos = 0;
  if (fuzz_byte == FuzzBytes::kFirstByte) {
    byte_pos = GenerateRandWithinRange<size_t>(0, offset, ctx.random_generator);
  } else if (fuzz_byte == FuzzBytes::kRandomByte) {
    byte_pos =
      GenerateRandWithinRange<size_t>(0, input_size - 1, ctx.random_generator);
  } else if (fuzz_byte == FuzzBytes::kLastByte) {
    byte_pos = GenerateRandWithinRange<size_t>(
      input_size - 1 - offset, input_size - 1, ctx.random_generator);
  }
  return byte_pos;
}

static void ProcessBytes(BuilderContext& ctx) {
  using Limits = KnownLimitValues;
  for (uint32_t i = 0; i < Limits::kNumFuzzIterations; ++i) {
    FuzzActions fuzz_action = static_cast<FuzzActions>(
      ctx.random_generator.mt64() % FuzzActions::kNumOptionsActions);
    FuzzBytes fuzz_byte = static_cast<FuzzBytes>(ctx.random_generator.mt64() %
                                                 FuzzBytes::kNumOptionsBytes);
    auto& builder_buffer = ctx.builder.bufferRef();
    std::string vpack_bytes{builder_buffer.toString()};
    size_t byte_pos = GetBytePos(ctx, fuzz_byte, vpack_bytes);
    switch (fuzz_action) {
      case kRemoveByte:
        if (vpack_bytes.empty()) {
          continue;
        }
        vpack_bytes.erase(byte_pos, 1);
        break;
      case kReplaceByte:
        if (vpack_bytes.empty()) {
          continue;
        }
        vpack_bytes[byte_pos] =
          GenerateRandWithinRange<uint8_t>(0, 255, ctx.random_generator);
        break;
      case kInsertByte:
        vpack_bytes.insert(
          byte_pos, 1,
          static_cast<std::string::value_type>(
            GenerateRandWithinRange<uint8_t>(0, 255, ctx.random_generator)));
        break;
      default:
        SDB_ASSERT(false);
    }
    builder_buffer.clear();
    builder_buffer.append(reinterpret_cast<const uint8_t*>(vpack_bytes.data()),
                          vpack_bytes.size());
  }
}

[[maybe_unused]] static void GenerateRandBinary(
  RandomGenerator& random_generator, std::string& output) {
  using Limits = KnownLimitValues;
  uint32_t length = random_generator.mt64() % Limits::kMaxBinaryRandLength;
  output.reserve(length);
  for (uint32_t i = 0; i < length; ++i) {
    output.push_back(
      static_cast<std::string::value_type>(random_generator.mt64() % 256));
  }
}

static void AppendRandUtf8Char(RandomGenerator& random_generator,
                               std::string& utf8_str) {
  using Limits = KnownLimitValues;
  uint32_t num_bytes = GenerateRandWithinRange(1, 4, random_generator);
  switch (num_bytes) {
    case 1: {
      utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
        Limits::kUtf81ByteFirstLowerBound, Limits::kUtf81ByteFirstUpperBound,
        random_generator));
      break;
    }
    case 2: {
      utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
        Limits::kUtf82BytesFirstLowerBound, Limits::kUtf82BytesFirstUpperBound,
        random_generator));
      utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
        Limits::kUtf8CommonLowerBound, Limits::kUtf8CommonUpperBound,
        random_generator));
      break;
    }
    case 3: {
      uint32_t rand_first_byte = GenerateRandWithinRange<uint8_t>(
        Limits::kUtf83BytesFirstLowerBound, Limits::kUtf83BytesFirstUpperBound,
        random_generator);
      utf8_str.push_back(rand_first_byte);
      if (rand_first_byte == 0xE0) {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf83BytesE0ValidatorLowerBound,
          Limits::kUtf8CommonUpperBound, random_generator));
      } else if (rand_first_byte == 0xED) {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf8CommonLowerBound,
          Limits::kUtf83BytesEdValidatorUpperBound, random_generator));
      } else {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf8CommonLowerBound, Limits::kUtf8CommonUpperBound,
          random_generator));
      }
      utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
        Limits::kUtf8CommonLowerBound, Limits::kUtf8CommonUpperBound,
        random_generator));
      break;
    }
    case 4: {
      uint32_t rand_first_byte = GenerateRandWithinRange<uint8_t>(
        Limits::kUtf84BytesFirstLowerBound, Limits::kUtf84BytesFirstUpperBound,
        random_generator);
      utf8_str.push_back(rand_first_byte);
      if (rand_first_byte == 0xF0) {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf84BytesF0ValidatorLowerBound,
          Limits::kUtf8CommonUpperBound, random_generator));
      } else if (rand_first_byte == 0xF4) {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf8CommonLowerBound,
          Limits::kUtf84BytesF4ValidatorUpperBound, random_generator));
      } else {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf8CommonLowerBound, Limits::kUtf8CommonUpperBound,
          random_generator));
      }
      for (uint32_t i = 0; i < 2; ++i) {
        utf8_str.push_back(GenerateRandWithinRange<uint8_t>(
          Limits::kUtf8CommonLowerBound, Limits::kUtf8CommonUpperBound,
          random_generator));
      }
      break;
    }
    default:
      SDB_ASSERT(false);
  }
}

static void GenerateUtf8String(RandomGenerator& random_generator,
                               std::string& utf8_str) {
  using Limits = KnownLimitValues;
  static_assert(Limits::kMaxUtf8RandStringLength >
                Limits::kMinUtf8RandStringLength);

  uint32_t length =
    Limits::kMinUtf8RandStringLength +
    (random_generator.mt64() %
     (Limits::kMaxUtf8RandStringLength - Limits::kMinUtf8RandStringLength));
  for (uint32_t i = 0; i < length; ++i) {
    AppendRandUtf8Char(random_generator, utf8_str);
  }
}

template<typename Format>
constexpr RandomBuilderAdditions MaxRandomType() {
  if constexpr (std::is_same_v<Format, VPackFormat>) {
    return RandomBuilderAdditions::kAddMaxVPackValue;
  } else {
    return RandomBuilderAdditions::kAddDouble;
  }
}

template<typename Format>
static void GenerateVPack(BuilderContext& ctx) {
  constexpr RandomBuilderAdditions kMaxValue = MaxRandomType<Format>();

  using Limits = KnownLimitValues;

  Builder& builder = ctx.builder;
  RandomGenerator& random_generator = ctx.random_generator;

  while (true) {
    RandomBuilderAdditions random_builder_adds =
      static_cast<RandomBuilderAdditions>(random_generator.mt64() % kMaxValue);
    if (ctx.recursion_depth > Limits::kMaxDepth &&
        random_builder_adds <= kAddObject) {
      continue;
    }
    switch (random_builder_adds) {
      case kAddArray: {
        builder.openArray(random_generator.mt64() % 2 ? true : false);
        uint32_t num_members =
          random_generator.mt64() % Limits::kArrayNumMembers;
        for (uint32_t i = 0; i < num_members; ++i) {
          ++ctx.recursion_depth;
          GenerateVPack<Format>(ctx);
          --ctx.recursion_depth;
        }
        builder.close();
        break;
      }
      case kAddObject: {
        builder.openObject(random_generator.mt64() % 2 ? true : false);
        uint32_t num_members = random_generator.mt64() % Limits::kObjNumMembers;
        if (ctx.temp_object_keys.size() <= ctx.recursion_depth) {
          ctx.temp_object_keys.resize(ctx.recursion_depth + 1);
        } else {
          ctx.temp_object_keys[ctx.recursion_depth].clear();
        }
        SDB_ASSERT(ctx.temp_object_keys.size() > ctx.recursion_depth);
        ++ctx.recursion_depth;
        for (uint32_t i = 0; i < num_members; ++i) {
          while (true) {
            ctx.temp_string.clear();
            GenerateUtf8String(random_generator, ctx.temp_string);
            if (ctx.temp_object_keys[ctx.recursion_depth - 1]
                  .emplace(ctx.temp_string)
                  .second) {
              break;
            }
          }
          // key
          builder.add(ctx.temp_string);
          // value
          GenerateVPack<Format>(ctx);
        }
        --ctx.recursion_depth;
        ctx.temp_object_keys[ctx.recursion_depth].clear();
        builder.close();
        break;
      }
      case kAddBoolean:
        builder.add(random_generator.mt64() % 2 ? true : false);
        break;
      case kAddString: {
        ctx.temp_string.clear();
        GenerateUtf8String(random_generator, ctx.temp_string);
        builder.add(ctx.temp_string);
        break;
      }
      case kAddNull:
        builder.add(Value(ValueType::Null));
        break;
      case kAddUinT64: {
        uint64_t value = random_generator.mt64();
        builder.add(value);
        break;
      }
      case kAddInT64: {
        uint64_t uint_value = random_generator.mt64();
        int64_t int_value;
        memcpy(&int_value, &uint_value, sizeof(uint_value));
        builder.add(int_value);
        break;
      }
      case kAddDouble: {
        double double_value;
        do {
          uint64_t uint_value = random_generator.mt64();
          memcpy(&double_value, &uint_value, sizeof(uint_value));
        } while (!std::isfinite(double_value));
        builder.add(double_value);
        break;
      }
      default:
        SDB_ASSERT(false);
    }
    break;
  }
}

static bool IsParamValid(const char* p, uint64_t& value) {
  auto result = std::from_chars(p, p + strlen(p), value);
  if (result.ec != std::errc()) {
    std::cerr << "Error: wrong parameter type: " << p << std::endl;
    return false;
  }
  return true;
}

int main(int argc, const char* argv[]) {
  VPACK_GLOBAL_EXCEPTION_TRY

  bool is_type_assigned = false;
  uint32_t num_iterations = 1;
  uint32_t num_threads = 1;
  bool is_json = false;
  absl::BitGen gen;
  uint64_t seed = gen();
  bool is_evil = false;

  int i = 1;
  while (i < argc) {
    bool is_failure = false;
    const char* p = argv[i];
    if (IsOption(p, "--help")) {
      Usage(argv);
      return EXIT_SUCCESS;
    } else if (IsOption(p, "--vpack") && !is_type_assigned) {
      is_type_assigned = true;
      is_json = false;
    } else if (IsOption(p, "--json") && !is_type_assigned) {
      is_type_assigned = true;
      is_json = true;
    } else if (IsOption(p, "--iterations")) {
      if (++i >= argc) {
        is_failure = true;
      } else {
        const char* p = argv[i];
        uint64_t value = 0;  // these values are uint64_t only for using the
                             // same isParamValid function as the seed
        if (!IsParamValid(p, value) || !value ||
            value > std::numeric_limits<uint32_t>::max()) {
          is_failure = true;
        } else {
          num_iterations = static_cast<uint32_t>(value);
        }
      }
    } else if (IsOption(p, "--threads")) {
      if (++i >= argc) {
        is_failure = true;
      } else {
        const char* p = argv[i];
        uint64_t value = 0;
        if (!IsParamValid(p, value) || !value) {
          is_failure = true;
        } else {
          num_threads = static_cast<uint32_t>(value);
        }
      }
    } else if (IsOption(p, "--seed")) {
      if (++i >= argc) {
        is_failure = true;
      } else {
        const char* p = argv[i];
        uint64_t value = 0;
        if (!IsParamValid(p, value)) {
          is_failure = true;
        } else {
          seed = value;
        }
      }
    } else if (IsOption(p, "--evil")) {
      is_evil = true;
    } else {
      is_failure = true;
    }
    if (is_failure) {
      Usage(argv);
      return EXIT_FAILURE;
    }
    ++i;
  }

  if (is_evil && is_json) {
    std::cerr << "combining --evil and --json is currently not supported"
              << std::endl;
    return EXIT_FAILURE;
  }

  const std::string_view prog_name(is_evil ? "Darkness fuzzer"
                                           : "Sunshine fuzzer");

  std::cout << prog_name << " is fuzzing "
            << (is_json ? "JSON Parser" : "VPack validator") << " with "
            << num_threads << " thread(s)..."
            << " Iterations: " << num_iterations << ". Initial seed is " << seed
            << std::endl;

  uint32_t its_per_thread = num_iterations / num_threads;
  uint32_t leftover_its = num_iterations % num_threads;
  std::atomic<bool> stop_threads{false};
  std::atomic<uint64_t> total_validation_errors{0};

  auto thread_callback =
    [&stop_threads, &total_validation_errors]<typename Format>(
      uint32_t iterations, Format, uint64_t seed, bool is_evil) {
      Options options;
      options.validate_utf8_strings = true;
      options.check_attribute_uniqueness = true;

      // extra options for parsing
      Options parse_options = options;
      parse_options.clear_builder_before_parse = true;
      parse_options.padding_behavior = Options::PaddingBehavior::kUsePadding;
      parse_options.dump_attributes_in_index_order = false;

      Options validation_options = options;

      // parser used for JSON-parsing
      Parser parser(&parse_options);
      // validator used for VPack validation
      Validator validator(&validation_options);

      uint64_t validation_errors = 0;
      BuilderContext ctx(&options, seed, is_evil);

      try {
        while (iterations-- > 0 &&
               !stop_threads.load(std::memory_order_relaxed)) {
          // clean up for new iteration
          ctx.builder.clear();
          ctx.temp_object_keys.clear();
          SDB_ASSERT(ctx.recursion_depth == 0);

          // generate vpack
          GenerateVPack<Format>(ctx);

          if (is_evil) {
            // intentionally bork the vpack
            ProcessBytes(ctx);
          }

          try {
            auto& builder_buffer = ctx.builder.bufferRef();
            if constexpr (std::is_same_v<Format, JSONFormat>) {
              Slice s(builder_buffer.data());
              SDB_ASSERT(s.byteSize() == builder_buffer.size());
              ctx.temp_string.clear();
              s.toJson(ctx.temp_string);
              parser.parse(ctx.temp_string);
            } else {
              validator.validate(builder_buffer.data(), builder_buffer.size());
            }
          } catch (const std::exception& e) {
            ++validation_errors;
            if (!is_evil) {
              // we don't expect any exceptions in non-evil mode, so rethrow
              // them.
              throw;
            }
            // in evil mode, we can't tell if the borked vpack is valid or
            // not. so have to catch the exception and silence it.
          }
        }
      } catch (const std::exception& e) {
        // need a mutex to prevent multiple threads from writing to std::cerr at
        // the same time
        std::lock_guard lock(gMtx);
        std::cerr << "Caught unexpected exception during fuzzing: " << e.what()
                  << std::endl
                  << "Slice data:" << std::endl;
        auto& builder_buffer = ctx.builder.bufferRef();
        if constexpr (std::is_same_v<Format, JSONFormat>) {
          std::cerr << Slice(builder_buffer.data()).toJson() << std::endl;
        } else {
          std::cerr << HexDump(builder_buffer.data(), builder_buffer.size())
                    << std::endl;
        }
        stop_threads.store(true);
      }
      // add to global counter
      total_validation_errors.fetch_add(validation_errors);
    };

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  auto join_threads = [&threads]() {
    for (auto& t : threads) {
      t.join();
    }
  };

  try {
    for (uint32_t i = 0; i < num_threads; ++i) {
      uint32_t iterations = its_per_thread;
      if (i == num_threads - 1) {
        iterations += leftover_its;
      }
      if (is_json) {
        threads.emplace_back(thread_callback, iterations, JSONFormat{},
                             seed + i, is_evil);
      } else {
        threads.emplace_back(thread_callback, iterations, VPackFormat{},
                             seed + i, is_evil);
      }
    }
    join_threads();
  } catch (const std::exception&) {
    stop_threads.store(true);
    join_threads();
  }

  std::cout << "Caught " << total_validation_errors.load()
            << " validation error(s).";
  if (is_evil) {
    std::cout << " These are potentially legitimate.";
  }
  std::cout << std::endl;

  // return proper exit code
  return stop_threads.load() ? EXIT_FAILURE : EXIT_SUCCESS;

  VPACK_GLOBAL_EXCEPTION_CATCH
}
