////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <array>
#include <bit>
#include <iresearch/store/caching_directory.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/utils/ctr_encryption.hpp>
#include <iresearch/utils/type_id.hpp>
#include <memory>

#include "tests_shared.hpp"

class TestBase;

namespace tests {

class Rot13Encryption final : public irs::CtrEncryption {
 public:
  static std::shared_ptr<Rot13Encryption> make(
    size_t block_size, size_t header_length = kDefaultHeaderLength) {
    return std::make_shared<Rot13Encryption>(block_size, header_length);
  }

  explicit Rot13Encryption(size_t block_size,
                           size_t header_length = kDefaultHeaderLength) noexcept
    : irs::CtrEncryption(_cipher),
      _cipher(block_size),
      _header_length(header_length) {}

  size_t header_length() noexcept final { return _header_length; }

 private:
  class Rot13Cipher final : public irs::Cipher {
   public:
    explicit Rot13Cipher(size_t block_size) noexcept
      : _block_size(block_size) {}

    size_t block_size() const noexcept final { return _block_size; }

    bool Decrypt(irs::byte_type* data) const final {
      for (size_t i = 0; i < _block_size; ++i) {
        data[i] -= 13;
      }
      return true;
    }

    bool Encrypt(irs::byte_type* data) const final {
      for (size_t i = 0; i < _block_size; ++i) {
        data[i] += 13;
      }
      return true;
    }

   private:
    size_t _block_size;
  };

  Rot13Cipher _cipher;
  size_t _header_length;
};

template<typename Impl, typename... Args>
std::shared_ptr<irs::Directory> MakePhysicalDirectory(
  const TestBase* test, irs::DirectoryAttributes attrs, Args&&... args) {
  if (test) {
    const auto dir_path = test->test_dir() / "index";
    std::filesystem::create_directories(dir_path);

    auto dir = std::make_unique<Impl>(std::forward<Args>(args)..., dir_path,
                                      std::move(attrs),
                                      test->GetResourceManager().options);

    return {dir.release(), [dir_path = std::move(dir_path)](irs::Directory* p) {
              std::filesystem::remove_all(dir_path);
              delete p;
            }};
  }

  return nullptr;
}

std::shared_ptr<irs::Directory> MemoryDirectory(const TestBase*,
                                                irs::DirectoryAttributes attrs);
std::shared_ptr<irs::Directory> FsDirectory(const TestBase*,
                                            irs::DirectoryAttributes attrs);
std::shared_ptr<irs::Directory> MmapDirectory(const TestBase*,
                                              irs::DirectoryAttributes attrs);
#ifdef IRESEARCH_URING
std::shared_ptr<irs::Directory> AsyncDirectory(const TestBase*,
                                               irs::DirectoryAttributes attrs);
#endif

using dir_generator_f = std::shared_ptr<irs::Directory> (*)(
  const TestBase*, irs::DirectoryAttributes);

std::string ToString(dir_generator_f generator);

template<dir_generator_f DirectoryGenerator>
std::pair<std::shared_ptr<irs::Directory>, std::string> Directory(
  const TestBase* ctx) {
  auto dir = DirectoryGenerator(ctx, irs::DirectoryAttributes{});

  return std::make_pair(dir, ToString(DirectoryGenerator));
}

template<dir_generator_f DirectoryGenerator, size_t BlockSize>
std::pair<std::shared_ptr<irs::Directory>, std::string> Rot13Directory(
  const TestBase* ctx) {
  auto dir = DirectoryGenerator(
    ctx,
    irs::DirectoryAttributes{std::make_unique<Rot13Encryption>(BlockSize)});

  return std::make_pair(dir, ToString(DirectoryGenerator) + "_cipher_rot13_" +
                               std::to_string(BlockSize));
}

using dir_param_f =
  std::pair<std::shared_ptr<irs::Directory>, std::string> (*)(const TestBase*);

enum Types : uint64_t {
  kTypesDefault = 1 << 0,
  kTypesRot1316 = 1 << 2,
  kTypesRot137 = 1 << 3,
};

inline constexpr auto kTypesDefaultRot13 = kTypesDefault | kTypesRot1316;
inline constexpr auto kTypesAllRot13 = kTypesRot1316 | kTypesRot137;
inline constexpr auto kTypesAll = kTypesDefault | kTypesRot1316 | kTypesRot137;

// #define IRS_TEST_ONLY_MEMORY_DIR

template<uint64_t Type>
constexpr auto GetDirectories() {
  constexpr auto kCount = std::popcount(Type);
#ifdef IRS_TEST_ONLY_MEMORY_DIR
  std::array<dir_param_f, kCount * 1> data;
#elif defined(IRESEARCH_URING)
  std::array<dir_param_f, kCount * 4> data;
#else
  std::array<dir_param_f, kCount * 3> data;
#endif
  auto* p = data.data();
  if constexpr (Type & kTypesDefault) {
    *p++ = &tests::Directory<&tests::MemoryDirectory>;
#ifndef IRS_TEST_ONLY_MEMORY_DIR
#ifdef IRESEARCH_URING
    *p++ = &tests::directory<&tests::AsyncDirectory>;
#endif
    *p++ = &tests::Directory<&tests::MmapDirectory>;
    *p++ = &tests::Directory<&tests::FsDirectory>;
#endif
  }
  if constexpr (Type & kTypesRot1316) {
    *p++ = &tests::Rot13Directory<&tests::MemoryDirectory, 16>;
#ifndef IRS_TEST_ONLY_MEMORY_DIR
#ifdef IRESEARCH_URING
    *p++ = &tests::rot13_directory<&tests::AsyncDirectory, 16>;
#endif
    *p++ = &tests::Rot13Directory<&tests::MmapDirectory, 16>;
    *p++ = &tests::Rot13Directory<&tests::FsDirectory, 16>;
#endif
  }
  if constexpr (Type & kTypesRot137) {
    *p++ = &tests::Rot13Directory<&tests::MemoryDirectory, 7>;
#ifndef IRS_TEST_ONLY_MEMORY_DIR
#ifdef IRESEARCH_URING
    *p++ = &tests::rot13_directory<&tests::AsyncDirectory, 7>;
#endif
    *p++ = &tests::Rot13Directory<&tests::MmapDirectory, 7>;
    *p++ = &tests::Rot13Directory<&tests::FsDirectory, 7>;
#endif
  }
  return data;
}

template<typename... Args>
class DirectoryTestCaseBase
  : public virtual TestParamBase<std::tuple<tests::dir_param_f, Args...>> {
 public:
  using ParamType = std::tuple<tests::dir_param_f, Args...>;

  static std::string to_string(const testing::TestParamInfo<ParamType>& info) {
    auto& p = info.param;
    return (*std::get<0>(p))(nullptr).second;
  }

  void SetUp() final {
    TestBase::SetUp();

    auto& p =
      TestParamBase<std::tuple<tests::dir_param_f, Args...>>::GetParam();

    auto* factory = std::get<0>(p);
    ASSERT_NE(nullptr, factory);

    _dir = (*factory)(this).first;
    ASSERT_NE(nullptr, _dir);
  }

  void TearDown() override {
    _dir = nullptr;
    TestBase::TearDown();
  }

  irs::Directory& dir() const noexcept { return *_dir; }

 protected:
  std::shared_ptr<irs::Directory> _dir;
};

}  // namespace tests

namespace irs {

template<>
struct Type<tests::Rot13Encryption> : Type<Encryption> {};

}  // namespace irs
