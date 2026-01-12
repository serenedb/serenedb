////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/timer_utils.hpp>

#include "assert_format.hpp"
#include "doc_generator.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

using namespace std::chrono_literals;

namespace irs {

struct TermAttr;

}

namespace tests {

class DirectoryMock : public irs::Directory {
 public:
  DirectoryMock(irs::Directory& impl)
    : Directory{impl.ResourceManager()}, _impl(impl) {}

  using Directory::attributes;

  irs::DirectoryAttributes& attributes() noexcept final {
    return _impl.attributes();
  }

  irs::IndexOutput::ptr create(std::string_view name) noexcept override {
    return _impl.create(name);
  }

  bool exists(bool& result, std::string_view name) const noexcept override {
    return _impl.exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept override {
    return _impl.length(result, name);
  }

  irs::IndexLock::ptr make_lock(std::string_view name) noexcept override {
    return _impl.make_lock(name);
  }

  bool mtime(std::time_t& result,
             std::string_view name) const noexcept override {
    return _impl.mtime(result, name);
  }

  irs::IndexInput::ptr open(std::string_view name,
                            irs::IOAdvice advice) const noexcept override {
    return _impl.open(name, advice);
  }

  bool remove(std::string_view name) noexcept override {
    return _impl.remove(name);
  }

  bool rename(std::string_view src, std::string_view dst) noexcept override {
    return _impl.rename(src, dst);
  }

  bool sync(std::span<const std::string_view> files) noexcept override {
    return _impl.sync(files);
  }

  bool visit(const irs::Directory::visitor_f& visitor) const final {
    return _impl.visit(visitor);
  }

 private:
  irs::Directory& _impl;
};

struct BlockingDirectory : DirectoryMock {
  explicit BlockingDirectory(irs::Directory& impl, const std::string& blocker)
    : tests::DirectoryMock(impl), blocker(blocker) {}

  irs::IndexOutput::ptr create(std::string_view name) noexcept final {
    auto stream = tests::DirectoryMock::create(name);

    if (name == blocker) {
      {
        auto guard = std::unique_lock(policy_lock);
        policy_applied.notify_all();
      }

      // wait for intermediate commits to be applied
      auto guard = std::unique_lock(intermediate_commits_lock);
    }

    return stream;
  }

  void wait_for_blocker() {
    bool has = false;
    exists(has, blocker);

    while (!has) {
      exists(has, blocker);

      auto policy_guard = std::unique_lock(policy_lock);
      policy_applied.wait_for(policy_guard, 1000ms);
    }
  }

  std::string blocker;
  std::mutex policy_lock;
  std::condition_variable policy_applied;
  std::mutex intermediate_commits_lock;
};

struct CallbackDirectory : DirectoryMock {
  typedef std::function<void()> AfterCallback;

  explicit CallbackDirectory(irs::Directory& impl, AfterCallback&& p)
    : tests::DirectoryMock(impl), after(p) {}

  irs::IndexOutput::ptr create(std::string_view name) noexcept final {
    auto stream = tests::DirectoryMock::create(name);
    after();
    return stream;
  }

  AfterCallback after;
};

struct FormatInfo {
  constexpr FormatInfo(const char* codec = "") noexcept : codec(codec) {}

  const char* codec;
};

typedef std::tuple<tests::dir_param_f, FormatInfo> index_test_context;

void AssertSnapshotEquality(irs::DirectoryReader lhs, irs::DirectoryReader rhs);

class IndexTestBase : public virtual TestParamBase<index_test_context> {
 public:
  static std::string to_string(
    const testing::TestParamInfo<index_test_context>& info);

 protected:
  std::shared_ptr<irs::Directory> get_directory(const TestBase& ctx) const;

  irs::Format::ptr get_codec() const;

  irs::Directory& dir() const { return *_dir; }
  irs::Format::ptr codec() const { return _codec; }
  const index_t& index() const { return _index; }
  index_t& index() { return _index; }

  irs::doc_id_t GetPostingsBlockSize() const;

  void sort(const irs::Comparer& comparator) {
    for (auto& segment : _index) {
      segment.sort(comparator);
    }
  }

  irs::IndexWriter::ptr open_writer(
    irs::Directory& dir, irs::OpenMode mode = irs::kOmCreate,
    const irs::IndexWriterOptions& options = {}) const {
    return irs::IndexWriter::Make(dir, _codec, mode, options);
  }

  irs::IndexWriter::ptr open_writer(
    irs::OpenMode mode = irs::kOmCreate,
    const irs::IndexWriterOptions& options = {}) const {
    return irs::IndexWriter::Make(*_dir, _codec, mode, options);
  }

  irs::DirectoryReader open_reader(
    const irs::IndexReaderOptions& options = {}) const {
    return irs::DirectoryReader{*_dir, _codec, options};
  }

  void AssertSnapshotEquality(const irs::IndexWriter& writer);

  void assert_index(irs::IndexFeatures features, size_t skip = 0,
                    irs::automaton_table_matcher* matcher = nullptr) const {
    tests::AssertIndex(open_reader().GetImpl(), index(), features, skip,
                       matcher);
  }

  void assert_columnstore(size_t skip = 0) const {
    tests::AssertColumnstore(open_reader().GetImpl(), index(), skip);
  }

  void SetUp() final {
    TestBase::SetUp();

    // set directory
    _dir = get_directory(*this);
    ASSERT_NE(nullptr, _dir);

    // set codec
    _codec = get_codec();
    ASSERT_NE(nullptr, _codec);
  }

  void TearDown() final {
    _dir = nullptr;
    _codec = nullptr;
    TestBase::TearDown();
    irs::timer_utils::InitStats();  // disable profile state tracking
  }

  void write_segment(irs::IndexWriter& writer, tests::IndexSegment& segment,
                     tests::DocGeneratorBase& gen);

  void write_segment_batched(irs::IndexWriter& writer,
                             tests::IndexSegment& segment,
                             tests::DocGeneratorBase& gen, size_t batch_size);

  void add_segment(irs::IndexWriter& writer, tests::DocGeneratorBase& gen);

  void add_segments(irs::IndexWriter& writer,
                    std::vector<DocGeneratorBase::ptr>& gens);

  void add_segment(tests::DocGeneratorBase& gen,
                   irs::OpenMode mode = irs::kOmCreate,
                   const irs::IndexWriterOptions& opts = {});
  void add_segment_batched(tests::DocGeneratorBase& gen, size_t batch_size,
                           irs::OpenMode mode = irs::kOmCreate,
                           const irs::IndexWriterOptions& opts = {});

 private:
  index_t _index;
  std::shared_ptr<irs::Directory> _dir;
  irs::Format::ptr _codec;
};

}  // namespace tests
