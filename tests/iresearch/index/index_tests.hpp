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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/timer_utils.hpp>

#include "assert_format.hpp"
#include "doc_generator.hpp"
#include "hook_file_system.hpp"
#include "postings_cursor.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

using namespace std::chrono_literals;

namespace irs {

struct TermAttr;

}

namespace tests {

irs::IndexWriterOptions CsDefaultWriterOptions();
irs::IndexReaderOptions CsDefaultReaderOptions();

// `db` is mandatory at IndexWriter::Make. Fixtures that build their own
// IndexWriterOptions (e.g. to set a custom scorer) often leave `db` unset;
// fill it from the shared CsDb() so they keep working. Also fills
// `norm_column_id` from the shared monotonic allocator when the caller
// didn't set one -- with a cs writer present, a Norm-featured field requires
// a norm_column_id callback (FieldsData::emplace asserts it).
irs::IndexWriterOptions EnsureWriterDb(irs::IndexWriterOptions opts);

duckdb::FileSystem& DatabaseFileSystem();

irs::IndexFiles OverFileSystem(const irs::IndexFiles& files,
                               duckdb::FileSystem& fs);

struct BlockingFileSystem : HookFileSystem {
  explicit BlockingFileSystem(const std::string& blocker)
    : HookFileSystem{DatabaseFileSystem()}, blocker(blocker) {}

  void OnCreated(std::string_view name) final {
    if (name == blocker) {
      {
        auto guard = std::unique_lock(policy_lock);
        created = true;
        policy_applied.notify_all();
      }

      // wait for intermediate commits to be applied
      auto guard = std::unique_lock(intermediate_commits_lock);
    }
  }

  void wait_for_blocker() {
    auto policy_guard = std::unique_lock(policy_lock);
    while (!created) {
      policy_applied.wait_for(policy_guard, 1000ms);
    }
  }

  std::string blocker;
  std::mutex policy_lock;
  std::condition_variable policy_applied;
  bool created = false;
  std::mutex intermediate_commits_lock;
};

struct CallbackFileSystem : HookFileSystem {
  using AfterCallback = std::function<void()>;

  explicit CallbackFileSystem(AfterCallback&& p)
    : HookFileSystem{DatabaseFileSystem()}, after(std::move(p)) {}

  void OnCreated(std::string_view) final { after(); }

  AfterCallback after;
};

typedef std::tuple<tests::dir_param_f> index_test_context;

void AssertSnapshotEquality(irs::DirectoryReader lhs, irs::DirectoryReader rhs);

// A posting list is written before anything is deleted, so a reader walking
// one sees documents the segment no longer has. Only a plan knows about the
// segment's mask, and a test that drives the postings directly builds none.
inline std::unique_ptr<PostingsCursor> MaskPostings(
  const irs::SubReader& segment, irs::TermPostings::ptr&& postings,
  irs::IndexFeatures features = irs::IndexFeatures::None) {
  return std::make_unique<PostingsCursor>(std::move(postings), features,
                                          segment.MaskedDocs());
}

class IndexTestBase : public virtual TestParamBase<index_test_context> {
 public:
  static std::string to_string(
    const testing::TestParamInfo<index_test_context>& info);

 protected:
  std::shared_ptr<irs::IndexFiles> get_directory(const TestBase& ctx) const;

  irs::IndexFiles& dir() const { return *_dir; }
  const index_t& index() const { return _index; }
  index_t& index() { return _index; }

  irs::doc_id_t GetPostingsBlockSize() const;

  irs::IndexWriter::ptr open_writer(
    const irs::IndexFiles& dir, irs::OpenMode mode = irs::kOmCreate,
    irs::IndexWriterOptions options = CsDefaultWriterOptions()) const {
    return irs::IndexWriter::Make(dir, mode,
                                  EnsureWriterDb(std::move(options)));
  }

  irs::IndexWriter::ptr open_writer(
    irs::OpenMode mode = irs::kOmCreate,
    irs::IndexWriterOptions options = CsDefaultWriterOptions()) const {
    return irs::IndexWriter::Make(*_dir, mode,
                                  EnsureWriterDb(std::move(options)));
  }

  irs::DirectoryReader open_reader(
    const irs::IndexReaderOptions& options = CsDefaultReaderOptions()) const {
    return irs::DirectoryReader{*_dir, options};
  }

  void AssertSnapshotEquality(const irs::IndexWriter& writer);

  void assert_index(irs::IndexFeatures features, size_t skip = 0,
                    const irs::RegexpAcceptor* acceptor = nullptr) const {
    tests::AssertIndex(open_reader().GetImpl(), index(), features, skip,
                       acceptor);
  }

  void SetUp() final {
    TestBase::SetUp();

    // set directory
    _dir = get_directory(*this);
    ASSERT_NE(nullptr, _dir);
  }

  void TearDown() final {
    _dir = nullptr;
    TestBase::TearDown();
    irs::timer_utils::InitStats();  // disable profile state tracking
  }

  // Called inside the per-doc insert transaction. Tests use it to copy
  // selected source fields into cs columns keyed by a per-file
  // `constexpr irs::field_id` constant (see `tests::StoreFieldAt`).
  using StoreHook =
    std::function<void(irs::IndexWriter::Document&, const tests::Document&)>;

  void write_segment(irs::IndexWriter& writer, tests::IndexSegment& segment,
                     tests::DocGeneratorBase& gen, const StoreHook& store = {});

  void write_segment_batched(irs::IndexWriter& writer,
                             tests::IndexSegment& segment,
                             tests::DocGeneratorBase& gen, size_t batch_size);

  void add_segment(irs::IndexWriter& writer, tests::DocGeneratorBase& gen,
                   const StoreHook& store = {});

  void add_segments(irs::IndexWriter& writer,
                    std::vector<DocGeneratorBase::ptr>& gens);

  void add_segment(tests::DocGeneratorBase& gen,
                   irs::OpenMode mode = irs::kOmCreate,
                   irs::IndexWriterOptions opts = CsDefaultWriterOptions(),
                   const StoreHook& store = {});
  void add_segment_batched(
    tests::DocGeneratorBase& gen, size_t batch_size,
    irs::OpenMode mode = irs::kOmCreate,
    irs::IndexWriterOptions opts = CsDefaultWriterOptions());

 private:
  index_t _index;
  std::shared_ptr<irs::IndexFiles> _dir;
};

}  // namespace tests
