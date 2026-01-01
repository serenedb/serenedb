////////////////////////////////////////////////////////////////////////////////
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/memory_directory.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;
using namespace irs;

class DoclistTestIterator : public DocIterator, private util::Noncopyable {
 public:
  DoclistTestIterator(const std::vector<doc_id_t>& documents)
    : _begin(documents.begin()),
      _end(documents.end()),
      _cost(documents.size()) {
    Reset();
  }

  bool next() final {
    if (_resetted) {
      _resetted = false;
      _current = _begin;
    }

    if (_current != _end) {
      _doc.value = *_current;
      ++_current;
      return true;
    } else {
      _doc.value = doc_limits::eof();
      return false;
    }
  }

  doc_id_t seek(doc_id_t target) final {
    while (_doc.value < target && next()) {
    }
    return _doc.value;
  }

  doc_id_t value() const noexcept final { return _doc.value; }

  Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    if (irs::Type<irs::DocAttr>::id() == id) {
      return &_doc;
    }
    if (irs::Type<irs::CostAttr>::id() == id) {
      return &_cost;
    }
    return nullptr;
  }

  void Reset() noexcept {
    _current = _end;
    _resetted = true;
    _doc.value = doc_limits::invalid();
  }

 private:
  std::vector<doc_id_t>::const_iterator _begin;
  std::vector<doc_id_t>::const_iterator _current;
  std::vector<doc_id_t>::const_iterator _end;
  irs::DocAttr _doc;
  irs::CostAttr _cost;
  bool _resetted;
};

class DoclistTestQuery : public Filter::Query {
 public:
  DoclistTestQuery(const std::vector<doc_id_t>& documents, score_t)
    : _documents(documents) {}

  DocIterator::ptr execute(const ExecutionContext&) const final {
    ++gExecutes;
    return memory::make_managed<DoclistTestIterator>(_documents);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }

  irs::score_t Boost() const noexcept final { return kNoBoost; }

  static size_t GetExecs() noexcept { return gExecutes; }

  static void ResetExecs() noexcept { gExecutes = 0; }

 private:
  const std::vector<doc_id_t>& _documents;
  static size_t gExecutes;
};

size_t DoclistTestQuery::gExecutes{0};

class DoclistTestFilter : public Filter {
 public:
  static size_t GetPrepares() noexcept { return gPrepares; }

  static void ResetPrepares() noexcept { gPrepares = 0; }

  Filter::Query::ptr prepare(const PrepareContext& ctx) const final {
    ++gPrepares;
    return memory::make_tracked<DoclistTestQuery>(ctx.memory, *_documents,
                                                  ctx.boost);
  }

  void SetExpected(const std::vector<doc_id_t>& documents) {
    _documents = &documents;
  }

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<DoclistTestFilter>::id();
  }

 private:
  const std::vector<doc_id_t>* _documents;
  inline static size_t gPrepares = 0;
};

class ProxyFilterTestCase : public ::testing::TestWithParam<size_t> {
 public:
  ProxyFilterTestCase() {
    auto codec = irs::formats::Get("1_5");
    auto writer = irs::IndexWriter::Make(_dir, codec, irs::kOmCreate);
    {  // make dummy document so we could have non-empty index
      auto ctx = writer->GetBatch();
      for (size_t i = 0; i < GetParam(); ++i) {
        auto doc = ctx.Insert();
        auto field = std::make_shared<tests::StringField>("foo", "bar");
        doc.Insert<Action::INDEX>(*field);
      }
    }
    writer->Commit();
    _index = irs::DirectoryReader(_dir, codec);
    AssertSnapshotEquality(writer->GetSnapshot(), _index);
  }

 protected:
  void SetUp() final {
    DoclistTestQuery::ResetExecs();
    DoclistTestFilter::ResetPrepares();
  }

  void VerifyFilter(const std::vector<doc_id_t>& expected, size_t line) {
    SCOPED_TRACE(::testing::Message("Failed on line: ") << line);
    MaxMemoryCounter prepare_counter;
    MaxMemoryCounter execute_counter;

    irs::ProxyFilter::cache_ptr cache;
    for (size_t i = 0; i < 3; ++i) {
      ProxyFilter proxy;
      if (i == 0) {
        auto res =
          proxy.set_filter<DoclistTestFilter>(irs::IResourceManager::gNoop);
        cache = res.second;
        res.first.SetExpected(expected);
      } else {
        proxy.set_cache(cache);
      }
      auto prepared_proxy = proxy.prepare({
        .index = _index,
        .memory = prepare_counter,
      });
      auto docs = prepared_proxy->execute({
        .segment = _index[0],
        .memory = execute_counter,
      });
      auto costs = irs::get<irs::CostAttr>(*docs);
      EXPECT_TRUE(costs);
      EXPECT_EQ(costs->estimate(), expected.size());
      auto expected_doc = expected.begin();
      while (docs->next() && expected_doc != expected.end()) {
        EXPECT_EQ(docs->value(), *expected_doc);
        ++expected_doc;
      }
      EXPECT_FALSE(docs->next());
      EXPECT_EQ(expected_doc, expected.end());
    }
    // Real filter should be exectued just once
    EXPECT_EQ(DoclistTestQuery::GetExecs(), 1);
    EXPECT_EQ(DoclistTestFilter::GetPrepares(), 1);

    cache.reset();

    EXPECT_EQ(prepare_counter.current, 0);
    EXPECT_GT(prepare_counter.max, 0);
    prepare_counter.Reset();

    EXPECT_EQ(execute_counter.current, 0);
    EXPECT_GT(execute_counter.max, 0);
    execute_counter.Reset();
  }

  irs::MemoryDirectory _dir;
  irs::DirectoryReader _index;
};

TEST_P(ProxyFilterTestCase, test_1first_bit) {
  std::vector<doc_id_t> documents{1};
  VerifyFilter(documents, __LINE__);
}

TEST_P(ProxyFilterTestCase, test_last_bit) {
  std::vector<doc_id_t> documents{63};
  VerifyFilter(documents, __LINE__);
}

TEST_P(ProxyFilterTestCase, test_2first_bit) {
  if (GetParam() >= 64) {
    std::vector<doc_id_t> documents{64};
    VerifyFilter(documents, __LINE__);
  }
}

TEST_P(ProxyFilterTestCase, test_2last_bit) {
  std::vector<doc_id_t> documents{static_cast<doc_id_t>(GetParam()) - 1};
  VerifyFilter(documents, __LINE__);
}

TEST_P(ProxyFilterTestCase, test_1last_2first_bit) {
  if (GetParam() >= 64) {
    std::vector<doc_id_t> documents{63, 64};
    VerifyFilter(documents, __LINE__);
  }
}

TEST_P(ProxyFilterTestCase, test_1first_2last_bit) {
  std::vector<doc_id_t> documents{1, static_cast<doc_id_t>(GetParam()) - 1};
  VerifyFilter(documents, __LINE__);
}

TEST_P(ProxyFilterTestCase, test_full_dense) {
  std::vector<doc_id_t> documents(GetParam());
  std::iota(documents.begin(), documents.end(), irs::doc_limits::min());
  VerifyFilter(documents, __LINE__);
}

INSTANTIATE_TEST_SUITE_P(proxy_filter_test_case, ProxyFilterTestCase,
                         ::testing::Values(10, 15, 64, 100, 128));

class ProxyFilterRealFilter : public tests::FilterTestCaseBase {
 public:
  void InitIndex() {
    auto writer = open_writer(irs::kOmCreate);

    std::vector<DocGeneratorBase::ptr> gens;
    gens.emplace_back(new tests::JsonDocGenerator(
      resource("simple_sequential.json"), &tests::GenericJsonFieldFactory));
    gens.emplace_back(new tests::JsonDocGenerator(
      resource("simple_sequential_common_prefix.json"),
      &tests::GenericJsonFieldFactory));
    add_segments(*writer, gens);
  }
};

TEST_P(ProxyFilterRealFilter, with_terms_filter) {
  InitIndex();
  auto rdr = open_reader();
  ProxyFilter proxy;
  auto [q, cache] = proxy.set_filter<ByTerm>(irs::IResourceManager::gNoop);
  *q.mutable_field() = "name";
  q.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view("A"));
  CheckQuery(proxy, Docs{1, 33}, rdr);
}

TEST_P(ProxyFilterRealFilter, with_disjunction_filter) {
  InitIndex();
  auto rdr = open_reader();
  ProxyFilter proxy;
  auto [root, cache] = proxy.set_filter<irs::Or>(irs::IResourceManager::gNoop);
  auto& q = root.add<ByTerm>();
  *q.mutable_field() = "name";
  q.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view("A"));
  auto& q1 = root.add<ByTerm>();
  *q1.mutable_field() = "name";
  q1.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view("B"));
  CheckQuery(proxy, Docs{1, 2, 33, 34}, rdr);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  proxy_filter_real_filter, ProxyFilterRealFilter,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5"},
                                       tests::FormatInfo{"1_5simd"})));
}  // namespace
