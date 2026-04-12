#include "index_tests.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "tests_shared.hpp"
#include "yaclib/runtime/fair_thread_pool.hpp"

namespace {

struct EmptyField : tests::Ifield {
  std::string_view Name() const final {
    EXPECT_FALSE(true);
    throw irs::NotImplError{};
  }

  irs::Tokenizer& GetTokens() const final {
    EXPECT_FALSE(true);
    throw irs::NotImplError{};
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    EXPECT_FALSE(true);
    throw irs::NotImplError{};
  }

  bool Write(irs::DataOutput&) const final { return false; }

  mutable irs::NullTokenizer stream;
};

auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

class SortedEuroparlDocTemplate : public tests::EuroparlDocTemplate {
 public:
  explicit SortedEuroparlDocTemplate(std::string field,
                                     irs::IndexFeatures features)
    : _field{std::move(field)}, _features{features} {}

  void init() final {
    indexed.push_back(
      std::make_shared<tests::StringField>("title", irs::kPosOffs | _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("title_anl", false, _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("title_anl_pay", true, _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("body_anl", false, _features));
    indexed.push_back(
      std::make_shared<text_ref_field>("body_anl_pay", true, _features));
    {
      insert(std::make_shared<tests::LongField>());
      auto& field = static_cast<tests::LongField&>(indexed.back());
      field.Name("date");
    }
    insert(std::make_shared<tests::StringField>("datestr",
                                                irs::kPosOffs | _features));
    insert(
      std::make_shared<tests::StringField>("body", irs::kPosOffs | _features));
    {
      insert(std::make_shared<tests::IntField>());
      auto& field = static_cast<tests::IntField&>(indexed.back());
      field.Name("id");
    }
    insert(
      std::make_shared<tests::StringField>("idstr", irs::kPosOffs | _features));

    auto fields = indexed.find(_field);

    if (!fields.empty()) {
      sorted = fields[0];
    }
  }

 private:
  std::string _field;  // sorting field
  irs::IndexFeatures _features;
};

class StringComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    const auto lhs_value = irs::ToString<irs::bytes_view>(lhs.data());
    const auto rhs_value = irs::ToString<irs::bytes_view>(rhs.data());

    return rhs_value.compare(lhs_value);
  }
};

class LongComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    auto* plhs = lhs.data();
    const auto lhs_value = sdb::ZigZagDecode64(irs::vread<uint64_t>(plhs));
    auto* prhs = rhs.data();
    const auto rhs_value = sdb::ZigZagDecode64(irs::vread<uint64_t>(prhs));

    if (lhs_value < rhs_value) {
      return -1;
    }

    if (rhs_value < lhs_value) {
      return 1;
    }

    return 0;
  }
};

struct CustomFeature {
  struct Header {
    explicit Header(std::span<const irs::bytes_view> headers) noexcept {
      for (const auto header : headers) {
        Update(header);
      }
    }

    void Write(irs::DataOutput& out) const {
      out.WriteU32(static_cast<uint32_t>(sizeof(count)));
      out.WriteU64(count);
    }

    void Update(irs::bytes_view in) {
      EXPECT_EQ(sizeof(count), in.size());
      auto* p = in.data();
      count += irs::read<decltype(count)>(p);
    }

    size_t count{0};
  };

  struct Writer : irs::FeatureWriter {
    explicit Writer(std::span<const irs::bytes_view> headers) noexcept
      : hdr{{}} {
      if (!headers.empty()) {
        init_header.emplace(headers);
      }
    }

    void write(const irs::FieldStats& stats, irs::doc_id_t doc,
               irs::ColumnOutput& writer) final {
      ++hdr.count;

      // We intentionally call `writer(doc)` multiple
      // times to check concatenation logic.
      writer(doc).WriteU32(stats.len);
      writer(doc).WriteU32(stats.max_term_freq);
      writer(doc).WriteU32(stats.num_overlap);
      writer(doc).WriteU32(stats.num_unique);
    }

    void write(irs::DataOutput& out, irs::bytes_view payload) final {
      if (!payload.empty()) {
        ++hdr.count;
        out.WriteBytes(payload.data(), payload.size());
      }
    }

    void finish(irs::DataOutput& out) final {
      if (init_header.has_value()) {
        // <= due to removals
        EXPECT_LE(hdr.count, init_header.value().count);
      }
      hdr.Write(out);
    }

    Header hdr;
    std::optional<Header> init_header;
    std::optional<size_t> expected_count;
  };

  static irs::FeatureWriter::ptr MakeWriter(
    std::span<const irs::bytes_view> payload) {
    return irs::memory::make_managed<Writer>(payload);
  }
};

class SortedIndexAsyncTestCase : public tests::IndexTestBase {
 protected:
  bool SupportsPluggableFeatures() const noexcept { return true; }

  irs::FeatureInfoProvider Features() {
    return [this](irs::IndexFeatures id) {
      if (SupportsPluggableFeatures()) {
        if (irs::IndexFeatures::Norm == id) {
          return std::make_pair(
            irs::ColumnInfo{
              irs::Type<irs::compression::None>::get(), {}, false},
            &irs::Norm::MakeWriter);
        }
      }

      return std::make_pair(
        irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
        irs::FeatureWriterFactory{});
    };
  }

  irs::IndexFeatures FieldFeatures() {
    return SupportsPluggableFeatures() ? irs::IndexFeatures::Norm
                                       : irs::IndexFeatures::None;
  }

  void assert_index(size_t skip = 0,
                    irs::automaton_table_matcher* matcher = nullptr) const {
    IndexTestBase::assert_index(irs::IndexFeatures::None, skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::None | irs::IndexFeatures::Freq, skip, matcher);
    IndexTestBase::assert_index(irs::IndexFeatures::None |
                                  irs::IndexFeatures::Freq |
                                  irs::IndexFeatures::Pos,
                                skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::None | irs::IndexFeatures::Freq |
        irs::IndexFeatures::Pos | irs::IndexFeatures::Offs,
      skip, matcher);
    IndexTestBase::assert_columnstore();
  }

  void CheckFeatureHeader(const irs::SubReader& segment, irs::field_id field_id,
                          irs::bytes_view header) {
    ASSERT_TRUE(SupportsPluggableFeatures());
    ASSERT_TRUE(irs::field_limits::valid(field_id));
    auto* column = segment.column(field_id);
    ASSERT_NE(nullptr, column);
    ASSERT_FALSE(irs::IsNull(column->payload()));
    ASSERT_EQ(header, column->payload());
  }

  void CheckFeatures(const irs::SubReader& segment, std::string_view field_name,
                     size_t count, bool after_consolidation) {
    auto* field_reader = segment.field(field_name);
    ASSERT_NE(nullptr, field_reader);
    auto& field = field_reader->meta();
    ASSERT_TRUE(irs::field_limits::valid(field.norm));

    // irs::Norm
    {
      irs::NormHeader hdr{after_consolidation ? irs::NormEncoding::Byte
                                              : irs::NormEncoding::Int};
      for (size_t i = 0; i < count; ++i) {
        hdr.Reset(1);
      }

      irs::bstring buf;
      irs::BytesOutput writer{buf};
      irs::NormHeader::Write(hdr, writer);
      buf = buf.substr(sizeof(uint32_t));  // skip size

      CheckFeatureHeader(segment, field.norm, buf);
    }
  }
};

struct SortedIndexAsyncStressTestCase : SortedIndexAsyncTestCase {};

TEST_P(SortedIndexAsyncStressTestCase, commit_on_tick) {
  yaclib::IntrusivePtr<yaclib::FairThreadPool> scheduler =
    yaclib::MakeFairThreadPool();

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, std::string_view name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });

  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<bool, kLen> in_store;
  std::array<char, kLen> results{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'};
  for (size_t commit = 0; commit < (1 << kLen); ++commit) {
    for (size_t reset = 0; reset < (1 << kLen); ++reset) {
      in_store.fill(false);
      uint64_t insert_time = irs::writer_limits::kMinTick + 1;
      uint64_t commit_time = insert_time;
      // open writer
      StringComparer compare;
      irs::IndexWriterOptions opts;
      opts.segment_docs_max = 2;
      opts.comparator = &compare;
      opts.features = Features();
      opts.executor = scheduler;
      auto writer = open_writer(irs::kOmCreate, opts);
      ASSERT_NE(nullptr, writer);
      ASSERT_EQ(&compare, writer->Comparator());
      {
        auto ctx = writer->GetBatch();
        for (size_t i = 0; i < kLen; ++i) {
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::StoreSorted>(
              *insert_docs[i].second->sorted));
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(
              insert_docs[i].second->indexed.begin(),
              insert_docs[i].second->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(
              insert_docs[i].second->stored.begin(),
              insert_docs[i].second->stored.end()));
          }
          if (((reset >> i) & 1U) == 1U) {
            ctx.Abort();
          } else {
            ctx.Commit(++insert_time);
            in_store[insert_docs[i].first] = true;
          }
          if (((commit >> i) & 1U) == 1U) {
            writer->Commit({.tick = commit_time});
            commit_time = insert_time;
            AssertSnapshotEquality(*writer);
          }
        }
      }
      size_t in_store_count = 0;
      for (auto v : in_store) {
        in_store_count += static_cast<size_t>(v);
      }
      writer->Commit({.tick = insert_time});
      AssertSnapshotEquality(*writer);

      auto reader = writer->GetSnapshot();
      ASSERT_TRUE(reader);
      EXPECT_EQ(in_store_count, reader->live_docs_count());
      EXPECT_LE(in_store_count, reader->docs_count());
      EXPECT_LE(reader->docs_count(), kLen);

      writer->Consolidate(MakePolicy(irs::index_utils::ConsolidateCount{}));
      writer->Commit({.tick = insert_time});
      AssertSnapshotEquality(*writer);

      writer = nullptr;

      // Check consolidated segment
      reader = irs::DirectoryReader(dir(), codec());
      ASSERT_TRUE(reader);
      if (in_store_count == 0) {
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader->docs_count());
        ASSERT_EQ(0, reader->live_docs_count());
        continue;
      }
      ASSERT_EQ(1, reader.size());
      EXPECT_EQ(in_store_count, reader->docs_count());
      EXPECT_EQ(in_store_count, reader->live_docs_count());
      const auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      const auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      const auto* terms = segment.field("name");
      ASSERT_NE(nullptr, terms);
      auto docs = segment.docs_iterator();
      for (size_t i = kLen; i > 0; --i) {
        if (!in_store[i - 1]) {
          continue;
        }
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(docs->value(), values->seek(docs->value()));
        EXPECT_EQ(results[i - 1], irs::ToString<std::string_view>(
                                    actual_value->value.data())[0]);
      }
      ASSERT_FALSE(docs->next());
    }
  }

  scheduler->HardStop();
  scheduler->Wait();
}

TEST_P(SortedIndexAsyncStressTestCase, split_empty_commit) {
  yaclib::IntrusivePtr<yaclib::FairThreadPool> scheduler =
    yaclib::MakeFairThreadPool();

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, std::string_view name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });
  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::ByTerm>>, kLen> remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::StringField&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();
  opts.executor = scheduler;
  auto writer = open_writer(irs::kOmCreate, opts);
  auto segment1 = writer->GetBatch();
  auto insert_doc = [&](size_t i) {
    auto doc = segment1.Insert();
    ASSERT_TRUE(
      doc.Insert<irs::Action::StoreSorted>(*insert_docs[i].second->sorted));
    ASSERT_TRUE(
      doc.Insert<irs::Action::INDEX>(insert_docs[i].second->indexed.begin(),
                                     insert_docs[i].second->indexed.end()));
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE>(insert_docs[i].second->stored.begin(),
                                     insert_docs[i].second->stored.end()));
  };
  auto remove_doc = [&](size_t i) { segment1.Remove(*remove_docs[i].second); };
  insert_doc(0);
  insert_doc(1);
  insert_doc(2);
  remove_doc(0);
  segment1.Commit(10);
  insert_doc(3);
  remove_doc(1);
  remove_doc(2);
  remove_doc(3);
  segment1.Commit(20);
  writer->Commit({.tick = 10});
  auto reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 4);
  EXPECT_EQ(reader->live_docs_count(), 2);
  EXPECT_EQ(reader->size(), 1);
  writer->Commit({.tick = 20});
  reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 0);
  EXPECT_EQ(reader->live_docs_count(), 0);
  EXPECT_EQ(reader->size(), 0);

  scheduler->HardStop();
  scheduler->Wait();
}

TEST_P(SortedIndexAsyncStressTestCase, remove_tick) {
  yaclib::IntrusivePtr<yaclib::FairThreadPool> scheduler =
    yaclib::MakeFairThreadPool();

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, std::string_view name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });
  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::Document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::ByTerm>>, kLen> remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::StringField&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = Features();
  opts.executor = scheduler;
  auto writer = open_writer(irs::kOmCreate, opts);
  auto segment1 = writer->GetBatch();
  auto insert_doc = [&](size_t i) {
    auto doc = segment1.Insert();
    ASSERT_TRUE(
      doc.Insert<irs::Action::StoreSorted>(*insert_docs[i].second->sorted));
    ASSERT_TRUE(
      doc.Insert<irs::Action::INDEX>(insert_docs[i].second->indexed.begin(),
                                     insert_docs[i].second->indexed.end()));
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE>(insert_docs[i].second->stored.begin(),
                                     insert_docs[i].second->stored.end()));
  };
  auto remove_doc = [&](size_t i) { segment1.Remove(*remove_docs[i].second); };
  insert_doc(0);
  insert_doc(1);
  insert_doc(2);
  remove_doc(0);
  segment1.Commit(10);
  insert_doc(3);
  insert_doc(4);
  remove_doc(4);
  segment1.Commit(20);
  writer->Commit({.tick = 10});
  auto reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 5);
  EXPECT_EQ(reader->live_docs_count(), 2);
  EXPECT_EQ(reader->size(), 1);
  writer->Commit({.tick = 20});
  reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 5);
  EXPECT_EQ(reader->live_docs_count(), 3);
  EXPECT_EQ(reader->size(), 1);

  scheduler->HardStop();
  scheduler->Wait();
}

INSTANTIATE_TEST_SUITE_P(
  SortedIndexAsyncStressTest, SortedIndexAsyncStressTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MemoryDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5simd"})),
  SortedIndexAsyncStressTestCase::to_string);

}  // namespace
