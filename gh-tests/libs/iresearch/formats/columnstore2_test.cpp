////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/formats/columnstore2.hpp>
#include <iresearch/search/score.hpp>
#include <vector>

#include "tests_param.hpp"
#include "tests_shared.hpp"

using namespace irs::columnstore2;

class Columnstore2TestCase
  : public virtual tests::DirectoryTestCaseBase<
      irs::ColumnHint, irs::columnstore2::Version, bool> {
 public:
  void TearDown() final {
    _dir.reset();

    auto& memory = GetResourceManager();
    ASSERT_EQ(memory.readers.Counter(), 0);
    ASSERT_EQ(memory.consolidations.Counter(), 0);
    ASSERT_EQ(memory.file_descriptors.Counter(), 0);
    ASSERT_EQ(memory.transactions.Counter(), 0);
  }

  static std::string to_string(const testing::TestParamInfo<ParamType>& info) {
    auto [factory, hint, version, buffered] = info.param;

    std::string name = (*factory)(nullptr).second;

    switch (hint) {
      case irs::ColumnHint::Normal:
        break;
      case irs::ColumnHint::Consolidation:
        name += "_consolidation";
        break;
      case irs::ColumnHint::Mask:
        name += "_mask";
        break;
      case irs::ColumnHint::PrevDoc:
        name += "_prev";
        break;
      default:
        EXPECT_FALSE(true);
        break;
    }

    if (buffered) {
      name += "_buffered";
    }

    return name + "_" + std::to_string(static_cast<uint32_t>(version));
  }

  bool Buffered() const noexcept {
    auto& p = this->GetParam();
    return std::get<3>(p);
  }

  bool Consolidation() const noexcept {
    return Hint() == irs::ColumnHint::Consolidation;
  }

  irs::ColumnstoreReader::Options ReaderOptions() {
    irs::ColumnstoreReader::Options options;
    options.warmup_column = [this](const irs::ColumnReader&) {
      return this->Buffered();
    };
    return options;
  }

  irs::ColumnInfo ColumnInfo() const noexcept {
    return {.compression = irs::Type<irs::compression::None>::get(),
            .options = {},
            .encryption = HasEncryption(),
            .track_prev_doc = HasPrevDoc()};
  }

  bool HasEncryption() const noexcept {
    return nullptr != dir().attributes().encryption();
  }

  bool HasPayload() const noexcept {
    return irs::ColumnHint::Normal == (Hint() & irs::ColumnHint::Mask);
  }

  bool HasPrevDoc() const noexcept {
    return irs::ColumnHint::PrevDoc == (Hint() & irs::ColumnHint::PrevDoc);
  }

  ColumnProperty ColumnProperty(ColumnProperty base_props) const noexcept {
    if (HasEncryption()) {
      base_props |= ColumnProperty::Encrypt;
    }
    if (HasPrevDoc()) {
      base_props |= ColumnProperty::PrevDoc;
    }
    return base_props;
  }

  irs::columnstore2::Version Version() const noexcept {
    auto& p = this->GetParam();
    return std::get<irs::columnstore2::Version>(p);
  }

  irs::ColumnHint Hint() const noexcept {
    auto& p = this->GetParam();
    return std::get<irs::ColumnHint>(p);
  }

  void AssertPrevDoc(irs::DocIterator& it, irs::DocIterator& prev_it) {
    auto prev_doc = [](irs::DocIterator& it, irs::doc_id_t target) {
      auto doc = it.value();
      auto prev = 0;
      while (doc < target && it.next()) {
        prev = doc;
        doc = it.value();
      }
      return prev;
    };

    auto* prev = irs::get<irs::PrevDocAttr>(it);
    ASSERT_EQ(HasPrevDoc(), nullptr != prev && nullptr != *prev);
    if (prev && *prev) {
      ASSERT_EQ(prev_doc(prev_it, it.value()), (*prev)());
    }
  }

  irs::ColumnFinalizer DefaultFinalizer(uint8_t num, std::string_view name) {
    return irs::ColumnFinalizer{
      [num](irs::DataOutput& out) {
        out.WriteU32(static_cast<uint32_t>(sizeof(num)));
        out.WriteByte(num);
      },
      [name] { return name; },
    };
  }

  irs::ColumnFinalizer NonCalledFinalizer() {
    return irs::ColumnFinalizer{
      [](irs::DataOutput&) { EXPECT_TRUE(false); },
      [] { return std::string_view{}; },
    };
  }
};

TEST_P(Columnstore2TestCase, reader_ctor) {
  irs::columnstore2::Reader reader;
  ASSERT_EQ(0, reader.size());
  ASSERT_EQ(nullptr, reader.column(0));
  ASSERT_EQ(nullptr, reader.header(0));
}

TEST_P(Columnstore2TestCase, empty_columnstore) {
  constexpr irs::doc_id_t kMax = 1;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  auto& memory = GetResourceManager();

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     *memory.options.transactions);
    writer.prepare(dir(), meta);
    const auto pinned = memory.transactions.Counter();
    ASSERT_GT(pinned, 0);
    writer.push_column({irs::Type<irs::compression::None>::get(), {}, false},
                       NonCalledFinalizer());
    writer.push_column({irs::Type<irs::compression::None>::get(), {}, false},
                       NonCalledFinalizer());
    const auto pinned2 = memory.transactions.Counter();

    ASSERT_GT(pinned2, pinned);
    ASSERT_FALSE(writer.commit(state));
    const auto pinned3 = memory.transactions.Counter();
    // we do not release columns memory from vector.
    // but that maybe makes no sense as in real application
    // writer would be terminated anyway.
    ASSERT_LE(pinned3, pinned2);
    auto last_readers_counter = memory.readers.Counter();
    irs::columnstore2::Reader reader;
    ASSERT_FALSE(reader.prepare(dir(), meta, ReaderOptions()));
    // empty columnstore - no readers to allocate
    ASSERT_EQ(memory.readers.Counter(), last_readers_counter);
  }
}

TEST_P(Columnstore2TestCase, empty_column) {
  constexpr irs::doc_id_t kMax = 1;
  constexpr std::string_view kTestName = "foobar";
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };
  auto& memory = GetResourceManager();
  ASSERT_EQ(memory.readers.Counter(), 0);
  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     *memory.options.transactions);
    writer.prepare(dir(), meta);
    [[maybe_unused]] auto [id0, handle0] =
      writer.push_column(ColumnInfo(), DefaultFinalizer(1, kTestName));
    [[maybe_unused]] auto [id1, handle1] =
      writer.push_column(ColumnInfo(), DefaultFinalizer(2, std::string_view{}));
    [[maybe_unused]] auto [id2, handle2] =
      writer.push_column(ColumnInfo(), NonCalledFinalizer());
    handle1(42).WriteByte(42);
    const auto pinned = memory.transactions.Counter();
    ASSERT_GT(pinned, 0);
    ASSERT_TRUE(writer.commit(state));
  }
  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(2, reader.size());
    ASSERT_GT(memory.readers.Counter(), 0);
    // column 0
    {
      auto* header = reader.header(0);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(0, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::invalid(), header->min);
      ASSERT_EQ(ColumnType::Mask, header->type);
      ASSERT_EQ(ColumnProperty(ColumnProperty::Normal), header->props);

      auto column = reader.column(0);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(0, column->id());
      ASSERT_EQ("foobar", column->name());
      ASSERT_EQ(0, column->size());
      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(1, header_payload[0]);
      auto it = column->iterator(Hint());
      ASSERT_NE(nullptr, it);
      ASSERT_EQ(0, irs::CostAttr::extract(*it));
      ASSERT_TRUE(irs::doc_limits::eof(it->value()));
    }

    // column 1
    {
      auto* header = reader.header(1);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(1, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(42, header->min);
      ASSERT_EQ(ColumnType::Sparse, header->type);  // FIXME why sparse?
      ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

      auto column = reader.column(1);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(1, column->id());
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(1, column->size());
      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(2, header_payload[0]);
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      auto* prev = irs::get<irs::PrevDocAttr>(*it);
      ASSERT_EQ(HasPrevDoc(), prev && *prev);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_NE(nullptr, it);
      ASSERT_FALSE(irs::doc_limits::valid(it->value()));
      ASSERT_TRUE(it->next());
      if (prev && *prev) {
        ASSERT_EQ(0, (*prev)());
      }
      ASSERT_EQ(42, it->value());
      if (HasPayload()) {
        ASSERT_EQ(1, payload->value.size());
        ASSERT_EQ(42, payload->value[0]);
      } else {
        ASSERT_TRUE(payload->value.empty());
      }
      ASSERT_FALSE(it->next());
      ASSERT_FALSE(it->next());
    }
  }
}

TEST_P(Columnstore2TestCase, sparse_mask_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };
  auto& memory = GetResourceManager();
  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     *memory.options.transactions);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));
    ASSERT_GT(memory.transactions.Counter(), 0);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      column(doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }
  ASSERT_EQ(memory.transactions.Counter(), 0);
  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());
    ASSERT_GT(memory.readers.Counter(), 0);
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax / 2, header->docs_count);
    ASSERT_NE(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Mask, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax / 2, column->size());
    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(irs::IsNull(column->name()));

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    // seek stateful
    {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
      }
    }

    // seek stateless
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      auto it = column->iterator(Hint());
      auto* prev = irs::get<irs::PrevDocAttr>(*it);
      ASSERT_EQ(HasPrevDoc(), prev && *prev);
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 5000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc - 1));

      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      auto* prev = irs::get<irs::PrevDocAttr>(*next_it);
      ASSERT_EQ(HasPrevDoc(), nullptr != prev && nullptr != *prev);
      ASSERT_EQ(doc, next_it->seek(doc));
      for (auto next_doc = doc + 2; next_doc <= kMax; next_doc += 2) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_EQ(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_TRUE(it->next());
      AssertPrevDoc(*it, *prev_it);
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      ASSERT_EQ(118775, it->seek(118774));
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(Columnstore2TestCase, sparse_column_m) {
  constexpr irs::doc_id_t kMax = 5000;
  constexpr std::string_view kTestName = "foobaz";
  irs::SegmentMeta meta;
  meta.name = "test_m";
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };
  auto& mem = GetResourceManager();
  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     mem.transactions);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      {irs::Type<irs::compression::None>::get(), {}, has_encryption},
      DefaultFinalizer(42, kTestName));
    ASSERT_GT(mem.transactions.Counter(), 0);
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                        str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }
  ASSERT_EQ(0, mem.transactions.Counter());
  {
    irs::columnstore2::Reader reader;
    auto options = ReaderOptions();
    ASSERT_TRUE(reader.prepare(dir(), meta, options));
    ASSERT_EQ(1, reader.size());
    if (this->Buffered()) {
      ASSERT_GT(mem.cached_columns.Counter(), 0);
    } else {
      ASSERT_EQ(0, mem.cached_columns.Counter());
    }
    ASSERT_GT(mem.readers.Counter(), 0);
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax / 2, header->docs_count);
    ASSERT_NE(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(has_encryption ? ColumnProperty::Encrypt : ColumnProperty::Normal,
              header->props);
  }
}

TEST_P(Columnstore2TestCase, sparse_column_mr) {
  constexpr irs::doc_id_t kMax = 5000;
  constexpr std::string_view kTestName = "foobaz";
  irs::SegmentMeta meta;
  meta.name = "test";
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  auto& memory = GetResourceManager();
  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     memory.transactions);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      {irs::Type<irs::compression::None>::get(), {}, has_encryption},
      DefaultFinalizer(42, kTestName));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                        str.size());
    }
    ASSERT_GT(memory.transactions.Counter(), 0);
    ASSERT_TRUE(writer.commit(state));
  }
  ASSERT_EQ(memory.transactions.Counter(), 0);
  memory.cached_columns.result = false;

  {
    irs::columnstore2::Reader reader;
    auto options = ReaderOptions();
    ASSERT_TRUE(reader.prepare(dir(), meta, options));
    ASSERT_EQ(1, reader.size());
    if (this->Buffered()) {
      // we still record an attempt of allocating
      ASSERT_GT(memory.cached_columns.Counter(), 0);
    } else {
      ASSERT_EQ(0, memory.cached_columns.Counter());
    }
    ASSERT_GT(memory.readers.Counter(), 0);
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax / 2, header->docs_count);
    ASSERT_NE(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(has_encryption ? ColumnProperty::Encrypt : ColumnProperty::Normal,
              header->props);
  }
  // should not be a deallocation because SimpleMemoryAccounter::Increase
  // throws exception AFTER counting
  if (this->Buffered()) {
    ASSERT_GT(memory.cached_columns.Counter(), 0);
  } else {
    ASSERT_EQ(0, memory.cached_columns.Counter());
  }
}

TEST_P(Columnstore2TestCase, SparseColumn) {
  constexpr irs::doc_id_t kMax = 1000000;
  constexpr std::string_view kTestName = "foobaz";
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] =
      writer.push_column(ColumnInfo(), DefaultFinalizer(42, kTestName));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.data()),
                        str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }
  TestResourceManager memory;
  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax / 2, header->docs_count);
    ASSERT_NE(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::Normal), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax / 2, column->size());
    ASSERT_EQ(0, column->id());
    ASSERT_EQ(kTestName, column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_iterator = [&](irs::ColumnHint hint) {
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        const irs::PayAttr* payload = nullptr;
        if (hint != irs::ColumnHint::Mask) {
          payload = irs::get<irs::PayAttr>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::PayAttr>(*it));
        }
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
             doc += 2) {
          SCOPED_TRACE(doc);
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          const auto str = std::to_string(doc);
          if (payload) {
            ASSERT_EQ(str, irs::ViewCast<char>(payload->value));
          }
          AssertPrevDoc(*it, *prev_it);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 2) {
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        const irs::PayAttr* payload = nullptr;
        if (hint != irs::ColumnHint::Mask) {
          payload = irs::get<irs::PayAttr>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::PayAttr>(*it));
        }
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc - 1));
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 5000) {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        const irs::PayAttr* payload = nullptr;
        if (hint != irs::ColumnHint::Mask) {
          payload = irs::get<irs::PayAttr>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::PayAttr>(*it));
        }
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc - 1));
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        AssertPrevDoc(*it, *prev_it);

        auto next_it = column->iterator(hint);
        ASSERT_NE(nullptr, next_it);
        const irs::PayAttr* next_payload = nullptr;
        if (hint != irs::ColumnHint::Mask) {
          next_payload = irs::get<irs::PayAttr>(*next_it);
          ASSERT_NE(nullptr, next_payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::PayAttr>(*next_it));
        }
        ASSERT_EQ(doc, next_it->seek(doc));
        if (next_payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(next_payload->value));
        }
        for (auto next_doc = doc + 2; next_doc <= kMax; next_doc += 2) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          const auto str = std::to_string(next_doc);
          if (next_payload) {
            EXPECT_EQ(str, irs::ViewCast<char>(next_payload->value));
          }
          AssertPrevDoc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        const irs::PayAttr* payload = nullptr;
        if (hint != irs::ColumnHint::Mask) {
          payload = irs::get<irs::PayAttr>(*it);
          ASSERT_NE(nullptr, payload);
        } else {
          ASSERT_EQ(nullptr, irs::get<irs::PayAttr>(*it));
        }
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_EQ(118775, it->seek(118774));
        const auto str = std::to_string(it->value());
        if (payload) {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        AssertPrevDoc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    };

    assert_iterator(Hint());
  }
}

TEST_P(Columnstore2TestCase, sparse_column_gap) {
  static constexpr irs::doc_id_t kMax = 500000;
  static constexpr auto kBlockSize = irs::SparseBitmapWriter::kBlockSize;
  static constexpr auto kGapBegin = ((kMax / kBlockSize) - 4) * kBlockSize;
  constexpr std::string_view kTestName = "foobarbaz";
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };
  TestResourceManager memory;
  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] =
      writer.push_column(ColumnInfo(), DefaultFinalizer(42, kTestName));

    auto write_payload = [](irs::doc_id_t doc, irs::DataOutput& stream) {
      if (doc <= kGapBegin || doc > (kGapBegin + kBlockSize)) {
        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                          sizeof doc);
      }
    };

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    auto assert_payload = [this](irs::doc_id_t doc, irs::bytes_view payload) {
      SCOPED_TRACE(doc);
      if (HasPayload() &&
          (doc <= kGapBegin || doc > (kGapBegin + kBlockSize))) {
        irs::doc_id_t actual_doc[1];
        ASSERT_EQ(sizeof actual_doc, payload.size());
        memcpy(&actual_doc, payload.data(), sizeof actual_doc);
        EXPECT_EQ(doc, actual_doc[0]);
      } else {
        ASSERT_TRUE(payload.empty());
      }
    };

    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::Normal), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_EQ(kTestName, column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        SCOPED_TRACE(doc);
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      const auto str = std::to_string(doc);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; doc += 5000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      const auto str = std::to_string(doc);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      auto* next_payload = irs::get<irs::PayAttr>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        assert_payload(next_doc, next_payload->value);
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_EQ(118775, it->seek(118775));
      assert_payload(118775, payload->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(Columnstore2TestCase, sparse_column_tail_block) {
  static constexpr irs::doc_id_t kMax = 500000;
  static constexpr auto kBlockSize = irs::SparseBitmapWriter::kBlockSize;
  static constexpr auto kTailBegin = (kMax / kBlockSize) * kBlockSize;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    auto write_payload = [](irs::doc_id_t doc, irs::DataOutput& stream) {
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                        sizeof doc);
      if (doc > kTailBegin) {
        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                          sizeof doc);
      }
    };

    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }
  TestResourceManager memory;
  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(irs::IsNull(column->name()));

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [this](irs::doc_id_t doc, irs::bytes_view payload) {
      SCOPED_TRACE(doc);

      if (!HasPayload()) {
        ASSERT_TRUE(payload.empty());
        return;
      }

      ASSERT_FALSE(payload.empty());
      if (doc > kTailBegin) {
        irs::doc_id_t actual_doc[2];
        ASSERT_EQ(sizeof actual_doc, payload.size());
        memcpy(&actual_doc, payload.data(), sizeof actual_doc);
        EXPECT_EQ(doc, actual_doc[0]);
        EXPECT_EQ(doc, actual_doc[1]);
      } else {
        irs::doc_id_t actual_doc[1];
        ASSERT_EQ(sizeof actual_doc, payload.size());
        memcpy(&actual_doc, payload.data(), sizeof actual_doc);
        EXPECT_EQ(doc, actual_doc[0]);
      }
    };

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      auto* next_payload = irs::get<irs::PayAttr>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        assert_payload(next_doc, next_payload->value);
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      constexpr irs::doc_id_t kDoc = 118774;

      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      ASSERT_EQ(kDoc, it->seek(kDoc));
      assert_payload(kDoc, payload->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(Columnstore2TestCase, sparse_column_tail_block_last_value) {
  static constexpr irs::doc_id_t kMax = 500000;
  // last value has different length
  static constexpr auto kTailBegin = kMax - 1;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    auto write_payload = [](irs::doc_id_t doc, irs::DataOutput& stream) {
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                        sizeof doc);
      if (doc > kTailBegin) {
        stream.WriteByte(42);
      }
    };
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    TestResourceManager memory;
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(irs::IsNull(column->name()));

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [this](irs::doc_id_t doc, irs::bytes_view payload) {
      SCOPED_TRACE(doc);

      if (!HasPayload()) {
        ASSERT_TRUE(payload.empty());
        return;
      }

      ASSERT_FALSE(payload.empty());
      if (doc > kTailBegin) {
        irs::doc_id_t actual_doc[1];
        ASSERT_EQ(1 + sizeof actual_doc, payload.size());
        memcpy(&actual_doc, payload.data(), sizeof actual_doc);
        EXPECT_EQ(doc, actual_doc[0]);
        EXPECT_EQ(42, payload[sizeof doc]);
      } else {
        irs::doc_id_t actual_doc[1];
        ASSERT_EQ(sizeof actual_doc, payload.size());
        memcpy(&actual_doc, payload.data(), sizeof actual_doc);
        EXPECT_EQ(doc, actual_doc[0]);
      }
    };

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      auto* next_payload = irs::get<irs::PayAttr>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        assert_payload(next_doc, next_payload->value);
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      constexpr irs::doc_id_t kDoc = 118774;

      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_EQ(kDoc, it->seek(kDoc));
      assert_payload(kDoc, payload->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(Columnstore2TestCase, sparse_column_full_blocks) {
  // Exactly 2 blocks
  static constexpr irs::doc_id_t kMax = 131072;
  // last value has different length
  static constexpr auto kTailBegin = kMax - 3;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  constexpr std::string_view kValue{"aaaaaaaaaaaaaaaaaaaaaaaaaaa"};
  static_assert(kValue.size() == 27);

  {
    auto write_payload = [value = kValue](irs::doc_id_t doc,
                                          irs::DataOutput& stream) {
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(value.data()),
                        value.size());
      if (doc <= kTailBegin) {
        stream.WriteByte(value.front());
      }
    };

    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(irs::IsNull(column->name()));

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [this, value = kValue](irs::doc_id_t doc,
                                                 irs::bytes_view payload) {
      SCOPED_TRACE(doc);

      if (!HasPayload()) {
        ASSERT_TRUE(payload.empty());
        return;
      }

      ASSERT_FALSE(payload.empty());
      if (doc <= kTailBegin) {
        ASSERT_EQ(1 + value.size(), payload.size());
        std::string expected{value.data(), value.size()};
        expected += value.front();
        ASSERT_EQ(expected, irs::ViewCast<char>(payload));
      } else {
        ASSERT_EQ(value.size(), payload.size());
        ASSERT_EQ(value, irs::ViewCast<char>(payload));
      }
    };

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      auto* next_payload = irs::get<irs::PayAttr>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        assert_payload(next_doc, next_payload->value);
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      constexpr irs::doc_id_t kDoc = 118774;

      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_EQ(kDoc, it->seek(kDoc));
      assert_payload(kDoc, payload->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(Columnstore2TestCase, sparse_column_full_blocks_all_equal) {
  // Exactly 2 blocks
  static constexpr irs::doc_id_t kMax = 131072;
  // last value has different length
  static constexpr auto kTailBegin = kMax - 1;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  constexpr std::string_view kValue{"aaaaaaaaaaaaaaaaaaaaaaaaaaa"};
  static_assert(kValue.size() == 27);

  {
    auto write_payload = [value = kValue](irs::doc_id_t doc,
                                          irs::DataOutput& stream) {
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(value.data()),
                        value.size());
      if (doc <= kTailBegin) {
        stream.WriteByte(value.front());
      }
    };

    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      write_payload(doc, column(doc));
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(irs::IsNull(column->name()));

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [this, value = kValue](irs::doc_id_t doc,
                                                 irs::bytes_view payload) {
      SCOPED_TRACE(doc);

      if (!HasPayload()) {
        ASSERT_TRUE(payload.empty());
        return;
      }

      ASSERT_FALSE(payload.empty());
      if (doc <= kTailBegin) {
        ASSERT_EQ(1 + value.size(), payload.size());
        std::string expected{value.data(), value.size()};
        expected += value.front();
        ASSERT_EQ(expected, irs::ViewCast<char>(payload));
      } else {
        ASSERT_EQ(value.size(), payload.size());
        ASSERT_EQ(value, irs::ViewCast<char>(payload));
      }
    };

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, payload->value);
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc));
      assert_payload(doc, payload->value);
      ASSERT_EQ(doc, it->seek(doc - 1));
      assert_payload(doc, payload->value);
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      auto* next_payload = irs::get<irs::PayAttr>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      ASSERT_EQ(doc, next_it->seek(doc));
      assert_payload(doc, next_payload->value);
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        assert_payload(next_doc, next_payload->value);
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      constexpr irs::doc_id_t kDoc = 118774;

      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_EQ(kDoc, it->seek(kDoc));
      assert_payload(kDoc, payload->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
    }
  }
}

TEST_P(Columnstore2TestCase, dense_mask_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  constexpr std::string_view kTestName = "foobar";
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] =
      writer.push_column(ColumnInfo(), DefaultFinalizer(42, kTestName));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      column(doc);
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Mask, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::Normal), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_EQ(kTestName, column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      ASSERT_TRUE(irs::IsNull(payload->value));
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_TRUE(irs::IsNull(payload->value));
        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_TRUE(irs::IsNull(payload->value));
        ASSERT_EQ(doc, it->seek(doc - 1));
        ASSERT_TRUE(irs::IsNull(payload->value));
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      const auto hint = doc % 2 ? this->Hint() : irs::ColumnHint::Mask;
      auto it = column->iterator(hint);
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      ASSERT_TRUE(irs::IsNull(payload->value));
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_TRUE(irs::IsNull(payload->value));
      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_TRUE(irs::IsNull(payload->value));
      ASSERT_EQ(doc, it->seek(doc - 1));
      ASSERT_TRUE(irs::IsNull(payload->value));
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(doc, it->seek(doc));
      ASSERT_EQ(doc, it->seek(doc));
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      ASSERT_EQ(doc, next_it->seek(doc));
      for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        AssertPrevDoc(*next_it, *prev_it);
      }
    }

    // next + seek
    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
      ASSERT_TRUE(it->next());
      ASSERT_EQ(irs::doc_limits::min(), document->value);
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::IsNull(payload->value));
      ASSERT_EQ(118774, it->seek(118774));
      AssertPrevDoc(*it, *prev_it);
      ASSERT_TRUE(irs::IsNull(payload->value));
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      ASSERT_TRUE(irs::doc_limits::eof(it->seek(irs::doc_limits::eof())));
    }
  }
}

TEST_P(Columnstore2TestCase, dense_column) {
  constexpr irs::doc_id_t kMax = 1000000;
  constexpr std::string_view kTestName = "foobar";
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] =
      writer.push_column(ColumnInfo(), DefaultFinalizer(42, kTestName));

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                        str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::Normal), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_EQ(kTestName, column->name());

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_iterator = [&](irs::ColumnHint hint) {
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          AssertPrevDoc(*it, *prev_it);
          const auto str = std::to_string(doc);
          if (hint == irs::ColumnHint::Mask) {
            EXPECT_TRUE(irs::IsNull(payload->value));
          } else {
            EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
          }
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc - 1));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        const auto str = std::to_string(doc);
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        ASSERT_EQ(doc, it->seek(doc));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        AssertPrevDoc(*it, *prev_it);

        auto next_it = column->iterator(hint);
        auto* next_payload = irs::get<irs::PayAttr>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(next_payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(next_payload->value));
        }
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          const auto str = std::to_string(next_doc);
          if (hint == irs::ColumnHint::Mask) {
            EXPECT_TRUE(irs::IsNull(next_payload->value));
          } else {
            EXPECT_EQ(str, irs::ViewCast<char>(next_payload->value));
          }
          AssertPrevDoc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(hint);
        auto it = column->iterator(hint);
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        AssertPrevDoc(*it, *prev_it);
        const auto str = std::to_string(118774);
        ASSERT_EQ(118774, it->seek(118774));
        if (hint == irs::ColumnHint::Mask) {
          EXPECT_TRUE(irs::IsNull(payload->value));
        } else {
          EXPECT_EQ(str, irs::ViewCast<char>(payload->value));
        }
        AssertPrevDoc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    };

    assert_iterator(Hint());
  }
}

TEST_P(Columnstore2TestCase, dense_column_range) {
  constexpr irs::doc_id_t kMin = 500000;
  constexpr irs::doc_id_t kMax = 1000000;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

    for (irs::doc_id_t doc = kMin; doc <= kMax; ++doc) {
      auto& stream = column(doc);
      const auto str = std::to_string(doc);
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                        str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax - kMin + 1, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(kMin, header->min);
    ASSERT_EQ(ColumnType::Sparse, header->type);
    ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

    auto* column = reader.column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(kMax - kMin + 1, column->size());

    ASSERT_EQ(0, column->id());
    ASSERT_TRUE(irs::IsNull(column->name()));

    const auto header_payload = column->payload();
    ASSERT_EQ(1, header_payload.size());
    ASSERT_EQ(42, header_payload[0]);

    auto assert_payload = [this](std::string_view str,
                                 const irs::PayAttr& payload) {
      if (HasPayload()) {
        EXPECT_EQ(str, irs::ViewCast<char>(payload.value));
      } else {
        ASSERT_TRUE(payload.value.empty());
      }
    };

    // seek before range
    {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      ASSERT_EQ(kMin, it->seek(42));
      assert_payload(std::to_string(kMin), *payload);

      irs::doc_id_t expected_doc = kMin + 1;
      for (; expected_doc <= kMax; ++expected_doc) {
        const auto str = std::to_string(expected_doc);
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
      }
      ASSERT_FALSE(it->next());
      ASSERT_TRUE(irs::doc_limits::eof(it->value()));
    }

    {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        const auto expected_doc = (doc <= kMin ? kMin : doc);
        const auto str = std::to_string(expected_doc);
        ASSERT_EQ(expected_doc, it->seek(doc));
        assert_payload(str, *payload);
        ASSERT_EQ(expected_doc, it->seek(doc));
        assert_payload(str, *payload);
        ASSERT_EQ(expected_doc, it->seek(doc - 1));
        assert_payload(str, *payload);
        AssertPrevDoc(*it, *prev_it);
      }
    }

    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      const auto expected_doc = (doc <= kMin ? kMin : doc);
      const auto str = std::to_string(expected_doc);
      ASSERT_EQ(expected_doc, it->seek(doc));
      assert_payload(str, *payload);
      ASSERT_EQ(expected_doc, it->seek(doc));
      assert_payload(str, *payload);
    }

    // seek + next
    for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
         doc += 10000) {
      auto prev_it = column->iterator(Hint());
      auto it = column->iterator(Hint());
      auto* document = irs::get<irs::DocAttr>(*it);
      ASSERT_NE(nullptr, document);
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);
      auto* cost = irs::get<irs::CostAttr>(*it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(column->size(), cost->estimate());
      auto* score = irs::get<irs::ScoreAttr>(*it);
      ASSERT_NE(nullptr, score);
      ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

      const auto expected_doc = (doc <= kMin ? kMin : doc);
      const auto str = std::to_string(expected_doc);
      ASSERT_EQ(expected_doc, it->seek(doc));
      assert_payload(str, *payload);
      ASSERT_EQ(expected_doc, it->seek(doc));
      assert_payload(str, *payload);
      AssertPrevDoc(*it, *prev_it);

      auto next_it = column->iterator(Hint());
      ASSERT_EQ(expected_doc, next_it->seek(doc));
      auto* next_payload = irs::get<irs::PayAttr>(*next_it);
      ASSERT_NE(nullptr, next_payload);
      assert_payload(str, *next_payload);
      for (auto next_doc = expected_doc + 1; next_doc <= kMax; ++next_doc) {
        ASSERT_TRUE(next_it->next());
        ASSERT_EQ(next_doc, next_it->value());
        const auto str = std::to_string(next_doc);
        assert_payload(str, *next_payload);
        AssertPrevDoc(*next_it, *prev_it);
      }
    }
  }
}

TEST_P(Columnstore2TestCase, dense_fixed_length_column_m) {
  constexpr irs::doc_id_t kMax = 5000;
  irs::SegmentMeta meta;
  meta.name = "test_m";
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };
  auto& mem = GetResourceManager();
  ASSERT_EQ(0, mem.readers.Counter());

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     mem.transactions);
    writer.prepare(dir(), meta);

    {
      auto [id, column] = writer.push_column(
        {irs::Type<irs::compression::None>::get(), {}, has_encryption},
        DefaultFinalizer(42, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                          sizeof doc);
      }
    }

    {
      auto [id, column] = writer.push_column(
        {irs::Type<irs::compression::None>::get(), {}, has_encryption},
        DefaultFinalizer(43, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteByte(static_cast<irs::byte_type>(doc & 0xFF));
      }
    }

    ASSERT_TRUE(writer.commit(state));
  }
  {
    irs::columnstore2::Reader reader;
    auto options = ReaderOptions();
    ASSERT_TRUE(reader.prepare(dir(), meta, options));
    ASSERT_EQ(2, reader.size());
    if (Buffered()) {
      ASSERT_GT(mem.cached_columns.Counter(), 0);
    } else {
      ASSERT_EQ(0, mem.cached_columns.Counter());
    }
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(Consolidation() ? ColumnType::DenseFixed : ColumnType::Fixed,
              header->type);
    ASSERT_EQ(has_encryption
                ? (ColumnProperty::Encrypt | ColumnProperty::NoName)
                : ColumnProperty::NoName,
              header->props);
  }
}

TEST_P(Columnstore2TestCase, dense_fixed_length_column_mr) {
  constexpr irs::doc_id_t kMax = 5000;
  irs::SegmentMeta meta;
  meta.name = "test_mr";
  const bool has_encryption = bool(dir().attributes().encryption());

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };
  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    {
      auto [id, column] = writer.push_column(
        {irs::Type<irs::compression::None>::get(), {}, has_encryption},
        DefaultFinalizer(42, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                          sizeof doc);
      }
    }

    {
      auto [id, column] = writer.push_column(
        {irs::Type<irs::compression::None>::get(), {}, has_encryption},
        DefaultFinalizer(43, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteByte(static_cast<irs::byte_type>(doc & 0xFF));
      }
    }

    ASSERT_TRUE(writer.commit(state));
  }
  auto& mem = GetResourceManager();
  mem.cached_columns.result = false;
  {
    irs::columnstore2::Reader reader;
    auto options = ReaderOptions();
    ASSERT_TRUE(reader.prepare(dir(), meta, options));
    ASSERT_EQ(2, reader.size());
    if (this->Buffered()) {
      // we still record an attempt of allocating
      ASSERT_GT(mem.cached_columns.Counter(), 0);
    } else {
      ASSERT_EQ(0, mem.cached_columns.Counter());
    }
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(Consolidation() ? ColumnType::DenseFixed : ColumnType::Fixed,
              header->type);
    ASSERT_EQ(has_encryption
                ? (ColumnProperty::Encrypt | ColumnProperty::NoName)
                : ColumnProperty::NoName,
              header->props);
  }
  // should not be a deallocation because SimpleMemoryAccounter::Increase
  // throws exception AFTER counting
  if (this->Buffered()) {
    ASSERT_GT(mem.cached_columns.Counter(), 0);
  } else {
    ASSERT_EQ(0, mem.cached_columns.Counter());
  }
}

TEST_P(Columnstore2TestCase, DenseFixedLengthColumn) {
  constexpr irs::doc_id_t kMax = 1000000;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    {
      auto [id, column] = writer.push_column(
        ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                          sizeof doc);
      }
    }

    {
      auto [id, column] = writer.push_column(
        ColumnInfo(), DefaultFinalizer(43, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteByte(static_cast<irs::byte_type>(doc & 0xFF));
      }
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(2, reader.size());

    {
      auto assert_payload = [this](irs::doc_id_t doc,
                                   const irs::PayAttr& payload) {
        if (HasPayload()) {
          irs::doc_id_t actual_doc[1];
          ASSERT_EQ(sizeof actual_doc, payload.value.size());
          memcpy(&actual_doc, payload.value.data(), sizeof actual_doc);
          EXPECT_EQ(doc, actual_doc[0]);
        } else {
          ASSERT_TRUE(payload.value.empty());
        }
      };

      constexpr irs::field_id kColumnId = 0;

      auto* header = reader.header(kColumnId);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(kMax, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::min(), header->min);
      ASSERT_EQ(Consolidation() ? ColumnType::DenseFixed : ColumnType::Fixed,
                header->type);
      ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

      auto* column = reader.column(kColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(kMax, column->size());

      ASSERT_EQ(0, column->id());
      ASSERT_TRUE(irs::IsNull(column->name()));

      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(42, header_payload[0]);

      {
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          assert_payload(doc, *payload);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
        ASSERT_EQ(doc, it->seek(doc - 1));
        assert_payload(doc, *payload);
        AssertPrevDoc(*it, *prev_it);

        auto next_it = column->iterator(Hint());
        auto* next_payload = irs::get<irs::PayAttr>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        assert_payload(doc, *next_payload);
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          assert_payload(next_doc, *next_payload);
          AssertPrevDoc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_EQ(118774, it->seek(118774));
        assert_payload(118774, *payload);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    }

    {
      auto assert_payload = [this](irs::doc_id_t doc,
                                   const irs::PayAttr& payload) {
        if (HasPayload()) {
          ASSERT_EQ(1, payload.value.size());
          EXPECT_EQ(static_cast<irs::byte_type>(doc & 0xFF), payload.value[0]);
        } else {
          ASSERT_TRUE(payload.value.empty());
        }
      };

      constexpr irs::field_id kColumnId = 1;

      auto* header = reader.header(kColumnId);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(kMax, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::min(), header->min);
      ASSERT_EQ(Consolidation() ? ColumnType::DenseFixed : ColumnType::Fixed,
                header->type);
      ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

      auto* column = reader.column(kColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(kMax, column->size());

      ASSERT_EQ(1, column->id());
      ASSERT_TRUE(irs::IsNull(column->name()));

      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(43, header_payload[0]);

      {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          assert_payload(doc, *payload);
          AssertPrevDoc(*it, *prev_it);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
        ASSERT_EQ(doc, it->seek(doc - 1));
        assert_payload(doc, *payload);
        AssertPrevDoc(*it, *prev_it);

        auto next_it = column->iterator(Hint());
        auto* next_payload = irs::get<irs::PayAttr>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        assert_payload(doc, *next_payload);
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          assert_payload(next_doc, *next_payload);
          AssertPrevDoc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_EQ(118774, it->seek(118774));
        assert_payload(static_cast<irs::byte_type>(118774 & 0xFF), *payload);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    }
  }
}

TEST_P(Columnstore2TestCase, dense_fixed_length_column_empty_tail) {
  constexpr irs::doc_id_t kMax = 1000000;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    {
      auto [id, column] = writer.push_column(
        ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto& stream = column(doc);
        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&doc),
                          sizeof doc);
      }
    }

    {
      // empty column has to be removed
      [[maybe_unused]] auto [id, column] =
        writer.push_column(ColumnInfo(), NonCalledFinalizer());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    ASSERT_EQ(1, reader.size());

    {
      auto assert_payload = [this](irs::doc_id_t doc,
                                   const irs::PayAttr& payload) {
        if (HasPayload()) {
          irs::doc_id_t actual_doc[1];
          ASSERT_EQ(sizeof actual_doc, payload.value.size());
          memcpy(&actual_doc, payload.value.data(), sizeof actual_doc);
          EXPECT_EQ(doc, actual_doc[0]);
        } else {
          ASSERT_TRUE(payload.value.empty());
        }
      };

      constexpr irs::field_id kColumnId = 0;

      auto* header = reader.header(kColumnId);
      ASSERT_NE(nullptr, header);
      ASSERT_EQ(kMax, header->docs_count);
      ASSERT_EQ(0, header->docs_index);
      ASSERT_EQ(irs::doc_limits::min(), header->min);
      ASSERT_EQ(Consolidation() ? ColumnType::DenseFixed : ColumnType::Fixed,
                header->type);
      ASSERT_EQ(ColumnProperty(ColumnProperty::NoName), header->props);

      auto* column = reader.column(kColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(kMax, column->size());

      ASSERT_EQ(0, column->id());
      ASSERT_TRUE(irs::IsNull(column->name()));

      const auto header_payload = column->payload();
      ASSERT_EQ(1, header_payload.size());
      ASSERT_EQ(42, header_payload[0]);

      {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
          ASSERT_EQ(doc, it->seek(doc));
          ASSERT_EQ(doc, it->seek(doc));
          assert_payload(doc, *payload);
          AssertPrevDoc(*it, *prev_it);
        }
      }

      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax; ++doc) {
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        ASSERT_EQ(doc, it->seek(doc));
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
      }

      // seek + next
      for (irs::doc_id_t doc = irs::doc_limits::min(); doc <= kMax;
           doc += 10000) {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        auto* score = irs::get<irs::ScoreAttr>(*it);
        ASSERT_NE(nullptr, score);
        ASSERT_TRUE(score->Func() == &irs::ScoreFunction::DefaultScore);

        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
        ASSERT_EQ(doc, it->seek(doc));
        assert_payload(doc, *payload);
        ASSERT_EQ(doc, it->seek(doc - 1));
        assert_payload(doc, *payload);
        AssertPrevDoc(*it, *prev_it);

        auto next_it = column->iterator(Hint());
        auto* next_payload = irs::get<irs::PayAttr>(*next_it);
        ASSERT_NE(nullptr, next_payload);
        ASSERT_EQ(doc, next_it->seek(doc));
        assert_payload(doc, *next_payload);
        for (auto next_doc = doc + 1; next_doc <= kMax; ++next_doc) {
          ASSERT_TRUE(next_it->next());
          ASSERT_EQ(next_doc, next_it->value());
          assert_payload(next_doc, *next_payload);
          AssertPrevDoc(*next_it, *prev_it);
        }
      }

      // next + seek
      {
        auto prev_it = column->iterator(Hint());
        auto it = column->iterator(Hint());
        auto* document = irs::get<irs::DocAttr>(*it);
        ASSERT_NE(nullptr, document);
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        auto* cost = irs::get<irs::CostAttr>(*it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(column->size(), cost->estimate());
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::doc_limits::min(), document->value);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_EQ(118774, it->seek(118774));
        assert_payload(118774, *payload);
        AssertPrevDoc(*it, *prev_it);
        ASSERT_TRUE(irs::doc_limits::eof(it->seek(kMax + 1)));
      }
    }
  }
}

TEST_P(Columnstore2TestCase, empty_columns) {
  constexpr irs::doc_id_t kMax = 1000000;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    {
      // empty column must be removed
      [[maybe_unused]] auto [id, column] =
        writer.push_column(ColumnInfo(), NonCalledFinalizer());
    }

    {
      // empty column must be removed
      [[maybe_unused]] auto [id, column] =
        writer.push_column(ColumnInfo(), NonCalledFinalizer());
    }

    ASSERT_FALSE(writer.commit(state));
  }

  size_t count = 0;
  ASSERT_TRUE(dir().visit([&count](auto) {
    ++count;
    return false;
  }));

  ASSERT_EQ(0, count);
}

TEST_P(Columnstore2TestCase, dense_fixed_large_values) {
  constexpr irs::doc_id_t kMin = 1;
  constexpr irs::doc_id_t kMax = 100;
  constexpr size_t kValueSize = 1'000;
  irs::SegmentMeta meta;
  meta.name = "test";

  irs::FlushState state{
    .name = meta.name,
    .doc_count = kMax,
  };

  {
    irs::columnstore2::Writer writer(Version(), Consolidation(),
                                     irs::IResourceManager::gNoop);
    writer.prepare(dir(), meta);

    auto [id, column] = writer.push_column(
      ColumnInfo(), DefaultFinalizer(42, std::string_view{}));

    for (irs::doc_id_t doc = kMin; doc <= kMax; ++doc) {
      auto& stream = column(doc);
      auto str = std::to_string(doc);
      str.resize(kValueSize, 'a');
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.c_str()),
                        str.size());
    }

    ASSERT_TRUE(writer.commit(state));
  }

  {
    irs::columnstore2::Reader reader;
    ASSERT_TRUE(reader.prepare(dir(), meta, ReaderOptions()));
    auto* header = reader.header(0);
    ASSERT_NE(nullptr, header);
    ASSERT_EQ(kMax, header->docs_count);
    ASSERT_EQ(0, header->docs_index);
    ASSERT_EQ(irs::doc_limits::min(), header->min);
    ASSERT_EQ(Consolidation() ? ColumnType::DenseFixed : ColumnType::Fixed,
              header->type);
    ASSERT_EQ(1, reader.size());

    auto* column = reader.column(0);

    ASSERT_NE(nullptr, column);
    auto it = column->iterator(Hint());
    irs::doc_id_t expected_doc = kMin - 1;
    while (it->next()) {
      expected_doc++;
      ASSERT_EQ(expected_doc, it->value());
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_NE(nullptr, payload);

      auto str = std::to_string(expected_doc);
      str.resize(kValueSize, 'a');
      if (HasPayload()) {
        ASSERT_EQ(str, irs::ViewCast<char>(payload->value));
      } else {
        ASSERT_TRUE(payload->value.empty());
      }
    }
    ASSERT_EQ(kMax, expected_doc);
  }
}

static constexpr auto kTestDirs =
  tests::GetDirectories<tests::kTypesDefaultRot13>();

INSTANTIATE_TEST_SUITE_P(
  columnstore2_test, Columnstore2TestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(irs::ColumnHint::Normal,
                                       irs::ColumnHint::Consolidation,
                                       irs::ColumnHint::Mask,
                                       irs::ColumnHint::PrevDoc),
                     ::testing::Values(irs::columnstore2::Version::Min),
                     ::testing::Values(true, false)),
  &Columnstore2TestCase::to_string);
