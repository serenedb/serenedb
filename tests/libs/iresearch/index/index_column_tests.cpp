
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
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/lz4compression.hpp>

#include "index_tests.hpp"
#include "tests_shared.hpp"

namespace {
bool Visit(const irs::ColumnReader& reader,
           const std::function<bool(irs::doc_id_t, irs::bytes_view)>& visitor) {
  auto it = reader.iterator(irs::ColumnHint::Consolidation);

  irs::PayAttr dummy;
  auto* doc = irs::get<irs::DocAttr>(*it);
  if (!doc) {
    return false;
  }
  auto* payload = irs::get<irs::PayAttr>(*it);
  if (!payload) {
    payload = &dummy;
  }

  while (it->next()) {
    if (!visitor(doc->value, payload->value)) {
      return false;
    }
  }

  return true;
}
}  // namespace

class IndexColumnTestCase : public tests::IndexTestBase {};

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_sparse_variable_length) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // SparseColumn<sparse_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";
  size_t inserted = 0;

  // write documents
  {
    struct Stored {
      std::string_view Name() const { return kColumnName; }

      bool Write(irs::DataOutput& out) const {
        auto str = std::to_string(value);
        if (value % 3) {
          str.append(kColumnName.data(), kColumnName.size());
        }

        irs::WriteStr(out, str);
        return true;
      }

      uint64_t value{};
    } field;

    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      auto doc = ctx.Insert();

      if (field.value % 2) {
        doc.Insert<irs::Action::STORE>(field);
        ++inserted;
      }
    } while (++field.value < kMaxDocs);  // insert MAX_DOCS documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - not cached
  // - cached
  // - cached
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs / 2, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        expected_doc += 2;
        expected_value += 2;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        expected_doc += 2;
        expected_value += 2;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }
  }

  // check inserted values:
  // - not cached
  // - not cached
  // - cached
  // - cached
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        expected_doc += 2;
        expected_value += 2;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        expected_doc += 2;
        expected_value += 2;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Consolidation);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }
  }

  // check inserted values:
  // - not cached
  // - not cached
  // - cached
  // - cached
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        expected_doc += 2;
        expected_value += 2;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;
      for (; expected_doc <= kMaxDocs;) {
        auto expected_value_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ASSERT_EQ(expected_doc,
                  it->seek(expected_value));  // seek before the existing key
                                              // (value should remain the same)
        actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }

    // seek over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;
      for (; expected_doc <= kMaxDocs;) {
        auto expected_value_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->seek(expected_value));
        auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ASSERT_EQ(expected_doc,
                  it->seek(expected_doc));  // seek to the existing key (value
                                            // should remain the same)
        actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 3) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      expected_doc += 2;
      expected_value += 2;
      ++docs;

      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 3) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      expected_doc += 2;
      expected_value += 2;
      ++docs;

      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs;
      auto expected_value = kMaxDocs - 1;
      auto expected_value_str = std::to_string(expected_value);
      if (expected_value % 3) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      it->seek(expected_doc);
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_doc, it->value());
      ASSERT_EQ(expected_value_str, actual_value_str);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_value = kMaxDocs - 1;
      auto expected_value_str = std::to_string(expected_value);
      if (expected_value % 3) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      it->seek(expected_value);
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(kMaxDocs, it->value());
      ASSERT_EQ(expected_value_str, actual_value_str);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to after the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Consolidation);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;

      for (;;) {
        it->seek(expected_doc);

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        ++docs;

        auto next_expected_doc = expected_doc + 2;
        auto next_expected_value = expected_value + 2;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());
          auto next_expected_value_str = std::to_string(next_expected_value);

          if (next_expected_value % 3) {
            next_expected_value_str.append(kColumnName.data(),
                                           kColumnName.size());
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          next_expected_doc += 2;
          next_expected_value += 2;
          ++docs;
        }

        expected_doc = next_expected_doc;
        expected_value = next_expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = 2;
      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;
      size_t docs = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs;) {
        auto it = column->iterator(irs::ColumnHint::Consolidation);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_value_str, actual_value_str);

        ++docs;

        auto next_expected_doc = expected_doc + 2;
        auto next_expected_value = expected_value + 2;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());
          auto next_expected_value_str = std::to_string(next_expected_value);

          if (next_expected_value % 3) {
            next_expected_value_str.append(kColumnName.data(),
                                           kColumnName.size());
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          next_expected_doc += 2;
          next_expected_value += 2;
        }

        expected_doc -= 2;
        expected_value -= 2;
      }

      ASSERT_EQ(inserted, docs);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      it->seek(expected_doc);
      expected_doc = min_doc;
      expected_value = expected_doc - 1;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 3) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      auto next_expected_doc = expected_doc + 2;
      auto next_expected_value = expected_value + 2;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        auto next_expected_value_str = std::to_string(next_expected_value);
        if (next_expected_value % 3) {
          next_expected_value_str.append(kColumnName.data(),
                                         kColumnName.size());
        }

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value_str, actual_value_str);

        next_expected_doc += 2;
        next_expected_value += 2;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 3) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      auto next_expected_doc = expected_doc + 2;
      auto next_expected_value = expected_value + 2;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto next_expected_value_str = std::to_string(next_expected_value);

        if (next_expected_value % 3) {
          next_expected_value_str.append(kColumnName.data(),
                                         kColumnName.size());
        }

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value_str, actual_value_str);

        next_expected_doc += 2;
        next_expected_value += 2;
      }

      expected_doc -= 2;
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
    }

    // seek over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 3) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        expected_doc += 2;
        expected_value += 2;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      irs::doc_id_t expected_value = 1;
      size_t docs = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 3) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        expected_doc += 2;
        expected_value += 2;
        ++docs;
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(inserted, docs);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_mask) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // SparseColumn<dense_mask_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kBlockSize = 1024;
  static const irs::doc_id_t kMaxDocs =
    kBlockSize * kBlockSize  // full index block
    + 2051;                  // tail index block
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      explicit Stored(const std::string_view& name) noexcept
        : column_name(name) {}

      std::string_view Name() const { return column_name; }

      bool Write(irs::DataOutput&) const { return true; }

      const std::string_view column_name;
    } field(kColumnName), gap("gap");

    irs::doc_id_t docs_count = 0;
    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++docs_count < kBlockSize);  // insert BLOCK_SIZE documents

    ctx.Insert().Insert<irs::Action::STORE>(gap);

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++docs_count < kMaxDocs);  // insert BLOCK_SIZE documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - not cached
  // - not cached
  // - cached
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;

        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;

        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);

      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++expected_doc;
        ++docs_count;

        if (docs_count == kBlockSize) {
          // gap
          ++expected_doc;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }
  }

  // check inserted values:
  // - not cached
  // - not cached
  // - cached
  // - cached
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;

        if (kBlockSize == docs_count) {
          // gap
          ++expected_doc;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);

      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++expected_doc;
        ++docs_count;

        if (kBlockSize == docs_count) {
          // gap
          ++expected_doc;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;

        if (kBlockSize == docs_count) {
          // gap
          ++expected_doc;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++expected_doc;
        ++docs_count;

        if (kBlockSize == docs_count) {
          // gap
          ++expected_doc;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }
  }

  // check inserted values:
  // - not cached
  // - not cached
  // - cached
  // - cached
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;

        if (kBlockSize == docs_count) {
          // gap
          ++expected_doc;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; expected_doc <= kMaxDocs + 1;) {
        if (expected_doc == 1 + kBlockSize) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;  // gap
        } else {
          ASSERT_EQ(expected_doc, it->seek(expected_doc));
        }
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek to begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t docs_count = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      ++expected_doc;
      ++docs_count;

      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++expected_doc;
        ++docs_count;

        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek before begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t docs_count = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      ++expected_doc;
      ++docs_count;

      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++expected_doc;
        ++docs_count;

        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      ASSERT_EQ(kMaxDocs + 1, it->seek(kMaxDocs + 1));

      ASSERT_EQ(irs::bytes_view{}, payload->value);  // mask block has no data
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      ASSERT_EQ(kMaxDocs, it->seek(kMaxDocs));

      ASSERT_EQ(irs::bytes_view{}, payload->value);  // mask block has no data

      ASSERT_TRUE(it->next());
      ASSERT_EQ(kMaxDocs + 1, it->value());

      ASSERT_EQ(irs::bytes_view{}, payload->value);  // mask block has no data

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek to after the end + next + seek before end
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      it->seek(kMaxDocs + 2);
      ASSERT_EQ(irs::doc_limits::eof(), it->value());

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek to gap + next(x5)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = kBlockSize + 2;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      ASSERT_EQ(expected_doc, it->value());

      for (; it->next();) {
        ++expected_doc;

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t docs_count = 0;

      for (;;) {
        if (docs_count == kBlockSize) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;  // gap
        } else {
          if (expected_doc > kMaxDocs + 1) {
            ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
          } else {
            ASSERT_EQ(expected_doc, it->seek(expected_doc));
          }
        }

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data

        ++docs_count;
        ASSERT_EQ(expected_doc, it->value());

        auto next_expected_doc = expected_doc + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          ASSERT_EQ(next_expected_doc, it->value());

          ASSERT_EQ(irs::bytes_view{},
                    payload->value);  // mask block has no data

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));

          ++next_expected_doc;
          ++docs_count;

          if (docs_count == kBlockSize) {
            ++next_expected_doc;  // gap
          }
        }

        expected_doc = next_expected_doc;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_doc = kMaxDocs + 1;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs + 1;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_TRUE(payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());

        ++docs_count;

        if (expected_doc == kBlockSize + 1) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;  // gap
        } else {
          ASSERT_EQ(expected_doc, it->seek(expected_doc));
        }

        auto next_expected_doc = expected_doc + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          if (next_expected_doc == kBlockSize + 1) {
            ++next_expected_doc;  // gap
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(irs::bytes_view{},
                    payload->value);  // mask block has no data
          ++next_expected_doc;
        }

        --expected_doc;

        if (expected_doc == kBlockSize + 1) {
          --expected_doc;  // gap
        }
      }
      ASSERT_EQ(kMaxDocs, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      ASSERT_EQ(min_doc, it->seek(expected_doc));
      expected_doc = min_doc;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      ASSERT_EQ(irs::bytes_view{}, payload->value);  // mask block has no data

      auto next_expected_doc = expected_doc + 1;
      for (size_t i = 0; i < steps_forward; ++i) {
        if (next_expected_doc == kBlockSize + 1) {
          ++next_expected_doc;  // gap
        }
        ASSERT_TRUE(it->next());
        ASSERT_EQ(next_expected_doc, it->value());
        ++next_expected_doc;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = kMaxDocs;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      ASSERT_EQ(irs::bytes_view{}, payload->value);  // mask block has no data

      auto next_expected_doc = expected_doc + 1;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data
        ++next_expected_doc;
      }

      --expected_doc;
      it->seek(expected_doc);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;

        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
        }

        ASSERT_EQ(irs::bytes_view{},
                  payload->value);  // mask block has no data

        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_variable_length) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // SparseColumn<dense_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::None>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kBlockSize = 1024;
  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      explicit Stored(const std::string_view& name) noexcept
        : column_name(name) {}

      std::string_view Name() const { return column_name; }

      bool Write(irs::DataOutput& out) const {
        auto str = std::to_string(value);
        if (value % 2) {
          str.append(column_name.data(), column_name.size());
        }

        irs::WriteStr(out, str);
        return true;
      }

      uint64_t value{};
      const std::string_view column_name;
    } field(kColumnName), gap("gap");

    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < kBlockSize);  // insert MAX_DOCS documents

    ctx.Insert().Insert<irs::Action::STORE>(gap);  // gap
    ++field.value;

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value <= kMaxDocs);  // insert MAX_DOCS documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
        ++docs_count;

        if (docs_count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++docs_count;
        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++docs_count;
        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - seek (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; expected_doc <= kMaxDocs + 1;) {
        if (expected_doc == kBlockSize + 1) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;  // gap
          ++expected_value;
        } else {
          ASSERT_EQ(expected_doc, it->seek(expected_doc));
        }

        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ++docs_count;
      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        ++docs_count;
        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ++docs_count;
      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        ++docs_count;
        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs + 1;
      auto expected_value = kMaxDocs;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs;
      auto expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ++expected_doc;
      ++expected_value;
      expected_value_str = std::to_string(expected_value);
      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_TRUE(it->next());
      ASSERT_EQ(expected_doc, it->value());
      ASSERT_EQ(expected_value_str,
                irs::ToString<std::string_view>(payload->value.data()));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to after the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 2));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      for (;;) {
        if (expected_doc == kBlockSize + 1) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;  // gap
          ++expected_value;
        } else {
          if (expected_doc > kMaxDocs + 1) {
            ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
          } else {
            ASSERT_EQ(expected_doc, it->seek(expected_doc));
          }
        }

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        ++docs_count;

        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          if (next_expected_doc == kBlockSize + 1) {
            ++next_expected_doc;  // gap
            ++next_expected_value;
          }

          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());
          auto next_expected_value_str = std::to_string(next_expected_value);

          if (next_expected_value % 2) {
            next_expected_value_str.append(kColumnName.data(),
                                           kColumnName.size());
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          ++docs_count;
          ++next_expected_doc;
          ++next_expected_value;
        }

        expected_doc = next_expected_doc;
        expected_value = next_expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_doc = kMaxDocs + 1;
      irs::doc_id_t expected_value = expected_doc - 1;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs + 1;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ++docs_count;

        ASSERT_EQ(expected_value_str, actual_value_str);

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          if (next_expected_doc == kBlockSize + 1) {
            ++next_expected_doc;  // gap
            ++next_expected_value;
          }

          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());
          auto next_expected_value_str = std::to_string(next_expected_value);

          if (next_expected_value % 2) {
            next_expected_value_str.append(kColumnName.data(),
                                           kColumnName.size());
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          ++next_expected_doc;
          ++next_expected_value;
        }

        --expected_doc;
        --expected_value;

        if (expected_doc == kBlockSize + 1) {
          --expected_doc;  // gap
          --expected_value;
        }
      }
      ASSERT_EQ(kMaxDocs, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      it->seek(expected_doc);
      expected_doc = min_doc;
      expected_value = expected_doc - 1;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        auto next_expected_value_str = std::to_string(next_expected_value);
        if (next_expected_value % 2) {
          next_expected_value_str.append(kColumnName.data(),
                                         kColumnName.size());
        }

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value_str, actual_value_str);

        ++next_expected_doc;
        ++next_expected_value;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        if (next_expected_doc == kBlockSize + 1) {
          ++next_expected_doc;  // gap
          ++next_expected_value;
        }

        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto next_expected_value_str = std::to_string(next_expected_value);

        if (next_expected_value % 2) {
          next_expected_value_str.append(kColumnName.data(),
                                         kColumnName.size());
        }

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value_str, actual_value_str);

        ++next_expected_doc;
        ++next_expected_value;
      }

      --expected_doc;
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++docs_count;
        ++expected_doc;
        ++expected_value;

        if (expected_doc == kBlockSize + 1) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, docs_count);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_fixed_offset) {
  // SparseColumn<dense_fixed_length_block>

  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::None>::get(),
                           irs::compression::Options{}, false};
  };

  // border case for sparse fixed offset columns, e.g.
  // |--------------|------------|
  // |doc           | value_size |
  // |--------------|------------|
  // | 1            | 0          |
  // | .            | 0          |
  // | .            | 0          |
  // | .            | 0          |
  // | BLOCK_SIZE-1 | 1          | <-- end of column block
  // | BLOCK_SIZE+1 | 0          |
  // | .            | 0          |
  // | .            | 0          |
  // | MAX_DOCS     | 1          |
  // |--------------|------------|

  static const irs::doc_id_t kBlockSize = 1024;
  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      explicit Stored(const std::string_view& name) noexcept
        : column_name(name) {}

      std::string_view Name() const { return column_name; }

      bool Write(irs::DataOutput& out) const {
        if (value == kBlockSize - 1) {
          out.WriteByte(0);
        } else if (value == kMaxDocs) {
          out.WriteByte(1);
        }

        return true;
      }

      uint32_t value{};
      const std::string_view column_name;
    } field(kColumnName), gap("gap");

    auto writer =
      irs::IndexWriter::Make(this->dir(), this->codec(), irs::kOmCreate);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < kBlockSize);  // insert BLOCK_SIZE documents

    ctx.Insert().Insert<irs::Action::STORE>(gap);  // gap
    ++field.value;

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < (1 + kMaxDocs));  // insert BLOCK_SIZE documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        ++expected_doc;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
        }

        if (count == kBlockSize) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\0", 1)) !=
              actual_data) {
            return false;
          }
        } else if (count == kMaxDocs) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\1", 1)) !=
              actual_data) {
            return false;
          }
        } else if (!actual_data.empty()) {
          return false;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        ++expected_doc;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
        }

        if (count == kBlockSize) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\0", 1)) !=
              actual_data) {
            return false;
          }
        } else if (count == kMaxDocs) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\1", 1)) !=
              actual_data) {
            return false;
          }
        } else if (!actual_data.empty()) {
          return false;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        const auto actual_data = payload->value;

        ASSERT_EQ(expected_doc, it->value());

        ++expected_doc;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
        }

        if (count == kBlockSize) {
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("\0", 1)),
                    actual_data);
        } else if (count == kMaxDocs) {
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("\1", 1)),
                    actual_data);
        } else {
          ASSERT_EQ(irs::bytes_view{}, actual_data);
        }
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, count);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_dense_column_dense_fixed_offset) {
  // DenseFixedLengthColumn<dense_fixed_length_block>

  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  // border case for dense fixed offset columns, e.g.
  // |--------------|------------|
  // |doc           | value_size |
  // |--------------|------------|
  // | 1            | 0          |
  // | .            | 0          |
  // | .            | 0          |
  // | .            | 0          |
  // | BLOCK_SIZE-1 | 1          | <-- end of column block
  // | BLOCK_SIZE   | 0          |
  // | .            | 0          |
  // | .            | 0          |
  // | MAX_DOCS     | 1          |
  // |--------------|------------|

  static const irs::doc_id_t kMaxDocs = 1500;
  static const irs::doc_id_t kBlockSize = 1024;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      std::string_view Name() const { return kColumnName; }

      bool Write(irs::DataOutput& out) const {
        if (value == kBlockSize - 1) {
          out.WriteByte(0);
        } else if (value == kMaxDocs - 1) {
          out.WriteByte(1);
        }
        return true;
      }

      uint64_t value{};
    } field;

    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < kMaxDocs);  // insert MAX_DOCS documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t count = 0;
      auto visitor = [&count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        ++expected_doc;
        ++count;

        if (count == kBlockSize) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\0", 1)) !=
              actual_data) {
            return false;
          }
        } else if (count == kMaxDocs) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\1", 1)) !=
              actual_data) {
            return false;
          }
        } else if (!actual_data.empty()) {
          return false;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t count = 0;
      auto visitor = [&count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        ++expected_doc;
        ++count;

        if (count == kBlockSize) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\0", 1)) !=
              actual_data) {
            return false;
          }
        } else if (count == kMaxDocs) {
          if (irs::ViewCast<irs::byte_type>(std::string_view("\1", 1)) !=
              actual_data) {
            return false;
          }
        } else if (!actual_data.empty()) {
          return false;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        const auto actual_data = payload->value;

        ASSERT_EQ(expected_doc, it->value());

        ++expected_doc;
        ++count;

        if (count == kBlockSize) {
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("\0", 1)),
                    actual_data);
        } else if (count == kMaxDocs) {
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("\1", 1)),
                    actual_data);
        } else {
          ASSERT_EQ(irs::bytes_view{}, actual_data);
        }
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, count);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_dense_fixed_length) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // SparseColumn<dense_fixed_length_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, false};
  };

  static const irs::doc_id_t kBlockSize = 1024;
  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      explicit Stored(const std::string_view& name) noexcept
        : column_name(name) {}

      std::string_view Name() const { return column_name; }

      bool Write(irs::DataOutput& out) const {
        irs::WriteStr(
          out, irs::numeric_utils::numeric_traits<uint32_t>::raw_ref(value));
        return true;
      }

      uint32_t value{};
      const std::string_view column_name;
    } field(kColumnName), gap("gap");

    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < kBlockSize);  // insert BLOCK_SIZE documents

    ctx.Insert().Insert<irs::Action::STORE>(gap);  // gap
    ++field.value;

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < (1 + kMaxDocs));  // insert BLOCK_SIZE documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&count, &expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&count, &expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), expected_value);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&count, &expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), expected_value);
    }

    // visit values (cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&count, &expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), expected_value);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - seek (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&count, &expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; expected_doc <= 1 + kMaxDocs;) {
        if (expected_doc == 1025) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;
          ++expected_value;
        } else {
          ASSERT_EQ(expected_doc, it->seek(expected_doc));
        }
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(1 + kMaxDocs, expected_value);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        if (expected_doc == 1025) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(1 + kMaxDocs, expected_value);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        if (expected_doc == 1025) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(1 + kMaxDocs, expected_value);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs + 1;
      auto expected_value = kMaxDocs;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs;
      auto expected_value = kMaxDocs - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ++expected_doc;
      ++expected_value;
      ASSERT_TRUE(it->next());
      actual_value_str = irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_doc, it->value());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to after the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 2));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // FIXME revisit
    // seek to gap + next(x5)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_TRUE(payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = kBlockSize + 2;
      irs::doc_id_t expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      ASSERT_EQ(expected_doc, it->value());
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      for (; it->next();) {
        ++expected_doc;
        ++expected_value;

        ASSERT_EQ(expected_doc, it->value());
        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      for (;;) {
        if (expected_doc == 1025) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          ++expected_doc;  // gap
          ++expected_value;
        } else {
          if (expected_doc > kMaxDocs + 1) {
            ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
          } else {
            ASSERT_EQ(expected_doc, it->seek(expected_doc));
          }
        }

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          if (next_expected_doc == 1025) {
            ++next_expected_doc;  // gap
            ++next_expected_value;
          }

          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(
            next_expected_value,
            *reinterpret_cast<const irs::doc_id_t*>(actual_value_str.data()));

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));
          ASSERT_EQ(
            next_expected_value,
            *reinterpret_cast<const irs::doc_id_t*>(actual_value_str.data()));

          ++next_expected_doc;
          ++next_expected_value;
        }

        expected_doc = next_expected_doc;
        expected_value = next_expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(1 + kMaxDocs, expected_value);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        if (expected_doc == 1025) {
          ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
          expected_doc++;
          expected_value++;
        } else {
          if (expected_doc > kMaxDocs + 1) {
            ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
          } else {
            ASSERT_EQ(expected_doc, it->seek(expected_doc));
          }
        }

        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ++docs_count;

        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          if (next_expected_doc == 1025) {
            ++next_expected_doc;  // gap
            ++next_expected_value;
          }

          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(
            next_expected_value,
            *reinterpret_cast<const irs::doc_id_t*>(actual_value_str.data()));

          ++next_expected_doc;
          ++next_expected_value;
        }

        --expected_doc;
        --expected_value;

        if (expected_doc == 1025) {
          // gap
          --expected_doc;
          --expected_value;
        }
      }
      ASSERT_EQ(kMaxDocs - 1, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      it->seek(expected_doc);
      expected_doc = min_doc;
      expected_value = expected_doc - 1;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                         actual_value_str.data()));

        ++next_expected_doc;
        ++next_expected_value;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;

      if (expected_doc == 1025) {
        ASSERT_EQ(expected_doc + 1, it->seek(expected_doc));
        expected_doc++;
        expected_value++;
      } else {
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
      }
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        if (next_expected_doc == 1025) {
          next_expected_doc++;  // gap
          next_expected_value++;
        }

        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                         actual_value_str.data()));

        ++next_expected_doc;
        ++next_expected_value;
      }

      --expected_doc;
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
    }

    // visit values (cached)
    {
      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&count, &expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }

        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      size_t count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;

        if (++count == kBlockSize) {
          ++expected_doc;  // gap
          ++expected_value;
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(1 + kMaxDocs), expected_value);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_sparse_column_sparse_mask) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // SparseColumn<sparse_mask_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      std::string_view Name() const { return kColumnName; }

      bool Write(irs::DataOutput&) const { return true; }
    } field;

    irs::doc_id_t docs_count = 0;
    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      auto doc = ctx.Insert();

      if (docs_count % 2) {
        doc.Insert<irs::Action::STORE>(field);
      }
    } while (++docs_count < kMaxDocs);  // insert MAX_DOCS/2 documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs / 2, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        expected_doc += 2;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        expected_doc += 2;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        expected_doc += 2;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // iterate over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        expected_doc += 2;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - seek (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        expected_doc += 2;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(
          expected_doc,
          it->seek(
            expected_doc -
            1));  // seek before the existing key (value should remain the same)
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // seek over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      size_t docs_count = 0;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(expected_doc,
                  it->seek(expected_doc));  // seek to the existing key (value
                                            // should remain the same)
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      size_t docs_count = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      expected_doc += 2;
      ++docs_count;

      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      size_t docs_count = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      expected_doc += 2;
      ++docs_count;

      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(kMaxDocs, it->seek(kMaxDocs));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(kMaxDocs, it->seek(kMaxDocs - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to after the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      size_t docs_count = 0;

      for (;;) {
        it->seek(expected_doc);

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ++docs_count;

        auto next_expected_doc = expected_doc + 2;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(irs::bytes_view{}, payload->value);

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));
          ASSERT_EQ(irs::bytes_view{}, payload->value);

          next_expected_doc += 2;
          ++docs_count;
        }

        expected_doc = next_expected_doc;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs / 2, docs_count);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = 2;
      irs::doc_id_t expected_doc = kMaxDocs;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);

      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ++docs_count;

        auto next_expected_doc = expected_doc + 2;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(irs::bytes_view{}, payload->value);

          next_expected_doc += 2;
        }

        expected_doc -= 2;
      }
      ASSERT_EQ(kMaxDocs / 2, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      it->seek(expected_doc);
      expected_doc = min_doc;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto next_expected_doc = expected_doc + 2;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        next_expected_doc += 2;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = kMaxDocs;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto next_expected_doc = expected_doc + 2;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        next_expected_doc += 2;
      }

      expected_doc -= 2;
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = 2;
      size_t docs_count = 0;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        expected_doc += 2;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = 2;
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        expected_doc += 2;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs / 2), docs_count);
    }
  }
}

TEST_P(IndexColumnTestCase, read_write_doc_attributes_dense_column_dense_mask) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // DenseFixedLengthColumn<dense_mask_block>

  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kMaxDocs = 1024 * 1024  // full index block
                                        + 2051;      // tail index block
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      std::string_view Name() const { return kColumnName; }

      bool Write(irs::DataOutput&) const { return true; }
    } field;

    irs::doc_id_t docs_count = 0;
    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++docs_count < kMaxDocs);  // insert MAX_DOCS documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - seek (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t docs_count = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      ++expected_doc;
      ++docs_count;

      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t docs_count = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      ++expected_doc;
      ++docs_count;

      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      ASSERT_EQ(kMaxDocs, it->seek(kMaxDocs));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      ASSERT_EQ(kMaxDocs - 1, it->seek(kMaxDocs - 1));

      ASSERT_TRUE(it->next());
      ASSERT_EQ(kMaxDocs, it->value());

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek to after the end + next + seek before end
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      it->seek(kMaxDocs + 1);
      ASSERT_EQ(irs::doc_limits::eof(), it->value());

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs - 1));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      size_t docs_count = 0;

      for (;;) {
        it->seek(expected_doc);

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        ++docs_count;
        ASSERT_EQ(expected_doc, it->value());

        auto next_expected_doc = expected_doc + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          ASSERT_EQ(next_expected_doc, it->value());

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));

          ++next_expected_doc;
          ++docs_count;
        }

        expected_doc = next_expected_doc;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(kMaxDocs, docs_count);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_doc = kMaxDocs;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        ASSERT_TRUE(
          !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());

        ++docs_count;
        ASSERT_EQ(expected_doc, it->seek(expected_doc));

        auto next_expected_doc = expected_doc + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          ASSERT_EQ(next_expected_doc, it->value());
          ++next_expected_doc;
        }

        --expected_doc;
      }
      ASSERT_EQ(kMaxDocs, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      it->seek(expected_doc);
      expected_doc = min_doc;
      ASSERT_EQ(min_doc, it->seek(expected_doc));

      auto next_expected_doc = expected_doc + 1;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        ASSERT_EQ(next_expected_doc, it->value());
        ++next_expected_doc;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t expected_doc = kMaxDocs;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));

      auto next_expected_doc = expected_doc + 1;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        ASSERT_EQ(next_expected_doc, it->value());
        ++next_expected_doc;
      }

      --expected_doc;
      it->seek(expected_doc);
    }

    // visit values (cached)
    {
      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      auto visitor = [&docs_count, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        if (!irs::IsNull(actual_data)) {
          return false;
        }

        ++expected_doc;
        ++docs_count;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(
        !irs::get<irs::PayAttr>(*it));  // dense_mask does not have a payload
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());

      irs::doc_id_t docs_count = 0;
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      for (; it->next();) {
        ASSERT_EQ(expected_doc, it->value());
        ++expected_doc;
        ++docs_count;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), docs_count);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_dense_column_dense_fixed_length) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // DenseFixedLengthColumn<dense_fixed_length_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      std::string_view Name() const { return kColumnName; }

      bool Write(irs::DataOutput& out) const {
        irs::WriteStr(
          out, irs::numeric_utils::numeric_traits<uint64_t>::raw_ref(value));
        return true;
      }

      uint64_t value{};
    } field;

    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < kMaxDocs);  // insert MAX_DOCS documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), expected_value);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), expected_value);
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), expected_value);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - seek (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs;
      auto expected_value = kMaxDocs - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs - 1;
      auto expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ++expected_doc;
      ++expected_value;
      ASSERT_TRUE(it->next());
      actual_value_str = irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_doc, it->value());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to after the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      for (;;) {
        it->seek(expected_doc);

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(
            next_expected_value,
            *reinterpret_cast<const irs::doc_id_t*>(actual_value_str.data()));

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));
          ASSERT_EQ(
            next_expected_value,
            *reinterpret_cast<const irs::doc_id_t*>(actual_value_str.data()));

          ++next_expected_doc;
          ++next_expected_value;
        }

        expected_doc = next_expected_doc;
        expected_value = next_expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ++docs_count;

        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(
            next_expected_value,
            *reinterpret_cast<const irs::doc_id_t*>(actual_value_str.data()));

          ++next_expected_doc;
          ++next_expected_value;
        }

        --expected_doc;
        --expected_value;
      }
      ASSERT_EQ(kMaxDocs, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      it->seek(expected_doc);
      expected_doc = min_doc;
      expected_value = expected_doc - 1;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                         actual_value_str.data()));

        ++next_expected_doc;
        ++next_expected_value;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                  actual_value_str.data()));

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                         actual_value_str.data()));

        ++next_expected_doc;
        ++next_expected_value;
      }

      --expected_doc;
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_value =
          irs::ToString<std::string_view>(actual_data.data());
        if (expected_value !=
            *reinterpret_cast<const irs::doc_id_t*>(actual_value.data())) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value, *reinterpret_cast<const irs::doc_id_t*>(
                                    actual_value_str.data()));

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(irs::doc_id_t(kMaxDocs), expected_value);
    }
  }
}

TEST_P(IndexColumnTestCase,
       read_write_doc_attributes_dense_column_dense_variable_length) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  // SparseColumn<dense_block>
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  static const irs::doc_id_t kMaxDocs = 1500;
  static const std::string_view kColumnName = "id";

  // write documents
  {
    struct Stored {
      std::string_view Name() const { return kColumnName; }

      bool Write(irs::DataOutput& out) const {
        auto str = std::to_string(value);
        if (value % 2) {
          str.append(kColumnName.data(), kColumnName.size());
        }

        irs::WriteStr(out, str);
        return true;
      }

      uint64_t value{};
    } field;

    auto writer = irs::IndexWriter::Make(this->dir(), this->codec(),
                                         irs::kOmCreate, options);
    auto ctx = writer->GetBatch();

    do {
      ctx.Insert().Insert<irs::Action::STORE>(field);
    } while (++field.value < kMaxDocs);  // insert MAX_DOCS documents

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of documents()
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // check number of documents in the column
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_EQ(kMaxDocs, column->size());
    }

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    {
      // iterate over column (not cached)
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - seek (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(this->dir(), this->codec());
    ASSERT_EQ(1, reader.size());

    auto& segment = *(reader.begin());
    ASSERT_EQ(irs::doc_id_t(kMaxDocs), segment.live_docs_count());

    auto* meta = segment.column(kColumnName);
    ASSERT_NE(nullptr, meta);

    // visit values (not cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // seek over column (not cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; expected_doc <= kMaxDocs;) {
        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek to the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek before the begin + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      ASSERT_EQ(expected_doc, it->seek(expected_doc - 1));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ++expected_doc;
      ++expected_value;

      for (; it->next();) {
        const auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        ++expected_doc;
        ++expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek to the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs;
      auto expected_value = kMaxDocs - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      const auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to before the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      auto expected_doc = kMaxDocs - 1;
      auto expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      ++expected_doc;
      ++expected_value;
      expected_value_str = std::to_string(expected_value);
      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_TRUE(it->next());
      ASSERT_EQ(expected_doc, it->value());
      ASSERT_EQ(expected_value_str,
                irs::ToString<std::string_view>(payload->value.data()));

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek to after the end + next
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs + 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      // can't seek backwards
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(kMaxDocs - 1));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;

      for (;;) {
        it->seek(expected_doc);

        if (irs::doc_limits::eof(it->value())) {
          break;
        }

        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_value_str);

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());
          auto next_expected_value_str = std::to_string(next_expected_value);

          if (next_expected_value % 2) {
            next_expected_value_str.append(kColumnName.data(),
                                           kColumnName.size());
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          // can't seek backwards
          ASSERT_EQ(next_expected_doc, it->seek(expected_doc));
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          ++next_expected_doc;
          ++next_expected_value;
        }

        expected_doc = next_expected_doc;
        expected_value = next_expected_value;
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      const irs::doc_id_t min_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;
      size_t docs_count = 0;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      for (; expected_doc >= min_doc && expected_doc <= kMaxDocs;) {
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        auto actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ++docs_count;

        ASSERT_EQ(expected_value_str, actual_value_str);

        auto next_expected_doc = expected_doc + 1;
        auto next_expected_value = expected_value + 1;
        for (size_t i = 0; i < steps_forward && it->next(); ++i) {
          actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());
          auto next_expected_value_str = std::to_string(next_expected_value);

          if (next_expected_value % 2) {
            next_expected_value_str.append(kColumnName.data(),
                                           kColumnName.size());
          }

          ASSERT_EQ(next_expected_doc, it->value());
          ASSERT_EQ(next_expected_value_str, actual_value_str);

          ++next_expected_doc;
          ++next_expected_value;
        }

        --expected_doc;
        --expected_value;
      }
      ASSERT_EQ(kMaxDocs, docs_count);

      // seek before the first document
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      it->seek(expected_doc);
      expected_doc = min_doc;
      expected_value = expected_doc - 1;
      ASSERT_EQ(min_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward; ++i) {
        ASSERT_TRUE(it->next());
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());

        auto next_expected_value_str = std::to_string(next_expected_value);
        if (next_expected_value % 2) {
          next_expected_value_str.append(kColumnName.data(),
                                         kColumnName.size());
        }

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value_str, actual_value_str);

        ++next_expected_doc;
        ++next_expected_value;
      }
    }

    // seek backwards + next(x5)
    {
      const size_t steps_forward = 5;

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = kMaxDocs;
      irs::doc_id_t expected_value = expected_doc - 1;

      ASSERT_EQ(expected_doc, it->seek(expected_doc));
      auto actual_value_str =
        irs::ToString<std::string_view>(payload->value.data());
      auto expected_value_str = std::to_string(expected_value);

      if (expected_value % 2) {
        expected_value_str.append(kColumnName.data(), kColumnName.size());
      }

      ASSERT_EQ(expected_value_str, actual_value_str);

      auto next_expected_doc = expected_doc + 1;
      auto next_expected_value = expected_value + 1;
      for (size_t i = 0; i < steps_forward && it->next(); ++i) {
        actual_value_str =
          irs::ToString<std::string_view>(payload->value.data());
        auto next_expected_value_str = std::to_string(next_expected_value);

        if (next_expected_value % 2) {
          next_expected_value_str.append(kColumnName.data(),
                                         kColumnName.size());
        }

        ASSERT_EQ(next_expected_doc, it->value());
        ASSERT_EQ(next_expected_value_str, actual_value_str);

        ++next_expected_doc;
        ++next_expected_value;
      }

      --expected_doc;
      ASSERT_EQ(irs::doc_limits::eof(), it->seek(expected_doc));
    }

    // visit values (cached)
    {
      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      auto visitor = [&expected_value, &expected_doc](
                       irs::doc_id_t actual_doc,
                       const irs::bytes_view& actual_data) {
        if (expected_doc != actual_doc) {
          return false;
        }

        const auto actual_str =
          irs::ToString<std::string_view>(actual_data.data());

        auto expected_str = std::to_string(expected_value);
        if (expected_value % 2) {
          expected_str.append(kColumnName.data(), kColumnName.size());
        }

        if (expected_str != actual_str) {
          return false;
        }

        ++expected_doc;
        ++expected_value;
        return true;
      };

      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column, segment.column(meta->id()));
      ASSERT_TRUE(Visit(*column, visitor));
    }

    // iterate over column (cached)
    {
      auto column = segment.column(kColumnName);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      irs::doc_id_t expected_doc = (irs::doc_limits::min)();
      irs::doc_id_t expected_value = 0;
      for (; it->next();) {
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        auto expected_value_str = std::to_string(expected_value);

        if (expected_value % 2) {
          expected_value_str.append(kColumnName.data(), kColumnName.size());
        }

        ASSERT_EQ(expected_doc, it->value());
        ASSERT_EQ(expected_value_str, actual_str_value);

        ++expected_doc;
        ++expected_value;
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(kMaxDocs, expected_value);
    }
  }
}

TEST_P(IndexColumnTestCase, read_write_doc_attributes_big) {
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  struct CsvDocTemplateT : public tests::CsvDocGenerator::DocTemplate {
    void init() final {
      clear();
      reserve(2);
      insert(std::make_shared<tests::StringField>("id"));
      insert(std::make_shared<tests::StringField>("label"));
    }

    void value(size_t idx, const std::string_view& value) final {
      switch (idx) {
        case 0:
          indexed.get<tests::StringField>("id")->value(value);
          break;
        case 1:
          indexed.get<tests::StringField>("label")->value(value);
      }
    }
  };

  CsvDocTemplateT csv_doc_template;
  tests::CsvDocGenerator gen(resource("simple_two_column.csv"),
                             csv_doc_template);
  size_t docs_count = 0;

  // write attributes
  {
    auto writer =
      irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options);

    const tests::Document* doc;
    while ((doc = gen.next())) {
      ASSERT_TRUE(Insert(*writer, doc->indexed.end(), doc->indexed.end(),
                         doc->stored.begin(), doc->stored.end()));
      ++docs_count;
    }
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - visit (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(dir());
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    auto columns = segment.columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("id", columns->value().name());
    ASSERT_EQ(0, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("label", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_FALSE(columns->next());
    ASSERT_FALSE(columns->next());

    // check 'id' column
    {
      const std::string_view column_name = "id";
      auto* meta = segment.column(column_name);
      ASSERT_NE(nullptr, meta);

      // visit column (not cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          const auto actual_value = irs::ToString<std::string_view>(in.data());
          if (field->value() != actual_value) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // visit column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          const auto actual_value = irs::ToString<std::string_view>(in.data());
          if (field->value() != actual_value) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // iterate over column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        for (; it->next();) {
          ++expected_id;

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);
          ASSERT_NE(nullptr, field);

          const auto actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(expected_id, it->value());
          ASSERT_EQ(field->value(), actual_value_str);
        }

        ASSERT_FALSE(it->next());
        ASSERT_EQ(irs::doc_limits::eof(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
        ASSERT_EQ(docs_count, expected_id);
      }
    }

    // check 'label' column
    {
      const std::string_view column_name = "label";
      auto* meta = segment.column(column_name);
      ASSERT_NE(nullptr, meta);

      // visit column (not cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() != irs::ToString<std::string_view>(in.data())) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // visit column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() != irs::ToString<std::string_view>(in.data())) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // iterate over 'label' column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        for (; it->next();) {
          ++expected_id;

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);
          ASSERT_NE(nullptr, field);
          const auto actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(expected_id, it->value());
          ASSERT_EQ(field->value(), actual_value_str);
        }

        ASSERT_FALSE(it->next());
        ASSERT_EQ(irs::doc_limits::eof(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
        ASSERT_EQ(docs_count, expected_id);
      }
    }
  }

  // check inserted values:
  // - visit (not cached)
  // - iterate (not cached)
  // - visit (cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(dir());
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    auto columns = segment.columns();
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("id", columns->value().name());
    ASSERT_EQ(0, columns->value().id());
    ASSERT_TRUE(columns->next());
    ASSERT_EQ("label", columns->value().name());
    ASSERT_EQ(1, columns->value().id());
    ASSERT_FALSE(columns->next());
    ASSERT_FALSE(columns->next());

    // check 'id' column
    {
      const std::string_view column_name = "id";
      auto* meta = segment.column(column_name);
      ASSERT_NE(nullptr, meta);

      // visit column (not cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          const auto actual_value = irs::ToString<std::string_view>(in.data());
          if (field->value() != actual_value) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // iterate over column (not cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        for (; it->next();) {
          ++expected_id;

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);
          ASSERT_NE(nullptr, field);
          const auto actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(expected_id, it->value());
          ASSERT_EQ(field->value(), actual_value_str);
        }

        ASSERT_FALSE(it->next());
        ASSERT_EQ(irs::doc_limits::eof(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
        ASSERT_EQ(docs_count, expected_id);
      }

      // visit column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          const auto actual_value = irs::ToString<std::string_view>(in.data());
          if (field->value() != actual_value) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // iterate over column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        for (; it->next();) {
          ++expected_id;

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);
          ASSERT_NE(nullptr, field);
          const auto actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(expected_id, it->value());
          ASSERT_EQ(field->value(), actual_value_str);
        }

        ASSERT_FALSE(it->next());
        ASSERT_EQ(irs::doc_limits::eof(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
        ASSERT_EQ(docs_count, expected_id);
      }
    }

    // check 'label' column
    {
      const std::string_view column_name = "label";
      auto* meta = segment.column(column_name);
      ASSERT_NE(nullptr, meta);

      // visit column (not cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() != irs::ToString<std::string_view>(in.data())) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // iterate over 'label' column (not cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        for (; it->next();) {
          ++expected_id;

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);
          ASSERT_NE(nullptr, field);
          const auto actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(expected_id, it->value());
          ASSERT_EQ(field->value(), actual_value_str);
        }

        ASSERT_FALSE(it->next());
        ASSERT_EQ(irs::doc_limits::eof(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
        ASSERT_EQ(docs_count, expected_id);
      }

      // visit column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id, const irs::bytes_view& in) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() != irs::ToString<std::string_view>(in.data())) {
            return false;
          }

          return true;
        };

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column, segment.column(meta->id()));
        ASSERT_TRUE(Visit(*column, visitor));
      }

      // iterate over 'label' column (cached)
      {
        gen.reset();
        irs::doc_id_t expected_id = 0;

        auto column = segment.column(column_name);
        ASSERT_NE(nullptr, column);
        auto it = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, it);

        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);

        for (; it->next();) {
          ++expected_id;

          auto* doc = gen.next();
          auto* field = doc->stored.get<tests::StringField>(column_name);
          ASSERT_NE(nullptr, field);
          const auto actual_value_str =
            irs::ToString<std::string_view>(payload->value.data());

          ASSERT_EQ(expected_id, it->value());
          ASSERT_EQ(field->value(), actual_value_str);
        }

        ASSERT_FALSE(it->next());
        ASSERT_EQ(irs::doc_limits::eof(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
        ASSERT_EQ(docs_count, expected_id);
      }
    }
  }
}

TEST_P(IndexColumnTestCase, read_write_doc_attributes) {
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // write documents
  {
    auto writer =
      irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options);

    // attributes only
    ASSERT_TRUE(Insert(*writer, doc1->indexed.end(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.end(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.end(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.end(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check inserted values:
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = *(reader.begin());

    // read attribute from invalid column
    {
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
    }

    // check number of values in the column
    {
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, column->size());
    }

    // iterate over 'name' column (cached)
    {
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<irs::doc_id_t, std::string_view>> expected_values =
        {{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(i, expected_values.size());
    }

    // iterate over 'prefix' column (cached)
    {
      auto column = segment.column("prefix");
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<irs::doc_id_t, std::string_view>> expected_values =
        {{1, "abcd"}, {4, "abcde"}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(i, expected_values.size());
    }
  }

  // check inserted values:
  // - iterate (not cached)
  // - iterate (cached)
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = *(reader.begin());

    // read attribute from invalid column
    {
      ASSERT_EQ(nullptr, segment.column("invalid_column"));
    }

    {
      // iterate over 'name' column (not cached)
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<irs::doc_id_t, std::string_view>> expected_values =
        {{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(i, expected_values.size());
    }

    // iterate over 'name' column (cached)
    {
      auto column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<irs::doc_id_t, std::string_view>> expected_values =
        {{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(i, expected_values.size());
    }

    {
      // iterate over 'prefix' column (not cached)
      auto column = segment.column("prefix");
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<irs::doc_id_t, std::string_view>> expected_values =
        {{1, "abcd"}, {4, "abcde"}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(i, expected_values.size());
    }

    // iterate over 'prefix' column (cached)
    {
      auto column = segment.column("prefix");
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<irs::doc_id_t, std::string_view>> expected_values =
        {{1, "abcd"}, {4, "abcde"}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
      ASSERT_EQ(i, expected_values.size());
    }
  }
}

TEST_P(IndexColumnTestCase, read_empty_doc_attributes) {
  irs::IndexWriterOptions options;
  options.column_info = [](const std::string_view&) {
    return irs::ColumnInfo{irs::Type<irs::compression::Lz4>::get(),
                           irs::compression::Options{}, true};
  };

  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // write documents without attributes
  {
    auto writer =
      irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options);

    // fields only
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size());
  auto& segment = *(reader.begin());

  const auto* column = segment.column("name");
  ASSERT_EQ(nullptr, column);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  index_column_test, IndexColumnTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  IndexColumnTestCase::to_string);
