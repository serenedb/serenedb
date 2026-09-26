////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string_view>
#include <tuple>
#include <vector>

#include "connector/functions/ai/common.h"
#include "query/config.h"

namespace sdb::connector::ai {
namespace {

using Embeddings = std::vector<std::vector<float>>;

constinit SettingRef gEmbeddingBatch{"sdb_ai_embedding_max_batch_size"};

struct EmbeddingBindData final : public AIFunctionData {
  uint32_t dimensions = 0;
  uint32_t max_batch = 0;
  bool similarity = false;

  std::unique_ptr<AIWork> Start(duckdb::DataChunk& args) const final;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<EmbeddingBindData>(*this);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& o = other.Cast<EmbeddingBindData>();
    return endpoint == o.endpoint && dimensions == o.dimensions &&
           max_batch == o.max_batch && similarity == o.similarity;
  }
};

Embeddings ParseEmbeddings(const EmbeddingBindData& bind, std::string_view body,
                           size_t expected) {
  const std::string_view fn = bind.endpoint.fn;
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(body.data(), body.size()).get(doc) != simdjson::SUCCESS) {
    ThrowBadReply(fn, "response is not valid JSON", body);
  }
  simdjson::dom::array data;
  if (doc["data"].get_array().get(data) != simdjson::SUCCESS) {
    ThrowBadReply(fn, "response has no 'data' array", body);
  }
  if (data.size() != expected) {
    ThrowBadReply(fn,
                  absl::StrCat("response has ", data.size(),
                               " embeddings, expected ", expected),
                  body);
  }

  Embeddings embeddings(expected);
  std::vector<bool> seen(expected);
  size_t position = 0;
  for (auto item : data) {
    uint64_t index = position++;
    std::ignore = item["index"].get_uint64().get(index);
    if (index >= expected || seen[index]) {
      ThrowBadReply(fn,
                    absl::StrCat("response 'data[", position - 1, "].index' ",
                                 index, " is out of range or repeated"),
                    body);
    }
    seen[index] = true;
    simdjson::dom::array values;
    if (item["embedding"].get_array().get(values) != simdjson::SUCCESS) {
      ThrowBadReply(fn,
                    absl::StrCat("response 'data[", position - 1,
                                 "].embedding' is not an array"),
                    body);
    }
    if (values.size() == 0 ||
        (bind.dimensions != 0 && values.size() != bind.dimensions)) {
      ThrowBadReply(
        fn,
        absl::StrCat(
          "response 'data[", position - 1, "].embedding' has ", values.size(),
          " values, expected ",
          bind.dimensions == 0 ? "at least 1" : absl::StrCat(bind.dimensions)),
        body);
    }
    auto& embedding = embeddings[index];
    embedding.reserve(values.size());
    for (auto val : values) {
      double d = 0.0;
      if (val.get_double().get(d) != simdjson::SUCCESS) {
        ThrowBadReply(fn, "embedding contains a non-numeric value", body);
      }
      embedding.push_back(static_cast<float>(d));
    }
  }
  return embeddings;
}

std::string BuildBody(const EmbeddingBindData& bind,
                      std::span<const std::string_view> texts) {
  size_t total = 64 + bind.endpoint.model.size();
  for (const auto text : texts) {
    total += text.size() + 4;
  }
  simdjson::builder::string_builder builder(total);
  builder.start_object();
  builder.append_raw("\"model\":");
  builder.escape_and_append_with_quotes(bind.endpoint.model);
  if (bind.dimensions != 0) {
    builder.append_comma();
    builder.append_raw("\"dimensions\":");
    builder.append(bind.dimensions);
  }
  builder.append_comma();
  builder.append_raw("\"input\":");
  builder.start_array();
  for (size_t i = 0; i != texts.size(); ++i) {
    if (i != 0) {
      builder.append_comma();
    }
    builder.escape_and_append_with_quotes(texts[i]);
  }
  builder.end_array();
  builder.end_object();
  return std::string{builder.view().value()};
}

class EmbeddingWork : public BatchWork {
 public:
  EmbeddingWork(const EmbeddingBindData& bind, duckdb::Vector texts,
                duckdb::idx_t count)
    : _bind{bind},
      _texts{std::move(texts)},
      _count{count},
      _inputs{CollectInputs(_texts, count, true, true)},
      _embeddings(_inputs.texts.size()) {
    SDB_ASSERT(_bind.max_batch != 0);
    QueueBatches(_inputs.texts.size(), _bind.max_batch);
  }

  void Finish(duckdb::Vector& result) override {
    size_t total = 0;
    for (duckdb::idx_t i = 0; i < _count; i++) {
      if (const auto* e = Embedding(i)) {
        total += e->size();
      }
    }
    result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(result, 0);
    duckdb::ListVector::Reserve(result, total);
    auto* data = duckdb::FlatVector::GetDataMutable<float>(
      duckdb::ListVector::GetEntry(result));
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    auto& validity = duckdb::FlatVector::ValidityMutable(result);
    size_t offset = 0;
    for (duckdb::idx_t i = 0; i < _count; i++) {
      const auto* e = Embedding(i);
      if (e == nullptr) {
        entries[i] = {0, 0};
        validity.SetInvalid(i);
        continue;
      }
      entries[i] = {offset, e->size()};
      validity.SetValid(i);
      std::copy(e->begin(), e->end(), data + offset);
      offset += e->size();
    }
    duckdb::ListVector::SetListSize(result, offset);
  }

 protected:
  const std::vector<float>* Embedding(duckdb::idx_t row) const {
    const auto slot = _inputs.slots[row];
    if (slot == Inputs::kNone || _embeddings[slot].empty()) {
      return nullptr;
    }
    return &_embeddings[slot];
  }

 private:
  std::string Body(size_t begin, size_t size) const final {
    return BuildBody(_bind, std::span{_inputs.texts}.subspan(begin, size));
  }

  void Parse(Requester& requester, Response response, size_t begin,
             size_t size) final {
    if (const auto body = requester.Accept(std::move(response))) {
      std::ranges::move(ParseEmbeddings(_bind, *body, size),
                        _embeddings.begin() + begin);
    }
  }

  const EmbeddingBindData& _bind;
  duckdb::Vector _texts;
  duckdb::idx_t _count;
  Inputs _inputs;
  Embeddings _embeddings;
};

class SimilarityWork final : public EmbeddingWork {
 public:
  SimilarityWork(const EmbeddingBindData& bind, duckdb::DataChunk& args)
    : EmbeddingWork{bind, Pairs(args), 2 * args.size()}, _pairs{args.size()} {}

  void Finish(duckdb::Vector& result) final {
    result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto* out = duckdb::FlatVector::GetDataMutable<double>(result);
    auto& validity = duckdb::FlatVector::ValidityMutable(result);
    validity.SetAllInvalid(_pairs);
    for (duckdb::idx_t i = 0; i < _pairs; i++) {
      const auto* a = Embedding(2 * i);
      const auto* b = Embedding(2 * i + 1);
      if (a == nullptr || b == nullptr || a->size() != b->size()) {
        continue;
      }
      double dot = 0;
      double norm_a = 0;
      double norm_b = 0;
      for (size_t d = 0; d != a->size(); ++d) {
        const double x = (*a)[d];
        const double y = (*b)[d];
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
      }
      if (norm_a == 0 || norm_b == 0) {
        continue;
      }
      out[i] =
        std::clamp(dot / (std::sqrt(norm_a) * std::sqrt(norm_b)), -1.0, 1.0);
      validity.SetValid(i);
    }
  }

 private:
  static duckdb::Vector Pairs(duckdb::DataChunk& args) {
    duckdb::Vector pairs{duckdb::LogicalType::VARCHAR, 2 * args.size()};
    auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(pairs);
    auto& validity = duckdb::FlatVector::ValidityMutable(pairs);
    auto left = args.data[0].Values<duckdb::string_t>();
    auto right = args.data[1].Values<duckdb::string_t>();
    for (duckdb::idx_t i = 0; i < args.size(); i++) {
      const auto l = left[i];
      const auto r = right[i];
      if (l.IsValid() && r.IsValid() && l.GetValue().GetSize() != 0 &&
          r.GetValue().GetSize() != 0) {
        data[2 * i] = l.GetValue();
        data[2 * i + 1] = r.GetValue();
      } else {
        validity.SetInvalid(2 * i);
        validity.SetInvalid(2 * i + 1);
      }
    }
    return pairs;
  }

  duckdb::idx_t _pairs;
};

std::unique_ptr<AIWork> EmbeddingBindData::Start(
  duckdb::DataChunk& args) const {
  if (similarity) {
    return std::make_unique<SimilarityWork>(*this, args);
  }
  return std::make_unique<EmbeddingWork>(
    *this, duckdb::Vector::Ref(args.data[0]), args.size());
}

duckdb::unique_ptr<duckdb::FunctionData> EmbeddingBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  const auto fn = input.GetBoundFunction().GetName().GetIdentifierName();
  const auto options = std::span{input.GetArguments()}.last(3);
  const auto model = FoldString(context, *options[0], fn, "model");
  if (!model) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": \"model\" must not be NULL"));
  }
  const auto secret_name = FoldString(context, *options[1], fn, "secret_name");
  const auto dimensions = FoldArgument(context, *options[2], fn, "dimensions");

  auto bind = duckdb::make_uniq<EmbeddingBindData>();
  bind->endpoint = LoadEndpoint(context, fn, secret_name, kEmbeddingApi);
  bind->endpoint.model = *model;
  bind->similarity = fn == "ai_similarity";
  if (dimensions) {
    const auto n = dimensions->GetValue<int32_t>();
    if (n < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(fn, ": \"dimensions\" must be a non-negative integer"));
    }
    bind->dimensions = static_cast<uint32_t>(n);
  }
  bind->max_batch = gEmbeddingBatch.Int(context);
  RebindEachExecution(input);
  return bind;
}

duckdb::ScalarFunction MakeEmbeddingFunction(
  std::string_view name, duckdb::LogicalType type,
  std::span<const std::string_view> texts) {
  auto fn = MakeAIFunction(name, std::move(type), EmbeddingBind);
  auto& signature = fn.GetSignature();
  for (const auto text : texts) {
    signature.AddParameter(duckdb::Identifier{text},
                           duckdb::LogicalType::VARCHAR);
  }
  signature.AddParameter(duckdb::Identifier{"model"},
                         duckdb::LogicalType::VARCHAR);
  AddOption(signature, "secret_name", duckdb::LogicalType::VARCHAR);
  AddOption(signature, "dimensions", duckdb::LogicalType::INTEGER);
  return fn;
}

}  // namespace

void RegisterEmbeddingFunctions(duckdb::ExtensionLoader& loader) {
  constexpr std::string_view kEmbedTexts[] = {"text"};
  constexpr std::string_view kSimilarityTexts[] = {"text1", "text2"};
  loader.RegisterFunction(MakeEmbeddingFunction(
    "ai_embed", duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT),
    kEmbedTexts));
  loader.RegisterFunction(MakeEmbeddingFunction(
    "ai_similarity", duckdb::LogicalType::DOUBLE, kSimilarityTexts));
}

}  // namespace sdb::connector::ai
