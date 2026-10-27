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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <s2/s2latlng.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>
#include <exception>
#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/analysis/sql_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/token_sink.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/inverter/fields_inverter.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/scorers/vector_similarity_scorer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/cpu_features.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

constexpr irs::field_id kTextField = 1;
constexpr irs::field_id kGeoField = 2;
constexpr irs::field_id kNormColumn = 3;
constexpr irs::field_id kVectorField = 4;

struct Document {
  std::string_view text;
  std::string_view geometry;
};

constexpr Document kDocuments[] = {
  {.text = "Москва quick brown fox",
   .geometry = R"({"type":"Point","coordinates":[37.6173,55.7558]})"},
  {.text = "Москва brown quick fox brown fox",
   .geometry = R"({"type":"Point","coordinates":[30.3158,59.9391]})"},
};

void IndexField(const irs::IndexWriter::Document& doc, irs::field_id field,
                irs::IndexFeatures features,
                irs::analysis::Tokenizer& tokenizer, std::string_view text) {
  const duckdb::string_t value{text.data(), static_cast<uint32_t>(text.size())};
  if (!doc.WithTokens(
        field, features, nullptr,
        [&](irs::FieldInverter& inverter, irs::TokenSink& sink) {
          inverter.Configure(tokenizer.Traits());
          if (!tokenizer.Fill(value, doc.DocId(), sink, {inverter.Layout()})) {
            throw std::runtime_error("tokenizer rejected the document");
          }
        })) {
    throw std::runtime_error("could not index the field");
  }
}

irs::DirectoryReader BuildIndex(irs::Directory& dir,
                                duckdb::DatabaseInstance& db,
                                irs::analysis::Tokenizer& text,
                                irs::analysis::Tokenizer& geo) {
  irs::IndexWriterOptions options;
  options.db = &db;
  options.reader_options.db = &db;
  options.norm_column_id = [](irs::field_id) { return kNormColumn; };
  options.column_options = [](irs::field_id id) {
    irs::ColumnOptions column;
    if (id == kVectorField) {
      column.ann_info = irs::AnnInfo{
        .kind = irs::AnnKind::Hnsw,
        .centroids_id = kVectorField,
        .postings_id = kVectorField,
        .d = 2,
        .metric = irs::VectorMetric::L2Sqr,
        .quant = {.kind = irs::VectorQuantization::None},
        .m = 8,
        .ef_construction = 64,
      };
    }
    return column;
  };
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
  auto batch = writer->GetBatch();
  for (const auto& record : kDocuments) {
    auto doc = batch.Insert();
    IndexField(doc, kTextField,
               irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                 irs::IndexFeatures::Norm,
               text, record.text);
    IndexField(doc, kGeoField, irs::IndexFeatures::None, geo, record.geometry);

    auto* columns = doc.GetColWriter();
    if (!columns) {
      throw std::runtime_error("stored columns are unavailable");
    }
    auto& column = columns->OpenColumn(kGeoField, duckdb::LogicalType::BLOB);
    duckdb::Vector values{duckdb::LogicalType::BLOB, 1};
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(values)[0] =
      duckdb::StringVector::AddStringOrBlob(values, record.geometry.data(),
                                            record.geometry.size());
    duckdb::FlatVector::ValidityMutable(values).SetAllValid(1);
    column.Append(doc.DocId() - irs::doc_limits::min(), values, 1);

    const auto vector_type =
      duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, 2);
    auto& vector_column = columns->OpenColumn(kVectorField, vector_type);
    duckdb::Vector vector{vector_type, 1};
    auto& child = duckdb::ArrayVector::GetChildMutable(vector);
    auto* data = duckdb::FlatVector::GetDataMutable<float>(child);
    data[0] = doc.DocId() == irs::doc_limits::min() ? 0.f : 4.f;
    data[1] = 0.f;
    duckdb::FlatVector::ValidityMutable(vector).SetAllValid(1);
    duckdb::FlatVector::ValidityMutable(child).SetAllValid(2);
    duckdb::FlatVector::SetSize(vector, 1);
    vector_column.Append(doc.DocId() - irs::doc_limits::min(), vector, 1);
  }
  if (!batch.Commit()) {
    throw std::runtime_error("could not commit the documents");
  }
  writer->RefreshCommit();
  return writer->GetSnapshot();
}

uint64_t CountMatches(const irs::DirectoryReader& reader,
                      const irs::Filter& filter) {
  uint64_t count = 0;
  for (auto& segment : reader) {
    auto query = irs::PrepareMasked(filter, segment, {});
    if (!query) {
      continue;
    }
    if (auto plan = query->PlanCount({})) {
      count += plan->Run(irs::doc_limits::min(), irs::doc_limits::eof());
    }
  }
  return count;
}

irs::Filter::ptr ParseQuery(std::string_view query,
                            irs::analysis::Tokenizer& tokenizer) {
  auto root = std::make_unique<irs::BooleanFilter>();
  irs::ParserContext context{*root, kTextField, tokenizer};
  if (!irs::ParseQuery(context, query) || !root->Valid()) {
    throw std::runtime_error(std::string{context.error_message});
  }
  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = true});
  return filter;
}

void Run(duckdb::DatabaseInstance& db) {
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto sql = irs::analysis::SqlTokenizer::Make(
    {.expression = "replace(trim(input), '|', '')"});
  absl::Cleanup unbind = [&] { sql->Unbind(); };
  sql->Bind(*connection->context);
  irs::ValueAnalyzer analyzer;
  irs::ValueTokens tokens;
  if (!analyzer.Analyze(*sql, duckdb::string_t{"  |fox|  "}, tokens) ||
      tokens.terms().size() != 1 ||
      std::string_view{tokens.terms()[0].GetData(),
                       tokens.terms()[0].GetSize()} != "fox") {
    throw std::runtime_error("SQL tokenizer returned wrong results");
  }
  auto timezone = connection->Query("SELECT current_setting('TimeZone')");
  timezone->ThrowIfError();
  auto timezone_chunk = timezone->Fetch();
  if (!timezone_chunk || timezone_chunk->size() != 1 ||
      timezone_chunk->GetValue(0, 0).GetValue<std::string>() != "UTC") {
    throw std::runtime_error("ICU did not apply the initialization settings");
  }
  auto collation = connection->Query("SELECT 'a' < 'b' COLLATE en");
  collation->ThrowIfError();
  auto chunk = collation->Fetch();
  if (!chunk || chunk->size() != 1 || !chunk->GetValue(0, 0).GetValue<bool>()) {
    throw std::runtime_error("ICU collation returned wrong results");
  }

  irs::MemoryDirectory dir;
  auto text =
    irs::analysis::TextTokenizer::Make(irs::analysis::TextTokenizer::Options{});
  auto geo = irs::analysis::GeoJsonTokenizer::Make(
    irs::analysis::GeoJsonTokenizer::Options{});
  auto reader = BuildIndex(dir, db, *text, *geo);
  if (reader.docs_count() != 2) {
    throw std::runtime_error("snapshot did not retain both documents");
  }
  irs::ByVectorSimilarity nearest;
  *nearest.mutable_field_id() = kVectorField;
  auto& vector_options = *nearest.mutable_options();
  vector_options.query = {3.f, 0.f};
  vector_options.centroids_id = kVectorField;
  vector_options.postings_id = kVectorField;
  vector_options.metric = irs::VectorMetric::L2Sqr;
  vector_options.quant = irs::VectorQuantization::None;
  vector_options.nprobe = 16;
  vector_options.ef_search = 16;
  irs::VectorSimilarityScorer vector_scorer;
  std::array<irs::ScoreDoc, 2> vector_hits;
  if (irs::ExecuteTopK(reader, nearest, vector_scorer, vector_hits.size(),
                       false, std::span{vector_hits}) != vector_hits.size() ||
      vector_hits[0].doc != irs::doc_limits::min() + 1 ||
      vector_hits[1].doc != irs::doc_limits::min() ||
      vector_hits[0].score != -1.f || vector_hits[1].score != -9.f) {
    throw std::runtime_error("vector query returned wrong neighbors");
  }
  auto phrase = ParseQuery(R"("quick brown")", *text);
  auto unicode = ParseQuery("МОСКВА", *text);
  if (CountMatches(reader, *phrase) != 1 ||
      CountMatches(reader, *unicode) != 2) {
    throw std::runtime_error("phrase or Unicode query returned wrong results");
  }

  irs::ByPrefix prefix;
  *prefix.mutable_field_id() = kTextField;
  prefix.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"моск"});
  if (CountMatches(reader, prefix) != 2) {
    throw std::runtime_error("prefix query returned wrong results");
  }

  auto scorer = irs::BM25::Make(irs::BM25::Options{});
  std::array<irs::ScoreDoc, 2> hits;
  const auto total = irs::ExecuteTopK(reader, *unicode, *scorer, hits.size(),
                                      false, std::span{hits});
  if (total != hits.size()) {
    throw std::runtime_error("BM25 query returned wrong results");
  }
  for (const auto& hit : hits) {
    if (!std::isfinite(hit.score) || hit.score <= 0) {
      throw std::runtime_error("BM25 query returned an invalid score");
    }
  }
  if (hits[0].doc != irs::doc_limits::min() || hits[0].score <= hits[1].score) {
    throw std::runtime_error("BM25 did not use document length norms");
  }

  irs::GeoDistanceFilter nearby;
  *nearby.mutable_field_id() = kGeoField;
  auto& options = *nearby.mutable_options();
  irs::analysis::GeoTokenizer::Cast(*geo).prepare(options);
  options.store_field_id = kGeoField;
  options.origin = S2LatLng::FromDegrees(55.7558, 37.6173).ToPoint();
  options.range.max_type = irs::BoundType::Inclusive;
  options.range.max = 1000;
  if (CountMatches(reader, nearby) != 1) {
    throw std::runtime_error("geo distance query returned wrong results");
  }
}

}  // namespace

int main(int, char* argv[]) {
  auto& engine = irs::DuckDBEngine::Instance();
  absl::Cleanup cleanup = [&] {
    engine.CloseDatabases();
    engine.Shutdown();
  };
  try {
    irs::CheckCpuFeatures(argv[0]);
    engine.Initialize([](duckdb::DBConfig& config) {
      config.SetOptionByName("autoinstall_known_extensions",
                             duckdb::Value::BOOLEAN(false));
      config.SetOptionByName("TimeZone", duckdb::Value{"UTC"});
    });
    const auto& config = duckdb::DBConfig::GetConfig(engine.instance());
    if (!config.options.unrecognized_options.empty()) {
      throw std::runtime_error("engine has unrecognized configuration options");
    }
    irs::InitOptimizeRules();
    Run(engine.instance());
    absl::PrintF(
      "Native IResearch SQL, ICU, text, BM25, vector and geo queries "
      "passed.\n");
    return 0;
  } catch (const std::exception& e) {
    absl::FPrintF(stderr, "%s\n", e.what());
    return 1;
  }
}
