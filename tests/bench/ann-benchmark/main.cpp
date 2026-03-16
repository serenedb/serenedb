#include <absl/strings/str_format.h>
#include <absl/strings/substitute.h>
#include <faiss/utils/distances.h>
#include <simdjson.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/formats/hnsw_index.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_writer.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/compression.hpp>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "app/options/argument_parser.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/assert.h"
#include "basics/resource_manager.hpp"
#include "basics/system-compiler.h"
#include "simdjson/dom/parser.h"

class Timer {
 public:
  void Start() { _start = std::chrono::system_clock::now(); }

  auto Finish() {
    auto finish = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration<double>(finish - _start);
    return elapsed;
  }

 private:
  std::chrono::time_point<std::chrono::system_clock> _start;
};

class VectorIndex {
 public:
  static constexpr std::string kIdField = "label";
  static constexpr std::string kEmbeddingsField = "vector";

  struct EmbeddingsField {
    std::vector<float> data;
    std::string_view Name() const { return kEmbeddingsField; }
    bool Write(irs::DataOutput& out) const {
      out.WriteBytes(reinterpret_cast<const irs::byte_type*>(data.data()),
                     data.size() * sizeof(float));
      return true;
    }
  };

  struct IdField {
    int64_t data;
    std::string_view Name() const { return kIdField; }
    bool Write(irs::DataOutput& out) const {
      out.WriteBytes(reinterpret_cast<const irs::byte_type*>(&data),
                     sizeof(data));
      return true;
    }
  };

  VectorIndex(std::string_view index_dir, int dim, size_t max_segment_size)
    : _max_segment_size{max_segment_size} {
    _dir = std::make_shared<irs::MMapDirectory>(index_dir);

    irs::IndexWriterOptions writer_options;
    writer_options.column_info = [&, d = dim](std::string_view name) {
      if (name == kEmbeddingsField) {
        return irs::ColumnInfo{
          .compression = irs::Type<irs::compression::None>::get(),
          .options = {},
          .track_prev_doc = false,
          .value_type = irs::ValueType::VectorF32,
          .hnsw_info =
            irs::HNSWInfo{
              .d = d,
            },
        };
      }
      if (name == kIdField) {
        return irs::ColumnInfo{
          .compression = irs::Type<irs::compression::None>::get(),
          .options = {},
          .track_prev_doc = false,
          .value_type = irs::ValueType::I64,
        };
      }
      return irs::ColumnInfo{
        .compression = irs::Type<irs::compression::None>::get(),
        .options = {},
        .track_prev_doc = false,
      };
    };
    _writer = irs::IndexWriter::Make(*_dir, irs::formats::Get("1_5simd"),
                                     irs::OpenMode::kOmCreate, writer_options);
  }

  auto GetWriter() const { return _writer; }

  auto GetDirectory() const { return _dir; }

  void Begin() {
    _txn = _writer->GetBatch();
    SDB_ASSERT(_txn.Valid());
  }

  void Commit() {
    _writtern_docs = 0;
    _current_doc.reset();
    _txn.Commit();
    _writer->Commit();
  }

  void InsertRow(int64_t id, std::vector<float> emb) {
    if (!_current_doc || _writtern_docs == _max_segment_size) [[unlikely]] {
      _writtern_docs = 0;
      _current_doc.reset();
      SDB_ASSERT(_txn.Valid());
      _txn.Commit();
      _current_doc = std::make_unique<irs::IndexWriter::Document>(
        _txn.Insert(false, _max_segment_size));
    }
    SDB_ASSERT(_current_doc);
    SDB_ASSERT(*_current_doc);
    _writtern_docs++;
    IdField id_field{.data = id};
    EmbeddingsField emb_field{.data = std::move(emb)};
    _current_doc->Insert<irs::Action::STORE>(id_field);
    _current_doc->Insert<irs::Action::STORE>(emb_field);
    _current_doc->NextDocument();
  }

 private:
  std::shared_ptr<irs::Directory> _dir;
  irs::IndexWriter::ptr _writer;
  std::unique_ptr<irs::IndexWriter::Document> _current_doc = nullptr;
  size_t _writtern_docs = 0;
  irs::IndexWriter::Transaction _txn;
  size_t _max_segment_size;
};

int main(int argc, char** argv) {
  std::string label, embeddings;
  std::string emb_name;
  std::string queries_name;
  uint64_t segment_max_size = 0;
  uint64_t dim = 0;
  std::string index_dir;
  uint64_t top_k = 10;
  uint64_t ef_search = 64;

  sdb::options::ProgramOptions options(
    argv[0], "Usage: ann-benchmark [options]",
    "For more information use --help", argv[0]);

  options.addOption("--embeddings", "path to embeddings JSON file",
                    std::make_unique<sdb::options::StringParameter>(&emb_name));
  options.addOption(
    "--queries", "path to queries JSON file",
    std::make_unique<sdb::options::StringParameter>(&queries_name));
  options.addOption(
    "--segment-max-size", "maximum number of documents per segment",
    std::make_unique<sdb::options::UInt64Parameter>(&segment_max_size));
  options.addOption("--dim", "vector dimension",
                    std::make_unique<sdb::options::UInt64Parameter>(&dim));
  options.addOption(
    "--index-dir", "directory for iresearch index",
    std::make_unique<sdb::options::StringParameter>(&index_dir));
  options.addOption("--top-k",
                    "number of nearest neighbors to retrieve (default: 10)",
                    std::make_unique<sdb::options::UInt64Parameter>(&top_k));
  options.addOption(
    "--ef-search", "efSearch parameter for HNSW (default: 64)",
    std::make_unique<sdb::options::UInt64Parameter>(&ef_search));

  sdb::options::ArgumentParser parser(&options);

  std::string help = parser.helpSection(argc, argv);
  if (!help.empty()) {
    options.printHelp(help);
    return EXIT_SUCCESS;
  }

  if (!parser.parse(argc, argv)) {
    return options.processingResult().exitCodeOrFailure();
  }

  if (emb_name.empty() || queries_name.empty() || segment_max_size == 0 ||
      dim == 0 || index_dir.empty()) {
    options.printUsage();
    return EXIT_FAILURE;
  }

  std::filesystem::path emb_path{emb_name};
  SDB_ASSERT(emb_path.extension().string() == ".json",
             "Expected json, but got ", emb_path.extension().string());
  std::filesystem::path queries_path{queries_name};
  SDB_ASSERT(queries_path.extension().string() == ".json",
             "Expected json, but got ", queries_path.string());

  irs::formats::Init();
  irs::compression::Init();

  VectorIndex index{index_dir, static_cast<int>(dim), segment_max_size};
  Timer t;
  t.Start();
  index.Begin();

  simdjson::dom::parser json_parser;
  auto doc = json_parser.load(emb_name);
  for (auto row : doc.get_array()) {
    auto idx = row.at_key(VectorIndex::kIdField).get_int64().value();
    std::vector<float> emb;
    emb.reserve(dim);

    auto emb_json =
      row.at_key(VectorIndex::kEmbeddingsField).get_array().value();
    for (auto value : emb_json) {
      emb.push_back(value.get_double().value());
    }
    SDB_ASSERT(emb.size() == dim);

    index.InsertRow(idx, std::move(emb));
  }
  index.Commit();
  std::cout << "Insertation elapsed time: " << t.Finish() << std::endl;

  // --- Query phase ---
  irs::IndexReaderOptions reader_opts;
  auto reader =
    irs::DirectoryReader{*index.GetDirectory(), nullptr, reader_opts};

  faiss::SearchParametersHNSW search_params;
  search_params.efSearch = static_cast<int>(ef_search);

  auto queries_doc = json_parser.load(queries_name);

  size_t query_count = 0;
  size_t hits = 0;
  double total_query_s = 0.0;

  for (auto query_row : queries_doc.get_array()) {
    std::vector<float> query_vec;
    query_vec.reserve(dim);
    int64_t correct_label =
      query_row.at_key(VectorIndex::kIdField).get_int64().value();
    for (auto v :
         query_row.at_key(VectorIndex::kEmbeddingsField).get_array().value()) {
      query_vec.push_back(static_cast<float>(v.get_double().value()));
    }
    SDB_ASSERT(query_vec.size() == dim);

    std::vector<float> distances(top_k, 0.0f);
    std::vector<uint64_t> packed_ids(top_k);

    irs::HNSWSearchInfo info{
      reinterpret_cast<const irs::byte_type*>(query_vec.data()),
      static_cast<size_t>(top_k),
      search_params,
    };

    t.Start();
    reader.Search(VectorIndex::kEmbeddingsField, info, distances.data(),
                  reinterpret_cast<int64_t*>(packed_ids.data()));
    total_query_s += t.Finish().count();

    auto [seg_id, local_doc] = irs::UnpackSegmentWithDoc(packed_ids[0]);
    const auto& segment = reader[seg_id];
    const auto* label_col = segment.column(VectorIndex::kIdField);
    int64_t result_label = -1;
    SDB_ASSERT(label_col);
    auto iter = label_col->iterator(irs::ColumnHint::Normal);
    SDB_ASSERT(iter && local_doc == iter->seek(local_doc));
    auto* payload = irs::get<irs::PayAttr>(*iter);
    SDB_ASSERT(payload);
    result_label = *reinterpret_cast<const int64_t*>(payload->value.data());

    hits += (result_label == correct_label);
    ++query_count;
  }

  const double avg_ms = total_query_s / query_count * 1000.0;
  absl::PrintF("Queried %v vectors, top_k = %v, efSearch = %v\n", query_count,
               top_k, ef_search);
  absl::PrintF("Average query latency: %v ms, QPS: %v, Recall: %v\n", avg_ms,
               (query_count / total_query_s),
               (static_cast<double>(hits) / query_count));
}
