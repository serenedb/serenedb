#include <nanobind/ndarray.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/compression.hpp>
#include <optional>

#include "basics/resource_manager.hpp"

static constexpr std::string_view kVectorField = "vector";
static constexpr std::string_view kIdField = "label";

static void ensure_init() {
  static bool done = false;
  if (!done) {
    irs::formats::Init();
    irs::compression::Init();
    done = true;
  }
}

class VectorIndex {
 public:
  VectorIndex(const std::string& index_dir, int dim, size_t segment_max_size,
              int m = 32, int ef_construction = 40)
    : _dim(dim), _max_seg(segment_max_size) {
    ensure_init();
    _dir = std::make_shared<irs::MMapDirectory>(index_dir);

    irs::IndexWriterOptions opts;
    opts.column_info = [dim, m, ef_construction](std::string_view name) -> irs::ColumnInfo {
      if (name == kVectorField) {
        return {
          .compression = irs::Type<irs::compression::None>::get(),
          .track_prev_doc = false,
          .value_type = irs::ValueType::VectorF32,
          .hnsw_info = irs::HNSWInfo{.d = dim, .m = m, .ef_construction = ef_construction},
        };
      }
      if (name == kIdField) {
        return {
          .compression = irs::Type<irs::compression::None>::get(),
          .track_prev_doc = false,
          .value_type = irs::ValueType::I64,
        };
      }
      return {.compression = irs::Type<irs::compression::None>::get()};
    };

    _writer = irs::IndexWriter::Make(*_dir, irs::formats::Get("1_5simd"),
                                     irs::OpenMode::kOmCreate, opts);
    _txn = _writer->GetBatch();
  }

  using NdArrayF32 =
    nanobind::ndarray<float, nanobind::numpy, nanobind::ndim<1>,
                      nanobind::device::cpu>;

  // Insert a single vector with an associated integer id.
  // vec must be a 1-D float32 numpy array of length `dim`.
  void insert(int64_t id, NdArrayF32 vec) {
    if (vec.shape(0) != static_cast<size_t>(_dim)) {
      throw std::runtime_error("vector length mismatch");
    }

    if (!_doc || _written == _max_seg) {
      _written = 0;
      _doc.reset();
      _txn.Commit();
      _doc = std::make_unique<irs::IndexWriter::Document>(
        _txn.Insert(false, _max_seg));
    }

    const float* data = vec.data();

    struct IdField {
      int64_t data;
      std::string_view Name() const { return kIdField; }
      bool Write(irs::DataOutput& out) const {
        out.WriteBytes(reinterpret_cast<const irs::byte_type*>(&data),
                       sizeof(data));
        return true;
      }
    };

    struct VecField {
      const float* data;
      size_t dim;
      std::string_view Name() const { return kVectorField; }
      bool Write(irs::DataOutput& out) const {
        out.WriteBytes(reinterpret_cast<const irs::byte_type*>(data),
                       dim * sizeof(float));
        return true;
      }
    };

    _doc->Insert<irs::Action::STORE>(IdField{id});
    _doc->Insert<irs::Action::STORE>(VecField{data, static_cast<size_t>(_dim)});
    _doc->NextDocument();
    ++_written;
  }

  void commit() {
    _written = 0;
    _doc.reset();
    _txn.Commit();
    _writer->Commit();
    _reader.reset();
  }

  // Search: returns list of (id, distance) pairs
  std::vector<std::pair<int64_t, float>> search(NdArrayF32 query, int top_k,
                                                int ef_search = 64) {
    if (query.shape(0) != static_cast<size_t>(_dim)) {
      throw std::runtime_error("query length mismatch");
    }
    if (!_reader) {
      _reader.emplace(*_dir, nullptr, _reader_opts);
    }

    faiss::SearchParametersHNSW params;
    params.efSearch = ef_search;

    irs::HNSWSearchInfo info{
      reinterpret_cast<const irs::byte_type*>(query.data()),
      static_cast<size_t>(top_k),
      params,
    };

    std::vector<float> distances(top_k, 0.0f);
    std::vector<uint64_t> packed_ids(top_k, irs::doc_limits::eof());

    _reader->Search(kVectorField, info, distances.data(),
                    reinterpret_cast<int64_t*>(packed_ids.data()));

    std::vector<std::pair<int64_t, float>> results;
    results.reserve(top_k);

    for (int i = 0; i < top_k; ++i) {
      if (packed_ids[i] == irs::doc_limits::eof()) {
        break;
      }

      auto [seg_id, local_doc] = irs::UnpackSegmentWithDoc(packed_ids[i]);
      const auto& seg = (*_reader)[seg_id];
      const auto* col = seg.column(kIdField);
      if (!col) {
        continue;
      }

      auto it = col->iterator(irs::ColumnHint::Normal);
      if (!it || local_doc != it->seek(local_doc)) {
        continue;
      }

      auto* pay = irs::get<irs::PayAttr>(*it);
      if (!pay) {
        continue;
      }

      int64_t label = *reinterpret_cast<const int64_t*>(pay->value.data());
      results.emplace_back(label, distances[i]);
    }

    return results;
  }

 private:
  int _dim;
  size_t _max_seg;
  size_t _written = 0;
  std::shared_ptr<irs::Directory> _dir;
  irs::IndexWriter::ptr _writer;
  irs::IndexWriter::Transaction _txn;
  std::unique_ptr<irs::IndexWriter::Document> _doc;
  irs::IndexReaderOptions _reader_opts;
  std::optional<irs::DirectoryReader> _reader;
};

NB_MODULE(pyiresearch, m) {
  nanobind::class_<VectorIndex>(m, "VectorIndex")
    .def(nanobind::init<const std::string&, int, size_t, int, int>(),
         nanobind::arg("index_dir"), nanobind::arg("dim"),
         nanobind::arg("segment_max_size") = 10000,
         nanobind::arg("m") = 32,
         nanobind::arg("ef_construction") = 40)
    .def("insert", &VectorIndex::insert, nanobind::arg("id"),
         nanobind::arg("vector"))
    .def("commit", &VectorIndex::commit)
    .def("search", &VectorIndex::search, nanobind::arg("query"),
         nanobind::arg("top_k"), nanobind::arg("ef_search") = 64);
}
