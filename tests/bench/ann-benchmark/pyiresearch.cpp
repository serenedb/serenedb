#include <faiss/utils/distances.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/compression.hpp>
#include <optional>

#include "basics/resource_manager.hpp"

namespace py = pybind11;

// ── helpers ────────────────────────────────────────────────────────────────

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

// ── VectorIndex ────────────────────────────────────────────────────────────

class VectorIndex {
 public:
  VectorIndex(const std::string& index_dir, int dim, size_t segment_max_size)
    : _dim(dim), _max_seg(segment_max_size) {
    ensure_init();
    _dir = std::make_shared<irs::MMapDirectory>(index_dir);

    irs::IndexWriterOptions opts;
    opts.column_info = [dim](std::string_view name) -> irs::ColumnInfo {
      if (name == kVectorField) {
        return {
          .compression = irs::Type<irs::compression::None>::get(),
          .track_prev_doc = false,
          .value_type = irs::ValueType::VectorF32,
          .hnsw_info = irs::HNSWInfo{.d = dim},
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

  // Insert a single vector with an associated integer id.
  // vec must be a 1-D float32 numpy array of length `dim`.
  void insert(
    int64_t id,
    py::array_t<float, py::array::c_style | py::array::forcecast> vec) {
    auto buf = vec.request();
    if (buf.size != static_cast<ssize_t>(_dim)) {
      throw std::runtime_error("vector length mismatch");
    }

    if (!_doc || _written == _max_seg) {
      _written = 0;
      _doc.reset();
      _txn.Commit();
      _doc = std::make_unique<irs::IndexWriter::Document>(
        _txn.Insert(false, _max_seg));
    }

    const float* data = static_cast<const float*>(buf.ptr);

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
    _reader.reset();
  }

  void commit() {
    _written = 0;
    _doc.reset();
    _txn.Commit();
    _writer->Commit();
  }

  // Search: returns list of (id, distance) pairs
  std::vector<std::pair<int64_t, float>> search(
    py::array_t<float, py::array::c_style | py::array::forcecast> query,
    int top_k, int ef_search = 64) {
    auto buf = query.request();
    if (buf.size != static_cast<ssize_t>(_dim)) {
      throw std::runtime_error("query length mismatch");
    }
    if (!_reader) {
      _reader.emplace(*_dir, nullptr, _reader_opts);
    }

    faiss::SearchParametersHNSW params;
    params.efSearch = ef_search;

    irs::HNSWSearchInfo info{
      reinterpret_cast<const irs::byte_type*>(buf.ptr),
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

// ── module ─────────────────────────────────────────────────────────────────

PYBIND11_MODULE(pyiresearch, m) {
  m.doc() = "Python bindings for iresearch vector (HNSW) search";

  py::class_<VectorIndex>(m, "VectorIndex")
    .def(py::init<const std::string&, int, size_t>(), py::arg("index_dir"),
         py::arg("dim"), py::arg("segment_max_size"),
         "Create (or open) a vector index at *index_dir*.")
    .def("insert", &VectorIndex::insert, py::arg("id"), py::arg("vec"),
         "Insert a float32 vector with the given integer id.")
    .def("commit", &VectorIndex::commit, "Flush all pending inserts to disk.")
    .def("search", &VectorIndex::search, py::arg("query"), py::arg("top_k"),
         py::arg("ef_search") = 64,
         "Return list of (id, distance) for the top_k nearest neighbours.");
}
