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

#include <vpack/iterator.h>
#include <vpack/parser.h>

#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <iostream>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/columnstore/column_writer.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/geo_filter.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/compression.hpp>
#include <iresearch/utils/text_format.hpp>
#include <iresearch/utils/vpack_utils.hpp>
#include <memory>

#include "geo/shape_container.h"
#include "s2/s2latlng.h"
#include "s2/s2loop.h"
#include "s2/s2polygon.h"

// This example demonstrates iresearch's S2-based geospatial filters:
//   1. Index a corpus of points (GeoJSON) with the geojson analyzer
//   2. Run a GeoFilter (shape Intersects) against a polygon
//   3. Run a GeoDistanceFilter against a center + radius (meters)
//
// The geo analyzer tokenizes a shape into S2 cell ids; the filter narrows to
// candidate cells, then re-checks each candidate's stored geometry to drop
// false positives. The "store" side is wired through the columnstore.

namespace {

// Per-segment columnstore needs a duckdb::DatabaseInstance for codec lookup
// and the buffer manager. C++11 thread-safe local statics keep this lazy
// and process-wide; a real app would wire its own DatabaseInstance.
duckdb::DatabaseInstance& Db() {
  static std::unique_ptr<duckdb::DuckDB> kDb = [] {
    duckdb::DBConfig cfg;
    cfg.options.access_mode = duckdb::AccessMode::AUTOMATIC;
    return std::make_unique<duckdb::DuckDB>(":memory:", &cfg);
  }();
  return *kDb->instance;
}

// Stored-geometry column id. The geo filter reads this column back to
// re-check each candidate (S2 cells are an approximation).
inline constexpr irs::field_id kGeoColumnId = 1;

// A geo field that tokenizes a GeoJSON shape (vpack::Slice) into S2 terms.
// The analyzer is constructed once per field instance; reset() is called
// per document with the shape to index.
struct GeoField {
  std::string_view name;
  vpack::Slice shape_slice;
  irs::analysis::Analyzer::ptr analyzer{irs::analysis::GeoJsonAnalyzer::make(
    irs::slice_to_view<char>(vpack::Slice::emptyObjectSlice()))};

  std::string_view Name() const noexcept { return name; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::None;
  }

  irs::Tokenizer& GetTokens() const {
    static_cast<irs::analysis::GeoAnalyzer&>(*analyzer).reset(shape_slice);
    return *analyzer;
  }
};

// Append one BLOB row (the raw vpack bytes of the shape) to the cs column.
void AppendStoredShape(irs::columnstore::ColumnWriter& cw, irs::doc_id_t doc,
                       vpack::Slice shape) {
  duckdb::Vector v{duckdb::LogicalType::BLOB, /*capacity=*/1};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
  slots[0] = duckdb::StringVector::AddStringOrBlob(
    v, reinterpret_cast<const char*>(shape.start()), shape.byteSize());
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  const uint64_t row = static_cast<uint64_t>(doc) - irs::doc_limits::min();
  cw.Append(row, v, /*count=*/1);
}

// 28 named points around central Moscow (lat/lon). The first 16 cluster
// tightly around a single intersection (~50m radius), the next ~5 sit a
// few hundred meters out, and the remainder are kilometers away. Plenty
// of variety for both radius and polygon queries.
constexpr std::string_view kCorpus = R"([
  { "name": "A", "geometry": { "type": "Point", "coordinates": [37.615895, 55.7039  ] } },
  { "name": "B", "geometry": { "type": "Point", "coordinates": [37.615315, 55.703915] } },
  { "name": "C", "geometry": { "type": "Point", "coordinates": [37.61509,  55.703537] } },
  { "name": "D", "geometry": { "type": "Point", "coordinates": [37.614183, 55.703806] } },
  { "name": "E", "geometry": { "type": "Point", "coordinates": [37.613792, 55.704405] } },
  { "name": "F", "geometry": { "type": "Point", "coordinates": [37.614956, 55.704695] } },
  { "name": "G", "geometry": { "type": "Point", "coordinates": [37.616297, 55.704831] } },
  { "name": "H", "geometry": { "type": "Point", "coordinates": [37.617053, 55.70461 ] } },
  { "name": "I", "geometry": { "type": "Point", "coordinates": [37.61582,  55.704459] } },
  { "name": "J", "geometry": { "type": "Point", "coordinates": [37.614634, 55.704338] } },
  { "name": "K", "geometry": { "type": "Point", "coordinates": [37.613121, 55.704193] } },
  { "name": "L", "geometry": { "type": "Point", "coordinates": [37.614135, 55.703298] } },
  { "name": "M", "geometry": { "type": "Point", "coordinates": [37.613663, 55.704002] } },
  { "name": "N", "geometry": { "type": "Point", "coordinates": [37.616522, 55.704235] } },
  { "name": "O", "geometry": { "type": "Point", "coordinates": [37.615508, 55.704172] } },
  { "name": "P", "geometry": { "type": "Point", "coordinates": [37.614629, 55.704081] } },
  { "name": "Q", "geometry": { "type": "Point", "coordinates": [37.610235, 55.709754] } },
  { "name": "R", "geometry": { "type": "Point", "coordinates": [37.605,    55.707917] } },
  { "name": "S", "geometry": { "type": "Point", "coordinates": [37.545776, 55.722083] } },
  { "name": "T", "geometry": { "type": "Point", "coordinates": [37.559509, 55.715895] } },
  { "name": "U", "geometry": { "type": "Point", "coordinates": [37.701645, 55.832144] } },
  { "name": "V", "geometry": { "type": "Point", "coordinates": [37.73735,  55.816715] } },
  { "name": "W", "geometry": { "type": "Point", "coordinates": [37.75589,  55.798193] } },
  { "name": "X", "geometry": { "type": "Point", "coordinates": [37.659073, 55.843711] } },
  { "name": "Y", "geometry": { "type": "Point", "coordinates": [37.778549, 55.823659] } },
  { "name": "Z", "geometry": { "type": "Point", "coordinates": [37.729797, 55.853733] } },
  { "name": "1", "geometry": { "type": "Point", "coordinates": [37.608261, 55.784682] } },
  { "name": "2", "geometry": { "type": "Point", "coordinates": [37.525177, 55.802825] } }
])";

irs::IndexWriterOptions MakeWriterOptions() {
  irs::IndexWriterOptions options;
  options.db = &Db();
  options.reader_options.db = &Db();
  options.column_options = [](irs::field_id) -> irs::ColumnOptions {
    return {.row_group_size = DEFAULT_ROW_GROUP_SIZE};
  };
  options.norm_column_options =
    [next = std::make_shared<std::atomic<irs::field_id>>(0)](
      std::string_view) -> irs::NormColumnOptions {
    return {.id = next->fetch_add(1, std::memory_order_relaxed),
            .row_group_size = DEFAULT_ROW_GROUP_SIZE};
  };
  return options;
}

// Build a tiny index from the corpus above. Returns the writer's snapshot.
irs::DirectoryReader BuildIndex(irs::Directory& dir,
                                std::shared_ptr<vpack::Builder> docs,
                                std::vector<std::string>& names_out) {
  auto format = irs::formats::Get("1_5simd");
  auto writer =
    irs::IndexWriter::Make(dir, format, irs::kOmCreate, MakeWriterOptions());

  GeoField geo;
  geo.name = "geometry";

  {
    auto trx = writer->GetBatch();
    irs::columnstore::ColumnWriter* geo_cw = nullptr;
    for (auto doc_slice : vpack::ArrayIterator(docs->slice())) {
      names_out.emplace_back(irs::slice_to_string_view(doc_slice.get("name")));

      geo.shape_slice = doc_slice.get("geometry");
      auto doc = trx.Insert();
      doc.Insert(geo);

      if (geo_cw == nullptr) {
        geo_cw = &doc.Columnstore()->OpenColumn(kGeoColumnId,
                                                duckdb::LogicalType::BLOB);
      }
      AppendStoredShape(*geo_cw, doc.DocId(), geo.shape_slice);
    }
  }
  writer->RefreshCommit();
  return writer->GetSnapshot();
}

// Run a prepared filter over all segments, collect names of matching docs.
std::vector<std::string> RunFilter(const irs::DirectoryReader& reader,
                                   const irs::Filter& filter,
                                   const std::vector<std::string>& names) {
  std::vector<std::string> hits;
  auto prepared = filter.prepare({.index = reader});
  for (auto& segment : reader) {
    auto it = prepared->execute({.segment = segment});
    while (it->next()) {
      const auto doc = it->value();
      const auto idx = doc - irs::doc_limits::min();
      if (idx < names.size()) {
        hits.push_back(names[idx]);
      }
    }
  }
  return hits;
}

void PrintHits(std::string_view label, const std::vector<std::string>& hits) {
  std::cout << label << ": " << hits.size() << " hit(s)";
  if (!hits.empty()) {
    std::cout << " -- {";
    for (size_t i = 0; i < hits.size(); ++i) {
      if (i) {
        std::cout << ", ";
      }
      std::cout << hits[i];
    }
    std::cout << "}";
  }
  std::cout << "\n";
}

}  // namespace

int main() {
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  auto docs = vpack::Parser::fromJson(kCorpus);
  std::vector<std::string> names;

  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, docs, names);
  std::cout << "Indexed " << reader.docs_count() << " points across "
            << reader.size() << " segment(s).\n\n";

  // 1) Distance query: everything within 300m of a center point.
  {
    std::cout << "=== GeoDistanceFilter (radius 300m) ===\n";
    irs::GeoDistanceFilter q;
    *q.mutable_field() = "geometry";
    q.mutable_options()->store_field_id = kGeoColumnId;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.70892, 37.607768).ToPoint();
    auto& range = q.mutable_options()->range;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 300;  // meters

    PrintHits("origin=(55.70892, 37.607768), max=300m",
              RunFilter(reader, q, names));
  }

  // 2) Annulus query: 1km <= distance <= 5km from a different center.
  {
    std::cout << "\n=== GeoDistanceFilter (annulus 1km..5km) ===\n";
    irs::GeoDistanceFilter q;
    *q.mutable_field() = "geometry";
    q.mutable_options()->store_field_id = kGeoColumnId;
    q.mutable_options()->origin =
      S2LatLng::FromDegrees(55.704, 37.615).ToPoint();
    auto& range = q.mutable_options()->range;
    range.min_type = irs::BoundType::Inclusive;
    range.min = 1000;
    range.max_type = irs::BoundType::Inclusive;
    range.max = 5000;

    PrintHits("origin=(55.704, 37.615), 1km <= d <= 5km",
              RunFilter(reader, q, names));
  }

  // 3) Polygon query: points falling inside an arbitrary quadrilateral
  //    covering the central cluster (lat/lon corners chosen to enclose
  //    the tightly clustered first 16 points).
  {
    std::cout << "\n=== GeoFilter (Intersects polygon) ===\n";
    std::vector<S2LatLng> corners{
      S2LatLng::FromDegrees(55.7030, 37.6130),
      S2LatLng::FromDegrees(55.7030, 37.6175),
      S2LatLng::FromDegrees(55.7050, 37.6175),
      S2LatLng::FromDegrees(55.7050, 37.6130),
    };
    std::vector<S2Point> points;
    points.reserve(corners.size());
    for (const auto& ll : corners) {
      points.push_back(ll.ToPoint());
    }
    auto loop = std::make_unique<S2Loop>(points);
    loop->Normalize();

    sdb::geo::ShapeContainer shape;
    shape.reset(std::make_unique<S2Polygon>(std::move(loop)),
                sdb::geo::ShapeContainer::Type::S2Polygon);

    irs::GeoFilter q;
    *q.mutable_field() = "geometry";
    q.mutable_options()->store_field_id = kGeoColumnId;
    q.mutable_options()->type = irs::GeoFilterType::Intersects;
    q.mutable_options()->shape = std::move(shape);

    PrintHits("polygon over central cluster", RunFilter(reader, q, names));
  }

  return 0;
}
