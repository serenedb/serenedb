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

#pragma once

#include <absl/container/btree_map.h>
#include <absl/functional/function_ref.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/async/future.hpp>

#include "iresearch/formats/column/col_file_writer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class AnnWriter;
class IdxWriter;
struct AnnBuildEnv;

inline constexpr size_t kMaxColumnDictionaries = 8;

class ColWriter final {
 public:
  ColWriter(Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db, WriteTier tier = WriteTier::Flush,
            bool stage_all = false);
  ~ColWriter();

  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept;

  ColumnWriter* StagedColumn(field_id id) noexcept;

  uint64_t StagedBytes() const noexcept;

  void SetHnswBases(HnswBases* bases) noexcept { _hnsw_bases = bases; }

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           bool skip_validity, uint32_t row_group_size,
                           duckdb::CompressionType compression =
                             duckdb::CompressionType::COMPRESSION_AUTO,
                           bool hyperloglog = false,
                           ColCodecParams codec_params = {});

  ColumnWriter& OpenedColumn(field_id id) noexcept;

  void CloseColumn(field_id id);

  void PlanParts(field_id id, uint32_t parts);

  ColumnWriter& OpenColumnPart(field_id id, uint32_t part,
                               duckdb::LogicalType type, bool skip_validity,
                               uint32_t row_group_size,
                               duckdb::CompressionType compression,
                               ColCodecParams codec_params);

  void CloseColumnPart(field_id id, uint32_t part);

  bool CopyColumnPart(field_id id, uint32_t part, const ColReader& reader,
                      const ColumnReader& column, bool validity,
                      std::span<const uint16_t> dictionaries,
                      absl::FunctionRef<bool()> progress);

  uint16_t AddColumnDictionary(field_id id, std::string_view bytes);

  uint16_t AddLeafDictionary(field_id id, std::span<const uint32_t> path,
                             std::string_view bytes);

  bool AddCopiedDictionaries(
    field_id id,
    std::span<const std::shared_ptr<const codecs::TrainedDictionary>>
      dictionaries,
    std::vector<uint16_t>& indexes);

  AnnWriter& AttachAnn(field_id column_id, AnnInfo info);

  void SetIdxWriter(IdxWriter& idx) noexcept;

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  NormColumnWriter& StreamNormColumn(field_id id);

  std::vector<std::unique_ptr<AnnWriter>> TakeAnnWriters() noexcept;

  bool Commit(uint64_t target_row, std::span<const uint32_t> new2old = {});
  bool Commit(uint64_t target_row, absl::FunctionRef<bool()> progress,
              std::span<const uint32_t> new2old = {});

  yaclib::Task<> ComputeAnn(const AnnBuildEnv* env);
  yaclib::Task<> ComputeAnn(field_id column, ColReader& reader,
                            const AnnBuildEnv* env);

  void Rollback() noexcept;

 private:
  struct AnnEntry {
    field_id column_id;
    AnnInfo info;
    std::unique_ptr<AnnWriter> writer;
  };

  struct ColumnEntry {
    std::unique_ptr<ColumnWriter> writer;
    ColumnMeta meta;
  };

  struct ColumnParts {
    explicit ColumnParts(uint32_t count) : parts(count) {}

    std::vector<ColumnEntry> parts;
    std::array<ColumnDictionaryMeta, kMaxColumnDictionaries> dictionaries;
    std::atomic<uint64_t> claims{0};
    absl::btree_map<std::vector<uint32_t>, ColumnDictionaryMeta>
      leaf_dictionaries;
  };

  ColumnParts& PartsOf(field_id id);

  ColumnEntry& PartOf(field_id id, uint32_t part);

  static void Close(ColumnEntry& entry);

  static ColumnMeta AssembleParts(field_id id, ColumnParts& parts);

  void EnsureOut();
  NormColumnWriter& AddNormColumn(field_id id, uint32_t row_group_size);
  bool Empty() const noexcept;

  Directory* _dir;
  std::string _segment_name;
  std::string _filename;
  duckdb::DatabaseInstance* _db;
  WriteTier _tier;
  const IndexFieldOptions* _field_options = nullptr;
  HnswBases* _hnsw_bases = nullptr;
  std::unique_ptr<ColFileWriter> _file;
  std::unique_ptr<WriteContext> _write_ctx;
  std::vector<std::unique_ptr<ColumnEntry>> _columns;
  irs::containers::FlatHashMap<field_id, ColumnEntry*> _by_id;
  absl::btree_map<field_id, std::unique_ptr<ColumnParts>> _parts;
  std::vector<std::unique_ptr<NormColumnWriter>> _norms;
  irs::containers::FlatHashMap<field_id, NormColumnWriter*> _norm_by_id;
  std::vector<std::unique_ptr<AnnEntry>> _ann_writers;
  irs::containers::FlatHashMap<field_id, AnnEntry*> _ann_by_id;
  bool _stage_all = false;
  bool _committed = false;
};

}  // namespace irs
