#!/usr/bin/env python3
"""Generates resources/tests/iceberg: two local iceberg
tables whose second snapshot carries a positional delete file that is
from a foreign writer: attributable to no single data file (no referenced_data_file, no file_path bounds in
its manifest entry) -- the shape foreign writers produce and the only way to
exercise REFRESH's partition/global delete-attribution rungs, since
duckdb-iceberg always writes attributable deletes.

  plain/  unpartitioned, 2 data files; snapshot 2 deletes id=2  -> global rung
          snapshot 3 = a global EQUALITY delete on id=1 plus a data file
          re-inserting id=1 (and a NULL id) in the same snapshot -> the
          remove-by-query road and its upsert-survival edge in one rung
          snapshot 4 = equality delete on id=NULL -> IS NULL translation
          plus the accumulation shape (only the new delete translates)
          snapshot 5 = multi-column equality delete on (id, body); the
          analyzed body refuses translation -> the scan road
          snapshot 6 = three appended rows (+ a no-match id=999 delete)
          snapshot 7 = ONE equality delete file with TWO body rows -> the
          multi-row (OR) scan-road shape
  part/   identity(part), 2 files per partition; snapshot 2 deletes id=2
          in partition a                                        -> partition rung
          snapshot 3 deletes id=4 (partition a scoped) AND id=5
          (pinned to its data file via file_path bounds) -> the mixed rung:
          partition a rescans while the pinned file MASKS in the same
          refresh (the per-file mask gate)
          snapshot 4 = partition-a-scoped equality delete on id=1
          snapshot 5 = equality delete on the non-indexed `part` column
          -> the translation-refusal (rescan fallback) rung

The fixture is NOT checked in: scripts/ensure_iceberg_fixture.sh generates
it before test runs that need it, in a throwaway python container
(pyiceberg[sql-sqlite], pyarrow, fastavro), stamping the output and
regenerating when this script changes.

pyiceberg writes the base tables (it cannot write delete files at all); the
delete parquet, delete manifest, manifest list and v2 metadata are crafted
here with fastavro against the schemas embedded in pyiceberg's own files.
The delete parquet's file_path column must hold the path exactly as recorded
in the data manifest: the reader matches delete content against
data_file.file_path BEFORE any allow_moved_paths remapping.
"""

import copy
import json
import os
import shutil
import sys
import time
import uuid

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(REPO, "resources", "tests", "iceberg")
WORK = os.path.abspath(os.environ.get("ICE_FIXTURE_WORK", "/tmp/ice_fixture_work"))

FILE_PATH_FIELD_ID = b"2147483546"
POS_FIELD_ID = b"2147483545"


def local_path(p):
    return p[len("file://"):] if p.startswith("file://") else p


def read_avro(path):
    with open(path, "rb") as f:
        reader = fastavro.reader(f)
        return reader.writer_schema, list(reader)


def write_avro(path, schema, records):
    with open(path, "wb") as f:
        fastavro.writer(f, schema, records)


def data_files_of(table):
    files = []
    snap = table.current_snapshot()
    for mf in snap.manifests(table.io):
        _, entries = read_avro(local_path(mf.manifest_path))
        for e in entries:
            files.append(e["data_file"])
    return files


def pa_type_of(iceberg_type):
    from pyiceberg.types import LongType, StringType
    return {LongType: pa.int64, StringType: pa.string}[type(iceberg_type)]()


def craft_snapshot(table_dir, table, deletes, appends, version, drop_manifests):
    """One crafted snapshot. Returns the delete manifest path it wrote.
    deletes = a list of either
      {dead_id, partition, pin}   positional: kills dead_id's row; pin=True
                                  writes equal lower/upper file_path bounds
                                  (the v2 file-scoped shape) so the delete
                                  attributes to exactly its data file,
                                  pin=False stays unattributable;
      {equality: {col: value}, partition}  an equality delete file on the
                                  named columns (field ids from the table
                                  schema).
    appends = [{col: value}] rows written as ONE new data file in the SAME
    snapshot -- its data sequence number equals the deletes', the strict
    comparison every applicability rule uses (the CDC upsert shape).
    drop_manifests = manifest paths to remove from the inherited manifest
    list (set() for none)."""
    meta_dir = os.path.join(table_dir, "metadata")
    data_dir = os.path.join(table_dir, "data")
    schema_fields = {f.name: f for f in table.schema().fields}

    prev_meta = os.path.join(meta_dir, f"v{version - 1}.metadata.json")
    meta_path = prev_meta if os.path.exists(prev_meta) else local_path(table.metadata_location)
    with open(meta_path) as f:
        meta = json.load(f)
    snap_id = max(s["snapshot-id"] for s in meta["snapshots"]) + 1
    seq = meta["last-sequence-number"] + 1
    now_ms = int(time.time() * 1000)

    cur_snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
    old_list_path = local_path(cur_snap["manifest-list"])
    list_schema, list_records = read_avro(old_list_path)
    template_manifest = local_path(list_records[0]["manifest_path"])
    entry_schema, template_entries = read_avro(template_manifest)

    dropped = {os.path.basename(p) for p in drop_manifests}
    list_records = [r for r in list_records
                    if os.path.basename(r["manifest_path"]) not in dropped]

    sample_path = template_entries[0]["data_file"]["file_path"]

    # A fresh manifest entry: schema from an existing manifest of the same
    # table; every stats field stays null and referenced_data_file (when the
    # schema has it) stays null.
    def make_entry(parquet, content, record_count, partition):
        entry = copy.deepcopy(template_entries[0])
        entry["status"] = 1
        entry["snapshot_id"] = snap_id
        for key in ("sequence_number", "file_sequence_number", "data_sequence_number"):
            if key in entry:
                entry[key] = None
        df = entry["data_file"]
        df["content"] = content
        df["file_path"] = "file://" + parquet if sample_path.startswith("file://") else parquet
        df["file_format"] = "PARQUET"
        df["record_count"] = record_count
        df["file_size_in_bytes"] = os.path.getsize(parquet)
        df["partition"] = partition
        keep = {"content", "file_path", "file_format", "record_count",
                "file_size_in_bytes", "partition", "equality_ids"}
        for key in df:
            if key not in keep:
                df[key] = None
        return entry

    def make_list_entry(manifest_path, content, added):
        list_entry = copy.deepcopy(list_records[0])
        list_entry["manifest_path"] = ("file://" + manifest_path
                                       if list_records[0]["manifest_path"].startswith("file://")
                                       else manifest_path)
        list_entry["manifest_length"] = os.path.getsize(manifest_path)
        list_entry["content"] = content
        list_entry["sequence_number"] = seq
        list_entry["min_sequence_number"] = seq
        list_entry["added_snapshot_id"] = snap_id
        for key in list_entry:
            if "added" in key and "count" in key:
                list_entry[key] = added
            elif ("existing" in key or "deleted" in key) and "count" in key:
                list_entry[key] = 0
        if "partitions" in list_entry:
            list_entry["partitions"] = None
        if "key_metadata" in list_entry:
            list_entry["key_metadata"] = None
        return list_entry

    entries = []
    for i, spec in enumerate(deletes):
        delete_parquet = os.path.join(data_dir, f"delete-{version:02d}{i:03d}.parquet")
        if "equality" in spec:
            fields = []
            arrays = {}
            ids = []
            for col, value in spec["equality"].items():
                f = schema_fields[col]
                fields.append(pa.field(col, pa_type_of(f.field_type), nullable=True,
                                       metadata={b"PARQUET:field_id": str(f.field_id).encode()}))
                arrays[col] = value if isinstance(value, list) else [value]
                ids.append(f.field_id)
            row_count = len(next(iter(arrays.values())))
            pq.write_table(pa.table(arrays, schema=pa.schema(fields)), delete_parquet)
            entry = make_entry(delete_parquet, 2, row_count, spec["partition"])
            assert "equality_ids" in entry["data_file"], "manifest schema lacks equality_ids"
            entry["data_file"]["equality_ids"] = ids
        else:
            if "positions" in spec:
                # Bulk positional delete against the target-th data file.
                dead_file = data_files_of(table)[spec["target"]]
                positions = spec["positions"]
            else:
                dead_file = None
                dead_pos = None
                for df in data_files_of(table):
                    ids = pq.read_table(local_path(df["file_path"]), columns=["id"])["id"].to_pylist()
                    if spec["dead_id"] in ids:
                        dead_file = df
                        dead_pos = ids.index(spec["dead_id"])
                        break
                assert dead_file, f"id={spec['dead_id']} not found in any data file"
                positions = [dead_pos]
            schema = pa.schema([
                pa.field("file_path", pa.string(), nullable=False,
                         metadata={b"PARQUET:field_id": FILE_PATH_FIELD_ID}),
                pa.field("pos", pa.int64(), nullable=False,
                         metadata={b"PARQUET:field_id": POS_FIELD_ID}),
            ])
            pq.write_table(
                pa.table({"file_path": [dead_file["file_path"]] * len(positions),
                          "pos": positions}, schema=schema),
                delete_parquet)
            entry = make_entry(delete_parquet, 1, len(positions), spec["partition"])
            if spec["pin"]:
                # The ladder's rung-2 attribution shape.
                bound = [{"key": int(FILE_PATH_FIELD_ID),
                          "value": dead_file["file_path"].encode()}]
                entry["data_file"]["lower_bounds"] = bound
                entry["data_file"]["upper_bounds"] = copy.deepcopy(bound)
        entries.append(entry)

    manifest_path = os.path.join(meta_dir, f"delete-{uuid.uuid4()}-m0.avro")
    write_avro(manifest_path, entry_schema, entries)
    new_records = list_records + [make_list_entry(manifest_path, 1, len(entries))]

    if appends:
        data_parquet = os.path.join(data_dir, f"data-{version:02d}.parquet")
        fields = []
        arrays = {}
        for f in table.schema().fields:
            fields.append(pa.field(f.name, pa_type_of(f.field_type), nullable=True,
                                   metadata={b"PARQUET:field_id": str(f.field_id).encode()}))
            arrays[f.name] = [row[f.name] for row in appends]
        pq.write_table(pa.table(arrays, schema=pa.schema(fields)), data_parquet)
        data_entry = make_entry(data_parquet, 0, len(appends), {})
        data_manifest_path = os.path.join(meta_dir, f"data-{uuid.uuid4()}-m0.avro")
        write_avro(data_manifest_path, entry_schema, [data_entry])
        new_records = new_records + [make_list_entry(data_manifest_path, 0, 1)]

    new_list_name = f"snap-{snap_id}-0-{uuid.uuid4()}.avro"
    new_list_path = os.path.join(meta_dir, new_list_name)
    write_avro(new_list_path, list_schema, new_records)

    meta["snapshots"].append({
        "snapshot-id": snap_id,
        "parent-snapshot-id": cur_snap["snapshot-id"],
        "sequence-number": seq,
        "timestamp-ms": now_ms,
        "summary": {"operation": "delete"},
        "manifest-list": ("file://" + new_list_path
                          if cur_snap["manifest-list"].startswith("file://")
                          else new_list_path),
        "schema-id": meta["current-schema-id"],
    })
    meta["current-snapshot-id"] = snap_id
    meta["last-sequence-number"] = seq
    meta["last-updated-ms"] = now_ms
    meta["snapshot-log"] = meta.get("snapshot-log", []) + [
        {"snapshot-id": snap_id, "timestamp-ms": now_ms}]
    meta["metadata-log"] = []
    meta.setdefault("refs", {})["main"] = {"snapshot-id": snap_id, "type": "branch"}

    if version == 2:
        shutil.copyfile(meta_path, os.path.join(meta_dir, "v1.metadata.json"))
    with open(os.path.join(meta_dir, f"v{version}.metadata.json"), "w") as f:
        json.dump(meta, f)

    keep = {f"v{k}.metadata.json" for k in range(1, version + 1)}
    for name in os.listdir(meta_dir):
        if name.endswith(".metadata.json") and name not in keep:
            os.remove(os.path.join(meta_dir, name))
        if name.endswith(".crc") or name == "version-hint.text":
            os.remove(os.path.join(meta_dir, name))
    return manifest_path


def noop_snapshot(table_dir, src_version, out_version):
    """A new snapshot id over the same files: the restamp/no-op tick shape."""
    meta_dir = os.path.join(table_dir, "metadata")
    with open(os.path.join(meta_dir, f"v{src_version}.metadata.json")) as f:
        meta = json.load(f)
    cur = [s for s in meta["snapshots"]
           if s["snapshot-id"] == meta["current-snapshot-id"]][0]
    new = dict(cur)
    new["snapshot-id"] = cur["snapshot-id"] + 1
    new["sequence-number"] = meta["last-sequence-number"] + 1
    new["parent-snapshot-id"] = cur["snapshot-id"]
    meta["snapshots"].append(new)
    meta["current-snapshot-id"] = new["snapshot-id"]
    meta["last-sequence-number"] = new["sequence-number"]
    meta.setdefault("snapshot-log", []).append(
        {"snapshot-id": new["snapshot-id"],
         "timestamp-ms": cur.get("timestamp-ms", 0) + 1})
    with open(os.path.join(meta_dir, f"v{out_version}.metadata.json"), "w") as f:
        json.dump(meta, f)


def main():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    catalog = SqlCatalog("local", uri=f"sqlite:///{WORK}/catalog.db", warehouse=f"file://{WORK}/wh")
    catalog.create_namespace("ns")

    plain = catalog.create_table("ns.plain", pa.schema([
        pa.field("id", pa.int64()), pa.field("body", pa.string())]))
    plain.append(pa.table({"id": [1, 2], "body": ["pudge goes mid", "anchin reads manga"]}))
    plain.append(pa.table({"id": [3], "body": ["vedernikoff pins snapshots"]}))
    plain = catalog.load_table("ns.plain")
    plain_dir = os.path.join(WORK, "wh", "ns", "plain")
    craft_snapshot(plain_dir, plain,
                   [{"dead_id": 2, "partition": {}, "pin": False}], [], 2, set())
    # The upsert shape: a global equality delete on id=1 and a data file
    # re-inserting id=1, in the SAME snapshot -- the delete's sequence number
    # is not strictly above the new file's, so the new row survives. The
    # appended file also carries a NULL id, snapshot 4's target.
    craft_snapshot(plain_dir, plain,
                   [{"equality": {"id": 1}, "partition": {}}],
                   [{"id": 1, "body": "pudge reborn"},
                    {"id": None, "body": "techies invisible"}], 3, set())
    # Snapshot 4 = equality delete on a NULL value (IS NULL through the null
    # marker field) -- also the accumulation shape: the files snapshot 3
    # covered translate only THIS delete the second time around.
    craft_snapshot(plain_dir, plain,
                   [{"equality": {"id": None}, "partition": {}}], [], 4, set())
    # Snapshot 5 = multi-column equality delete (id, body): body rides an
    # analyzer, so translation refuses -- the scan-road shape (rescan
    # fallback before it existed).
    craft_snapshot(plain_dir, plain,
                   [{"equality": {"id": 3, "body": "vedernikoff pins snapshots"},
                     "partition": {}}], [], 5, set())
    # Snapshot 6 = three fresh rows plus a same-snapshot no-match equality
    # delete (id=999) -- pure append fodder for snapshot 7.
    craft_snapshot(plain_dir, plain,
                   [{"equality": {"id": 999}, "partition": {}}],
                   [{"id": 10, "body": "io farms safely"},
                    {"id": 11, "body": "kunkka lands torrent"},
                    {"id": 12, "body": "sniper stays back"}], 6, set())
    # Snapshot 7 = ONE equality delete file with TWO rows on the analyzed
    # body column -- the multi-row (OR) shape through the scan road.
    craft_snapshot(plain_dir, plain,
                   [{"equality": {"body": ["io farms safely",
                                           "kunkka lands torrent"]},
                     "partition": {}}], [], 7, set())
    # v8 = a no-op snapshot over v7 (the restamp rung); v9 = the same shape
    # over v5 (the periodic no-op tick) -- the hint picks them directly, the
    # numbering gap is legal.
    noop_snapshot(plain_dir, 7, 8)
    noop_snapshot(plain_dir, 5, 9)

    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, LongType, StringType
    schema = Schema(
        NestedField(1, "part", StringType(), required=False),
        NestedField(2, "id", LongType(), required=False),
        NestedField(3, "body", StringType(), required=False))
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000,
                                        transform=IdentityTransform(), name="part"))
    part = catalog.create_table("ns.part", schema, partition_spec=spec)
    part.append(pa.table({"part": ["a", "a", "b"], "id": [1, 2, 3],
                          "body": ["pudge goes mid", "anchin reads manga",
                                   "vedernikoff reviews part b"]}))
    part.append(pa.table({"part": ["a", "b"], "id": [4, 5],
                          "body": ["pudge farms jungle", "techies plants mines"]}))
    part = catalog.load_table("ns.part")
    part_dir = os.path.join(WORK, "wh", "ns", "part")
    craft_snapshot(
        part_dir, part,
        [{"dead_id": 2, "partition": {"part": "a"}, "pin": False}], [], 2, set())
    craft_snapshot(
        part_dir, part,
        [{"dead_id": 4, "partition": {"part": "a"}, "pin": False},
         {"dead_id": 5, "partition": {"part": None}, "pin": True}], [], 3, set())
    # Equality rungs: snapshot 4 scopes an equality delete on id=1 to
    # partition a (partition b's files must not move); snapshot 5 deletes on
    # `part` -- a column a (id, body) index has no term dictionary for, the
    # translation-refusal shape.
    craft_snapshot(
        part_dir, part,
        [{"equality": {"id": 1}, "partition": {"part": "a"}}], [], 4, set())
    craft_snapshot(
        part_dir, part,
        [{"equality": {"part": "b"}, "partition": {"part": "b"}}], [], 5, set())

    shutil.rmtree(OUT, ignore_errors=True)

    # One directory per (table, version): tests flip versions by re-pointing
    # a $__TEST_DIR__ symlink at the next variant instead of copying the
    # table and rewriting its hint. nohint variants keep only the metadata
    # jsons up to N for the version-guessing rungs.
    def emit(name, table, hint, max_meta_json):
        src = os.path.join(WORK, "wh", "ns", table)
        dst = os.path.join(OUT, name)
        os.makedirs(dst)
        shutil.copytree(os.path.join(src, "data"), os.path.join(dst, "data"))
        meta_dst = os.path.join(dst, "metadata")
        shutil.copytree(os.path.join(src, "metadata"), meta_dst)
        if hint is not None:
            with open(os.path.join(meta_dst, "version-hint.text"), "w") as f:
                f.write(str(hint))
        if max_meta_json is not None:
            for entry in os.listdir(meta_dst):
                if (entry.startswith("v") and entry.endswith(".metadata.json")
                        and int(entry[1:-len(".metadata.json")]) > max_meta_json):
                    os.remove(os.path.join(meta_dst, entry))

    for n in range(1, 10):
        emit(f"plain_v{n}", "plain", n, None)
    for n in range(1, 6):
        emit(f"part_v{n}", "part", n, None)
    for n in range(1, 4):
        emit(f"plain_nohint_v{n}", "plain", None, n)
    print("fixture written to", OUT)


if __name__ == "__main__":
    sys.exit(main())
