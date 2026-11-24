"""Quack requests built byte by byte, for tests that send what no client would."""

from __future__ import annotations

import copy
import http.client
import struct
from typing import NamedTuple

CONNECTION_REQUEST, CONNECTION_RESPONSE = 1, 2
PREPARE_REQUEST, SEND_DATA_REQUEST, ERROR_RESPONSE = 3, 9, 100

END = struct.pack("<H", 0xFFFF)
# CONNECTION_RESPONSE's field with the SereneDB storage version the server agreed on
SERENEDB_STORAGE_VERSION_FIELD = 16389


def uvar(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if not value:
            out.append(byte)
            return bytes(out)
        out.append(byte | 0x80)


def field(field_id: int) -> bytes:
    return struct.pack("<H", field_id)


def text(value: str | bytes) -> bytes:
    if isinstance(value, str):
        value = value.encode()
    return uvar(len(value)) + value


def logical_type(type_id: int) -> bytes:
    return field(100) + uvar(type_id) + END


BOOLEAN, BIGINT, VARCHAR, UTINYINT, BIT = 10, 14, 25, 28, 36
STRUCT, LIST, ENUM, UNION, ARRAY = 100, 101, 104, 107, 108
CONSTANT_VECTOR, DICTIONARY_VECTOR, SEQUENCE_VECTOR = 2, 3, 4


def list_type(child: bytes) -> bytes:
    info = field(100) + uvar(4) + field(200) + child + END
    return field(100) + uvar(LIST) + field(101) + b"\x01" + info + END


def _type_with_info(type_id: int, info_type: int, info: bytes) -> bytes:
    return (field(100) + uvar(type_id) + field(101) + b"\x01" + field(100) +
            uvar(info_type) + info + END + END)


def enum_type(values: list[str]) -> bytes:
    return _type_with_info(
        ENUM, 6, field(200) + uvar(len(values)) + field(201) +
        uvar(len(values)) + b"".join(text(value) for value in values))


def _children_type(type_id: int, children: list[tuple[str, bytes]]) -> bytes:
    return _type_with_info(
        type_id, 5, field(200) + uvar(len(children)) + b"".join(
            field(0) + text(name) + field(1) + child + END
            for name, child in children))


def struct_type(children: list[tuple[str, bytes]]) -> bytes:
    return _children_type(STRUCT, children)


def union_type(members: list[tuple[str, bytes]]) -> bytes:
    return _children_type(UNION, [("", logical_type(UTINYINT)), *members])


def array_type(child: bytes, size: int) -> bytes:
    info = (field(100) + uvar(9) + field(200) + child + field(201) +
            uvar(size) + END)
    return field(100) + uvar(ARRAY) + field(101) + b"\x01" + info + END


def flat_vector(body: bytes, null: bool = False) -> bytes:
    """A vector of up to 64 rows, all of them NULL with `null`."""
    validity = b"\x01" + field(101) + text(bytes(8)) if null else b"\x00"
    return field(90) + uvar(0) + field(100) + validity + body + END


def fixed_vector(data: bytes, null: bool = False) -> bytes:
    return flat_vector(field(102) + text(data), null)


def string_vector(lengths: list[int], data: bytes, null: bool = False) -> bytes:
    length_data = b"".join(struct.pack("<I", length) for length in lengths)
    return flat_vector(field(107) + uvar(len(data)) + field(108) +
                       text(length_data) + field(109) + text(data), null)


def list_vector(list_size: int, entries: int, child: bytes,
                offset: int = 0, length: int = 0) -> bytes:
    entry = field(100) + uvar(offset) + field(101) + uvar(length) + END
    return flat_vector(field(104) + uvar(list_size) + field(105) +
                       uvar(entries) + entry * entries + field(106) + child)


def struct_vector(children: list[bytes]) -> bytes:
    return flat_vector(field(103) + uvar(len(children)) + b"".join(children))


def array_vector(array_size: int, child: bytes) -> bytes:
    return flat_vector(field(103) + uvar(array_size) + field(104) + child)


def dictionary_vector(selection: list[int], dict_count: int,
                      data: bytes) -> bytes:
    sel = b"".join(struct.pack("<I", index) for index in selection)
    return (field(90) + uvar(DICTIONARY_VECTOR) + field(91) + text(sel) +
            field(92) + uvar(dict_count) + field(100) + b"\x00" +
            field(102) + text(data) + END)


def sequence_vector() -> bytes:
    return (field(90) + uvar(SEQUENCE_VECTOR) + field(91) + uvar(0) +
            field(92) + uvar(0) + END)


def constant_vector(value: bytes) -> bytes:
    """`value` is a vector of one row."""
    return field(90) + uvar(CONSTANT_VECTOR) + value


def nested_constant_vector(depth: int) -> bytes:
    return (field(90) + uvar(CONSTANT_VECTOR)) * depth


def chunk(rows: int, types: list[bytes], columns: list[bytes]) -> bytes:
    return (field(100) + uvar(rows) + field(101) + uvar(len(types)) +
            b"".join(types) + field(102) + uvar(len(columns)) +
            b"".join(columns) + END)


class Session:
    """One quack session on a server, opened with `token` as a DuckDB client, or as a SereneDB one when
    `serenedb_header` names the newest storage version it writes."""

    def __init__(self, port: int, token: str, serenedb_header: str = ""):
        self._http = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        self._headers = {"Content-Type": "application/octet-stream"}
        if serenedb_header:
            self._headers["X-SereneDB-Quack"] = serenedb_header
        body = (field(1) + text(token) + field(2) + text("v2.0.0") +
                field(3) + text("linux_amd64") + field(4) + uvar(3) +
                field(5) + uvar(3) + field(7) + uvar(60) + END)
        response = self._post(self._header(CONNECTION_REQUEST, "") + body)
        self.message_type = response.message_type
        self.connection_id = response.connection_id
        self.error = response.error
        self.serenedb_storage_version = response.serenedb_storage_version

    def close(self) -> None:
        self._http.close()

    def on_another_connection(self) -> Session:
        """The same session over an HTTP connection of its own, for a request sent while another one waits."""
        other = copy.copy(self)
        other._http = http.client.HTTPConnection("127.0.0.1", self._http.port, timeout=30)
        return other

    @staticmethod
    def _header(message_type: int, connection_id: str) -> bytes:
        out = field(1) + uvar(message_type)
        if connection_id:
            out += field(2) + text(connection_id)
        return out + field(3) + uvar(1) + END

    def _post(self, body: bytes) -> Response:
        self._http.request("POST", "/quack", body, self._headers)
        return _read_response(self._http.getresponse().read())

    def prepare(self, sql: str, query_uuid: int) -> tuple[int, str]:
        body = (field(1) + text(sql) + field(2) + uvar(0) + uvar(query_uuid)
                + field(3) + uvar(0) + END)
        response = self._post(
            self._header(PREPARE_REQUEST, self.connection_id) + body)
        return response.message_type, response.error

    def send_data(self, stream_id: str, batch: bytes) -> tuple[int, str]:
        """SEND_DATA of one chunk as batch 1; the error message when the server refuses it."""
        body = (field(1) + text(stream_id) + field(2) + uvar(1) + field(4) +
                uvar(1) + END)
        response = self._post(
            self._header(SEND_DATA_REQUEST, self.connection_id) + body + batch)
        return response.message_type, response.error


class Response(NamedTuple):
    message_type: int
    connection_id: str
    error: str
    serenedb_storage_version: int


def _read_response(body: bytes) -> Response:
    position = 0

    def read_field() -> int:
        nonlocal position
        (value,) = struct.unpack_from("<H", body, position)
        position += 2
        return value

    def read_uvar() -> int:
        nonlocal position
        value = shift = 0
        while True:
            byte = body[position]
            position += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return value

    def read_text() -> str:
        nonlocal position
        size = read_uvar()
        value = body[position:position + size]
        position += size
        return value.decode(errors="replace")

    message_type, connection_id, message, serenedb_storage_version = 0, "", "", 0
    while (field_id := read_field()) != 0xFFFF:
        if field_id == 1:
            message_type = read_uvar()
        elif field_id == 2:
            connection_id = read_text()
        else:
            read_uvar()
    if message_type == ERROR_RESPONSE and read_field() == 1:
        message = read_text()
    elif message_type == CONNECTION_RESPONSE:
        while (field_id := read_field()) != 0xFFFF:
            if field_id in (1, 2):
                read_text()
            elif field_id == SERENEDB_STORAGE_VERSION_FIELD:
                serenedb_storage_version = read_uvar()
            else:
                read_uvar()
    return Response(message_type, connection_id, message, serenedb_storage_version)
