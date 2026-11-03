"""Quack requests built byte by byte, for tests that send what no client would."""

from __future__ import annotations

import http.client
import struct

CONNECTION_REQUEST, CONNECTION_RESPONSE = 1, 2
PREPARE_REQUEST, SEND_DATA_REQUEST, ERROR_RESPONSE = 3, 9, 100

END = struct.pack("<H", 0xFFFF)


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


BOOLEAN, BIGINT, VARCHAR = 10, 14, 25
LIST = 101


def list_type(child: bytes) -> bytes:
    info = field(100) + uvar(4) + field(200) + child + END
    return field(100) + uvar(LIST) + field(101) + b"\x01" + info + END


def flat_vector(body: bytes) -> bytes:
    return field(90) + uvar(0) + field(100) + b"\x00" + body + END


def fixed_vector(data: bytes) -> bytes:
    return flat_vector(field(102) + text(data))


def string_vector(lengths: list[int], data: bytes) -> bytes:
    length_data = b"".join(struct.pack("<I", length) for length in lengths)
    return flat_vector(field(107) + uvar(len(data)) + field(108) +
                       text(length_data) + field(109) + text(data))


def list_vector(list_size: int, entries: int, child: bytes) -> bytes:
    entry = field(100) + uvar(0) + field(101) + uvar(0) + END
    return flat_vector(field(104) + uvar(list_size) + field(105) +
                       uvar(entries) + entry * entries + field(106) + child)


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
        self.message_type, self.connection_id, self.error, self.serenedb_storage_version = response

    def close(self) -> None:
        self._http.close()

    @staticmethod
    def _header(message_type: int, connection_id: str) -> bytes:
        out = field(1) + uvar(message_type)
        if connection_id:
            out += field(2) + text(connection_id)
        return out + field(3) + uvar(1) + END

    def _post(self, body: bytes) -> tuple[int, str, str, int]:
        self._http.request("POST", "/quack", body, self._headers)
        return _read_response(self._http.getresponse().read())

    def prepare(self, sql: str, query_uuid: int) -> tuple[int, str]:
        body = (field(1) + text(sql) + field(2) + uvar(0) + uvar(query_uuid)
                + field(3) + uvar(0) + END)
        message_type, _, message, _ = self._post(
            self._header(PREPARE_REQUEST, self.connection_id) + body)
        return message_type, message

    def send_data(self, stream_id: str, batch: bytes) -> tuple[int, str]:
        """SEND_DATA of one chunk as batch 1; the error message when the server refuses it."""
        body = (field(1) + text(stream_id) + field(2) + uvar(1) + field(4) +
                uvar(1) + END)
        message_type, _, message, _ = self._post(
            self._header(SEND_DATA_REQUEST, self.connection_id) + body + batch)
        return message_type, message


def _read_response(body: bytes) -> tuple[int, str, str, int]:
    """The message type, the connection id, an error's message and a CONNECT's SereneDB storage version."""
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
            elif field_id == 16389:
                serenedb_storage_version = read_uvar()
            else:
                read_uvar()
    return message_type, connection_id, message, serenedb_storage_version
