from __future__ import annotations

import argparse
import mmap
import os
import pathlib
import struct
import subprocess
import sys
from typing import BinaryIO

TAG = b"sdb-docs-index-1"
SECTION = b".sdb_docs"
INDEXES = ("docs", "objects")
ALIGNMENT = 64
CAPACITY_OFFSET = len(TAG)
SIZE_OFFSET = len(TAG) + 8
PAYLOAD_OFFSET = 64
ELF_MAGIC = b"\x7fELF"
MACHO_MAGIC = b"\xcf\xfa\xed\xfe"
ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
PROGRAM_HEADER = struct.Struct("<IIQQQQQQ")
SECTION_HEADER = struct.Struct("<IIQQQQIIQQ")
SHOFF_OFFSET = 40
PT_LOAD = 1
SHF_ALLOC = 2


class EmbedError(Exception):
    pass


def human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KiB"
    return f"{size / (1024 * 1024):.1f} MiB"


def unpack(image: bytes) -> list[list[tuple[str, int]]]:
    indexes = []
    offset = 0
    try:
        for index in INDEXES:
            (count,) = struct.unpack_from("<I", image, offset)
            offset += 4
            files = []
            for _ in range(count):
                (length,) = struct.unpack_from("<I", image, offset)
                offset += 4
                name = image[offset:offset + length].decode()
                offset += length
                (size,) = struct.unpack_from("<Q", image, offset)
                offset += 8 + (-(offset + 8) % ALIGNMENT) + size
                files.append((name, size))
            if not files:
                raise EmbedError(f"the image holds no {index} index files")
            indexes.append(files)
    except struct.error as error:
        raise EmbedError(f"the image is truncated: {error}") from error
    if offset != len(image):
        raise EmbedError(f"the image holds {len(image)} bytes, its files "
                         f"end at {offset}")
    return indexes


def build_image(binary: pathlib.Path, corpus: pathlib.Path) -> bytes:
    read, write = os.pipe()
    with os.fdopen(read, "rb") as pipe:
        process = subprocess.Popen(
            [str(binary), "docs-index", str(corpus), f"/dev/fd/{write}"],
            pass_fds=(write,))
        os.close(write)
        image = pipe.read()
    if process.wait() != 0:
        raise EmbedError(f"{binary} docs-index failed with exit code "
                         f"{process.returncode}")
    return image


def grow_section(file: BinaryIO, image: bytes) -> None:
    with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
        ident, *_, phoff, shoff, _, _, phentsize, phnum, shentsize, shnum, \
            shstrndx = ELF_HEADER.unpack_from(mapped, 0)
        if ident[4:6] != b"\x02\x01":
            raise EmbedError("expected a 64-bit little-endian ELF binary")
        sections = [
            list(SECTION_HEADER.unpack_from(mapped, shoff + i * shentsize))
            for i in range(shnum)
        ]
        programs = [
            list(PROGRAM_HEADER.unpack_from(mapped, phoff + i * phentsize))
            for i in range(phnum)
        ]
        names = sections[shstrndx][4]
        found = [
            section for section in sections
            if mapped[names + section[0]:
                      mapped.find(b"\0", names + section[0])] == SECTION
        ]
        if len(found) != 1:
            raise EmbedError(f"expected exactly one {SECTION.decode()} "
                             f"section")
        docs = found[0]
        address, offset = docs[3], docs[4]
        if mapped[offset:offset + len(TAG)] != TAG:
            raise EmbedError(f"{SECTION.decode()} does not start with the "
                             f"docs index header")
        size = len(mapped)

    segments = [
        program for program in programs
        if program[0] == PT_LOAD and program[2:4] == [offset, address]
        and program[5] == program[6] == docs[5]
    ]
    last = (
        len(segments) == 1
        and all(program is segments[0] or program[5] == 0
                or program[2] + program[5] <= offset for program in programs)
        and all(program is segments[0] or program[0] != PT_LOAD
                or program[3] < address for program in programs)
        and all(section is docs or not section[2] & SHF_ALLOC
                or section[3] < address for section in sections)
    )
    if not last:
        raise EmbedError(f"{SECTION.decode()} must be alone in the last "
                         f"loadable segment; link with "
                         f"server/docs/docs_index.ld")

    content = (TAG + struct.pack("<QQ", len(image), len(image))).ljust(
        PAYLOAD_OFFSET, b"\0") + image
    end = offset + docs[5]
    start = offset + len(content)
    alignment = max([8] + [section[8] for section in sections
                           if section[4] >= end])
    shift = start + (end - start) % alignment - end
    if shift > 0:
        file.truncate(size + shift)
    with mmap.mmap(file.fileno(), 0) as mapped:
        mapped.move(end + shift, end, size - end)
        mapped[offset:start] = content
        mapped[start:end + shift] = bytes(end + shift - start)
        docs[5] = len(content)
        segments[0][5] = segments[0][6] = len(content)
        for section in sections:
            if section is not docs and section[4] >= end:
                section[4] += shift
        if shoff >= end:
            shoff += shift
            struct.pack_into("<Q", mapped, SHOFF_OFFSET, shoff)
        for i, section in enumerate(sections):
            SECTION_HEADER.pack_into(mapped, shoff + i * shentsize, *section)
        for i, program in enumerate(programs):
            PROGRAM_HEADER.pack_into(mapped, phoff + i * phentsize, *program)
    if shift < 0:
        file.truncate(size + shift)


def fill_region(file: BinaryIO, image: bytes) -> None:
    with mmap.mmap(file.fileno(), 0) as mapped:
        at = mapped.find(TAG)
        if at < 0 or mapped.find(TAG, at + 1) >= 0:
            raise EmbedError("expected exactly one docs index region")
        (capacity,) = struct.unpack_from("<Q", mapped, at + CAPACITY_OFFSET)
        if len(image) > capacity:
            raise EmbedError(f"the docs index takes {len(image)} bytes, more "
                             f"than the {capacity}-byte region; raise "
                             f"kCapacity in server/docs/docs_index_image.cpp")
        payload = at + PAYLOAD_OFFSET
        mapped[at + SIZE_OFFSET:at + SIZE_OFFSET + 8] = struct.pack(
            "<Q", len(image))
        mapped[payload:payload + capacity] = image + bytes(
            capacity - len(image))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Write the documentation index into the binary: an ELF "
                    "binary's .sdb_docs section grows to fit it exactly, a "
                    "Mach-O binary's fixed region "
                    "(server/docs/docs_index_image.cpp) is filled in place.")
    parser.add_argument("binary", type=pathlib.Path)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--corpus", type=pathlib.Path,
                        help="build the index from this corpus with the "
                             "binary's docs-index command, in memory")
    source.add_argument("--image", type=pathlib.Path,
                        help="embed this image, as docs-index writes it")
    parser.add_argument("--builder", type=pathlib.Path,
                        help="the serenedb binary that builds the index from "
                             "--corpus (default: the binary itself)")
    args = parser.parse_args()

    try:
        image = (build_image(args.builder or args.binary, args.corpus)
                 if args.corpus else args.image.read_bytes())
        indexes = unpack(image)
        with args.binary.open("r+b") as file:
            magic = file.read(len(ELF_MAGIC))
            if magic == ELF_MAGIC:
                grow_section(file, image)
            elif magic == MACHO_MAGIC:
                fill_region(file, image)
            else:
                raise EmbedError("expected an ELF or a Mach-O binary")
    except (EmbedError, OSError) as error:
        print(f"{args.binary}: {error}", file=sys.stderr)
        return 1

    listed = [
        (f"{name}/{file}", size)
        for name, files in zip(INDEXES, indexes)
        for file, size in files
    ]
    total = sum(size for _, size in listed)
    print(f"embedded docs index: {len(listed)} files, {human_size(total)} "
          f"({total} bytes) -> {args.binary}")
    width = max(len(name) for name, _ in listed)
    for name, size in sorted(listed, key=lambda item: item[1], reverse=True):
        print(f"  {name:<{width}}  {human_size(size):>10}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
