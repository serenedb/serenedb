import argparse
import hashlib
import struct
from pathlib import Path


PADDING = bytes.fromhex(
    "28bf4e5e4e758a4164004e56fffa0108"
    "2e2e00b6d0683e802f0ca9fe6453697a"
)


def rc4(key, data):
    state = list(range(256))
    j = 0
    for i in range(256):
        j = (j + state[i] + key[i % len(key)]) % 256
        state[i], state[j] = state[j], state[i]
    i = j = 0
    result = bytearray()
    for value in data:
        i = (i + 1) % 256
        j = (j + state[i]) % 256
        state[i], state[j] = state[j], state[i]
        result.append(value ^ state[(state[i] + state[j]) % 256])
    return bytes(result)


def write_pdf(path, pages, password=None, unicode_map=False):
    objects = [b"", b""]
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    document_id = hashlib.md5(b"SereneDB PDF fixtures").digest()
    key = None
    encryption = b""
    if password is not None:
        user = (password.encode() + PADDING)[:32]
        owner = rc4(hashlib.md5((b"owner" + PADDING)[:32]).digest()[:5], user)
        key = hashlib.md5(user + owner + struct.pack("<i", -4) + document_id).digest()[:5]
        user_key = rc4(key, PADDING)
        encryption = (
            b"<< /Filter /Standard /V 1 /R 2 /Length 40 /P -4 /O <"
            + owner.hex().encode()
            + b"> /U <"
            + user_key.hex().encode()
            + b"> >>"
        )

    def stream(content):
        number = len(objects) + 1
        if key is not None:
            object_key = hashlib.md5(key + struct.pack("<I", number)[:3] + b"\0\0").digest()[:10]
            content = rc4(object_key, content)
        objects.append(
            b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n"
            + content + b"\nendstream"
        )
        return number

    if unicode_map:
        mapping = {0x41: "П", 0x42: "中"} if unicode_map is True else unicode_map
        mappings = b"".join(
            f"<{code:02X}> <{text.encode('utf-16-be').hex().upper()}>\n".encode()
            for code, text in mapping.items()
        )
        cmap = stream(
            b"/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n"
            b"/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def\n"
            b"/CMapName /Fixture def\n/CMapType 2 def\n"
            b"1 begincodespacerange\n<00> <FF>\nendcodespacerange\n"
            + f"{len(mapping)} beginbfchar\n".encode() + mappings + b"endbfchar\n"
            b"endcmap\nCMapName currentdict /CMap defineresource pop\nend\nend"
        )
        objects[2] = (
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode "
            + str(cmap).encode() + b" 0 R >>"
        )
    kids = []
    for content in pages:
        content_number = stream(content)
        kids.append(len(objects) + 1)
        objects.append(
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 3 0 R >> >> /Contents "
            + str(content_number).encode() + b" 0 R >>"
        )
    objects[0] = b"<< /Type /Catalog /Pages 2 0 R >>"
    objects[1] = (
        b"<< /Type /Pages /Count " + str(len(kids)).encode() + b" /Kids ["
        + b" ".join(str(kid).encode() + b" 0 R" for kid in kids) + b"] >>"
    )
    trailer = b"/Root 1 0 R"
    if key is not None:
        objects.append(encryption)
        trailer += (
            b" /Encrypt " + str(len(objects)).encode() + b" 0 R /ID [<"
            + document_id.hex().encode() + b"><" + document_id.hex().encode() + b">]"
        )
    data = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data.extend(str(index).encode() + b" 0 obj\n" + obj + b"\nendobj\n")
    xref = len(data)
    data.extend(b"xref\n0 " + str(len(offsets)).encode() + b"\n0000000000 65535 f \n")
    for offset in offsets[1:]:
        data.extend(f"{offset:010d} 00000 n \n".encode())
    data.extend(
        b"trailer\n<< /Size " + str(len(offsets)).encode() + b" " + trailer
        + b" >>\nstartxref\n" + str(xref).encode() + b"\n%%EOF\n"
    )
    path.write_bytes(data)


def generate(directory):
    directory.mkdir(parents=True, exist_ok=True)
    first = b"BT /F1 12 Tf 72 720 Td (Hello PDF) Tj ET"
    second = b"BT /F1 12 Tf 72 720 Td (Second page) Tj ET"
    write_pdf(directory / "text.pdf", [first, second])
    write_pdf(directory / "more_text.pdf", [second])
    write_pdf(directory / "encrypted.pdf", [first], password="secret")
    write_pdf(directory / "blank.pdf", [b""])
    write_pdf(directory / "mixed.pdf", [first, b"", second])
    write_pdf(directory / "image.pdf", [
        b"q 100 0 0 100 72 620 cm BI /W 1 /H 1 /BPC 8 /CS /G ID \x00 EI Q"
    ])
    write_pdf(directory / "columns.pdf", [
        b"BT /F1 12 Tf 320 720 Td (Right column) Tj ET\n"
        b"BT /F1 12 Tf 72 720 Td (Left column) Tj ET"
    ])
    write_pdf(directory / "layout.pdf", [
        b"\n".join(
            f"BT /F1 10 Tf {x} {720 - i * 20} Td ({side}-{i} column reading order) Tj ET".encode()
            for side, x in (("RIGHT", 330), ("LEFT", 48)) for i in range(8)
        ),
        b"BT /F1 12 Tf 72 720 Td (A hyphen-) Tj ET\n"
        b"BT /F1 12 Tf 72 700 Td (ated word) Tj ET\n"
        b"BT /F1 12 Tf 72 680 Td (Last line-) Tj ET",
    ])
    write_pdf(directory / "unicode.pdf", [
        b"BT /F1 12 Tf 72 720 Td <4142> Tj ET"
    ], unicode_map=True)
    write_pdf(directory / "rtl.pdf", [
        b"BT /F1 12 Tf 20 720 Td <45464748> Tj ET "
        b"BT /F1 12 Tf 100 720 Td <41424344> Tj ET"
    ], unicode_map=dict(zip(range(0x41, 0x49), "שלוםעולם")))
    write_pdf(directory / "scaled_words.pdf", [
        b"BT /F1 1 Tf 10 0 0 10 20 700 Tm (12 34) Tj ET"
    ])
    spaced_table = (
        b"0 70 120 40 re S 40 70 m 40 110 l S 80 70 m 80 110 l S 0 90 m 120 90 l S "
        b"BT /F1 1 Tf 4.1107 Tc 5.76 0 0 5.76 20 100 Tm "
        b"[(02) 4125 (,) 4098 (2) 4125 (4) 4125 (81) 4125 (,) 4098 (1) 4125 (3) 4125 (5)] TJ ET "
        b"BT 0 Tc /F1 8 Tf 1 0 0 1 20 80 Tm (A) Tj 40 0 Td (B) Tj 40 0 Td (C) Tj ET"
    )
    write_pdf(directory / "spaced_table.pdf", [spaced_table])
    write_pdf(directory / "many_pages.pdf", [first] * 2050)
    table = []
    for row in range(34):
        y = 750 - row * 20
        table.append(f"50 {y} m 250 {y} l S".encode())
        if row < 33:
            for col, text in enumerate((f"Row {row + 1}", str(row + 1))):
                table.append(f"BT /F1 10 Tf {60 + col * 100} {y - 14} Td ({text}) Tj ET".encode())
    for x in (50, 150, 250):
        table.append(f"{x} 750 m {x} 90 l S".encode())
    write_pdf(directory / "many_tables.pdf", [b"\n".join(table)] * 65)
    write_pdf(directory / "many_blocks.pdf", [b"\n".join(
        f"BT /F1 1 Tf 72 {i} Td (x) Tj ET".encode() for i in range(2050)
    )])
    (directory / "invalid.pdf").write_bytes(b"This is not a PDF.")
    (directory / "empty.pdf").write_bytes(b"")
    (directory / "truncated.pdf").write_bytes(b"%PDF-1.4\n1 0 obj\n<<")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    generate(parser.parse_args().directory)
