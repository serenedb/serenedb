import argparse
from contextlib import closing
import io
from pathlib import Path
import re

import pikepdf
import pypdfium2 as pdfium
import reportlab
from pypdf import PdfReader, PdfWriter
from pypdf.generic import RectangleObject
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import Paragraph, Table, TableStyle


UNICODE_TEXT = "Résumé: café, naïve, façade \u2014 €137.50; Ω"


def canvas(path):
    result = Canvas(str(path), pagesize=(612, 792), pageCompression=1, invariant=1)
    result.setTitle(path.stem.replace("_", " "))
    result.setAuthor("SereneDB PDF test fixtures")
    return result


def paragraph(pdf, text, x, top, width):
    style = ParagraphStyle("body", fontName="FixtureSans", fontSize=11, leading=16)
    body = Paragraph(text, style)
    _, height = body.wrap(width, 600)
    body.drawOn(pdf, x, top - height)


def report(path):
    pdf = canvas(path)
    pdf.setFillColor(colors.HexColor("#153e60"))
    pdf.rect(0, 744, 612, 48, fill=1, stroke=0)
    pdf.setFillColor(colors.white)
    pdf.setFont("FixtureSans", 19)
    pdf.drawString(48, 760, "SereneDB sample invoice")
    pdf.setFillColor(colors.black)
    pdf.setFont("FixtureSans", 16)
    pdf.drawString(48, 700, "Invoice 2026-010")
    paragraph(pdf, "Customer: Example Research Ltd.<br/>"
              "Project: PDF ingestion acceptance test", 48, 670, 510)
    table = Table([
        ["Item", "Quantity", "Unit price", "Amount"],
        ["vedernikov poesh banochku sopley", "4", "25.00", "100.00"],
        ["this is fine", "3", "12.50", "37.50"],
        ["Total EUR", "", "", "137.50"],
    ], colWidths=[234, 72, 108, 102], rowHeights=30)
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "FixtureSans"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e0edf5")),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    table.wrapOn(pdf, 516, 200)
    table.drawOn(pdf, 48, 450)
    paragraph(pdf, "Payment reference: PDF-REAL-001. This invoice is a fictional "
              "test document. No payment is due.", 48, 400, 516)
    pdf.setFont("FixtureSans", 9)
    pdf.drawString(48, 42, "Invoice page 1")
    pdf.showPage()
    pdf.setFont("FixtureSans", 17)
    pdf.drawString(48, 740, "Processing notes")
    paragraph(pdf, "RIGHT COLUMN<br/>Retention: 30 days.<br/>"
              "Status: reviewed.<br/>Reference: RIGHT-204.", 330, 690, 234)
    paragraph(pdf, "LEFT COLUMN<br/>Owner: document team.<br/>"
              "Priority: normal.<br/>Reference: LEFT-103.", 48, 690, 234)
    pdf.showPage()
    pdf.showPage()
    pdf.setFont("FixtureSans", 17)
    pdf.drawString(48, 740, "Appendix: embedded font")
    pdf.setFont("FixtureSans", 12)
    pdf.drawString(48, 690, UNICODE_TEXT)
    paragraph(pdf, "Final marker: END-OF-REPORT. The preceding page is "
              "intentionally blank.", 48, 650, 516)
    pdf.showPage()
    pdf.save()


def geometry(path):
    data = io.BytesIO()
    pdf = Canvas(data, pagesize=(612, 792), pageCompression=1, invariant=1)
    for angle in (0, 90, 180, 270):
        pdf.setFont("FixtureSans", 12)
        pdf.drawString(72, 700, f"Rotation {angle}")
        pdf.showPage()
    pdf.save()
    writer = PdfWriter(clone_from=PdfReader(data))
    for page, angle in zip(writer.pages, (0, 90, 180, 270)):
        page.rotate(angle)
    writer.write(path)


def forms(path):
    pdf = canvas(path)
    pdf.beginForm("stamp", 0, 0, 200, 60)
    pdf.setFont("FixtureSans", 12)
    pdf.drawString(0, 20, "Approved form stamp")
    pdf.endForm()
    for offset in (600, 300):
        pdf.saveState()
        pdf.translate(72, offset)
        pdf.doForm("stamp")
        pdf.restoreState()
    pdf.showPage()
    pdf.save()


def render_page(document, index):
    with closing(document[index]) as page, closing(page.render(scale=96 / 72)) as bitmap:
        return bitmap.to_pil().copy()


def generate(directory, previews):
    directory.mkdir(parents=True, exist_ok=True)
    font = Path(reportlab.__file__).parent / "fonts" / "Vera.ttf"
    pdfmetrics.registerFont(TTFont("FixtureSans", str(font)))
    report(directory / "business_report.pdf")
    geometry(directory / "geometry.pdf")
    forms(directory / "forms.pdf")
    source = directory / "business_report.pdf"
    with pikepdf.open(source) as document:
        document.save(directory / "compressed_report.pdf", compress_streams=True,
                      object_stream_mode=pikepdf.ObjectStreamMode.generate)
    with pdfium.PdfDocument(source) as document:
        pdf = canvas(directory / "scan.pdf")
        pdf.drawImage(ImageReader(render_page(document, 0)), 0, 0, width=612, height=792)
        pdf.showPage()
        pdf.save()
        if previews:
            previews.mkdir(parents=True, exist_ok=True)
            for index in range(len(document)):
                render_page(document, index).save(previews / f"report-{index + 1}.png")
    for algorithm in ("AES-128", "AES-256"):
        writer = PdfWriter(clone_from=source)
        writer.encrypt("reader-secret", "owner-secret", algorithm=algorithm)
        writer.write(directory / f"protected_{algorithm.lower()}.pdf")
    writer = PdfWriter(clone_from=source)
    writer.encrypt("aIX", "owner-secret", algorithm="AES-256")
    writer.write(directory / "protected_unicode.pdf")
    writer = PdfWriter()
    reader = PdfReader(source)
    writer.add_page(reader.pages[0])
    writer.add_page(PdfReader(directory / "scan.pdf").pages[0])
    writer.add_page(reader.pages[3])
    writer.write(directory / "hybrid.pdf")
    writer = PdfWriter()
    page = writer.add_page(PdfReader(directory / "geometry.pdf").pages[0])
    page.mediabox = RectangleObject([36, 36, 648, 828])
    page.cropbox = RectangleObject([48, 48, 600, 780])
    writer.write(directory / "offset_page.pdf")


def generate_info(directory):
    directory.mkdir(parents=True, exist_ok=True)
    document = pikepdf.Pdf.new()
    font = document.make_indirect(pikepdf.Dictionary(
        Type=pikepdf.Name.Font, Subtype=pikepdf.Name.Type1,
        BaseFont=pikepdf.Name.Helvetica,
    ))
    for rotation in (0, 90, 180, 270):
        page = document.add_blank_page(page_size=(612, 792))
        page.CropBox = pikepdf.Array([20, 30, 420, 630])
        page.Rotate = rotation
        page.Resources = pikepdf.Dictionary(Font=pikepdf.Dictionary(F1=font))
        page.Contents = document.make_stream(
            f"BT /F1 12 Tf 50 550 Td (varusik rotation {rotation}) Tj ET".encode()
        )
    document.pages[1].Dur = 2.5
    document.Root.PageLabels = document.make_indirect(pikepdf.Dictionary(Kids=pikepdf.Array([
        document.make_indirect(pikepdf.Dictionary(Nums=pikepdf.Array([
            0, pikepdf.Dictionary(S=pikepdf.Name.r, P=pikepdf.String("varusik-"), St=1),
        ]))),
        document.make_indirect(pikepdf.Dictionary(Nums=pikepdf.Array([
            2, pikepdf.Dictionary(S=pikepdf.Name.A, St=1),
        ]))),
    ])))
    document.docinfo.Title = "varusik geometry"
    document.docinfo.Author = "varusik"
    document.docinfo.CreationDate = "D:20260102153000+03'00'"
    document.docinfo.ModDate = "D:20260203120000Z"

    def metadata(value):
        document.Root.Metadata = document.make_stream(value)
        document.Root.Metadata.Type = pikepdf.Name.Metadata
        document.Root.Metadata.Subtype = pikepdf.Name.XML

    def save(name, **options):
        document.save(directory / name, static_id=True, fix_metadata_version=False, **options)

    prefix = b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:Description xmlns:pdfaid="http://www.aiim.org/pdfa/ns/id/"'
    suffix = b'</rdf:Description></rdf:RDF></x:xmpmeta>'
    metadata(prefix + b'><pdfaid:part>2</pdfaid:part><pdfaid:conformance>B</pdfaid:conformance>' + suffix)
    save("inspect_geometry.pdf")
    save("inspect_linearized.pdf", linearize=True)
    metadata(prefix + b' pdfaid:part="3" pdfaid:conformance="A">' + suffix)
    save("inspect_xmp_attributes.pdf")
    metadata(prefix + b'><pdfaid:part>4</pdfaid:part>' + suffix)
    save("inspect_xmp_partial.pdf")
    metadata(b'<!DOCTYPE x [<!ENTITY forbidden SYSTEM "file:///nonexistent-varusik-secret">]>'
             + prefix + b'><pdfaid:part>2</pdfaid:part><pdfaid:conformance>&forbidden;</pdfaid:conformance>' + suffix)
    save("inspect_xmp_untrusted.pdf")
    metadata(b'<broken><pdfaid:part>2</pdfaid:part>')
    save("inspect_xmp_invalid.pdf")
    metadata(prefix.replace(b'http://www.aiim.org/pdfa/ns/id/', b'urn:varusik:not-pdfa')
             + b'><pdfaid:part>2</pdfaid:part><pdfaid:conformance>B</pdfaid:conformance>' + suffix)
    save("inspect_xmp_namespace.pdf")
    del document.Root.Metadata
    document.docinfo.CreationDate = "varusik is not a timestamp"
    document.docinfo.ModDate = "D:garbage"
    save("inspect_bad_dates.pdf")
    document.Root.PageLabels = document.make_indirect(pikepdf.Dictionary())
    document.Root.PageLabels.Kids = pikepdf.Array([document.Root.PageLabels])
    save("inspect_label_cycle.pdf")


def generate_inventory(directory):
    import pikepdf

    document = pikepdf.Pdf.new()
    page = document.add_blank_page(page_size=(612, 792))
    font = document.make_indirect(pikepdf.Dictionary(
        Type=pikepdf.Name.Font, Subtype=pikepdf.Name.Type1,
        BaseFont=pikepdf.Name.Helvetica,
    ))
    page.Resources = pikepdf.Dictionary(Font=pikepdf.Dictionary(F1=font))
    page.Contents = document.make_stream(b"BT /F1 12 Tf 72 720 Td (varusik inventory) Tj ET")
    page.Annots = pikepdf.Array([
        document.make_indirect(pikepdf.Dictionary(
            Type=pikepdf.Name.Annot, Subtype=pikepdf.Name.Link,
            Rect=pikepdf.Array([10, 20, 110, 40]), Contents="varusik link",
            A=pikepdf.Dictionary(S=pikepdf.Name.URI, URI="https://example.com/varusik"),
        )),
        document.make_indirect(pikepdf.Dictionary(
            Type=pikepdf.Name.Annot, Subtype=pikepdf.Name.Text,
            Rect=pikepdf.Array([50, 60, 70, 80]), Contents="varusik note",
        )),
    ])
    form = document.make_stream(b"BT /F2 10 Tf 0 0 Td (varusik nested) Tj ET")
    form.Type = pikepdf.Name.XObject
    form.Subtype = pikepdf.Name.Form
    form.BBox = pikepdf.Array([0, 0, 100, 30])
    form.Resources = pikepdf.Dictionary(Font=pikepdf.Dictionary(F2=document.make_indirect(
        pikepdf.Dictionary(Type=pikepdf.Name.Font, Subtype=pikepdf.Name.Type1, BaseFont=pikepdf.Name.Courier)
    )))
    page.Resources.XObject = pikepdf.Dictionary(Varusik=form)
    document.Root.OpenAction = pikepdf.Dictionary(S=pikepdf.Name.JavaScript, JS="varusik")
    document.Root.PageMode = pikepdf.Name.UseOutlines
    document.Root.PageLayout = pikepdf.Name.TwoColumnLeft
    document.Root.AcroForm = pikepdf.Dictionary(Fields=pikepdf.Array())
    document.save(directory / "inventory_varusik.pdf", static_id=True)
    document.save(directory / "inventory_encrypted.pdf", static_id=True, encryption=pikepdf.Encryption(
        owner="varusik-owner", user="reader-secret", R=6,
        allow=pikepdf.Permissions(extract=False, modify_annotation=False, modify_assembly=False,
                                 modify_form=False, modify_other=False, print_lowres=False, print_highres=False),
    ))
    page.Annots = pikepdf.Array([page.Annots[0]] * 2050)
    document.save(directory / "inventory_many.pdf", static_id=True)


def validate(directory):
    for path in sorted(directory.glob("*.pdf")):
        password = "aIX" if path.stem == "protected_unicode" else "reader-secret"
        with pdfium.PdfDocument(path, password=password) as document:
            text = []
            rotations = []
            images = []
            for index in range(len(document)):
                with closing(document[index]) as page, closing(page.get_textpage()) as text_page:
                    text.append(text_page.get_text_bounded())
                    rotations.append(page.get_rotation())
                    images.append(any(obj.type == pdfium.raw.FPDF_PAGEOBJ_IMAGE
                                      for obj in page.get_objects()))
            if path.stem in {"business_report", "compressed_report"} or path.stem.startswith("protected_"):
                assert len(text) == 4, path
                assert "Invoice 2026-010" in text[0] and "137.50" in text[0], path
                assert "LEFT-103" in text[1] and "RIGHT-204" in text[1], path
                assert not text[2].strip(), path
                assert UNICODE_TEXT in text[3] and "END-OF-REPORT" in text[3], path
            elif path.stem == "scan":
                assert len(text) == 1 and not text[0].strip(), path
                assert images[0], path
            elif path.stem == "hybrid":
                assert len(text) == 3 and not text[1].strip(), path
                assert "Invoice 2026-010" in text[0] and UNICODE_TEXT in text[2], path
            elif path.stem == "geometry":
                assert len(text) == 4, path
                for page_text, rotation, angle in zip(text, rotations, (0, 90, 180, 270)):
                    assert rotation == angle and f"Rotation {angle}" in page_text, path
            elif path.stem == "forms":
                assert text[0].count("Approved form stamp") == 2, path
            elif path.stem == "offset_page":
                assert "Rotation 0" in text[0], path
            print(f"{path.name}: {len(text)} pages; independent text validation passed")
    assert b"/ObjStm" in (directory / "compressed_report.pdf").read_bytes()
    assert re.search(rb"/Type\s*/XRef\b", (directory / "compressed_report.pdf").read_bytes())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--previews", type=Path)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    if not args.check_only:
        generate(args.directory, args.previews)
        generate_info(args.directory)
        generate_inventory(args.directory)
    validate(args.directory)
