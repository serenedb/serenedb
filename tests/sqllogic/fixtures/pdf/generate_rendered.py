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
        ["Document review", "4", "25.00", "100.00"],
        ["Archive storage", "3", "12.50", "37.50"],
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


def validate(directory):
    for path in sorted(directory.glob("*.pdf")):
        with pdfium.PdfDocument(path, password="reader-secret") as document:
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
    validate(args.directory)
