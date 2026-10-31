from pathlib import Path
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[3]
INSTALL_FILE = ROOT / "cmake/PdfNotices.cmake"


def test_pdf_notices_in_binary_install(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(ROOT / "resources/licenses/pdf", source / "resources/licenses/pdf")
    licenses = {
        "podofo": ["COPYING.MPL", "NOTICE", "3rdparty/chromium/pdfium/LICENSE",
                   "3rdparty/chromium/numerics/LICENSE"],
        "freetype": ["LICENSE.TXT", "docs/FTL.TXT"],
    }
    for dependency, paths in licenses.items():
        directory = source / "third_party" / dependency
        for path in paths:
            target = directory / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(f"{dependency}/{path}\n")
        for args in (["init", "-q"], ["add", "."],
                     ["-c", "user.name=test", "-c", "user.email=test@example.com",
                      "-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture"]):
            subprocess.run(["git", "-C", str(directory), *args], check=True, timeout=10)
    project = tmp_path / "project"
    project.mkdir()
    (project / "CMakeLists.txt").write_text(
        'cmake_minimum_required(VERSION 3.28)\n'
        'project(serenedb LANGUAGES NONE)\n'
        'set(CMAKE_INSTALL_LIBDIR lib)\n'
        'include(GNUInstallDirs)\n'
        f'set(PROJECT_SOURCE_DIR "{source.as_posix()}")\n'
        f'list(APPEND CMAKE_MODULE_PATH "{ROOT.as_posix()}/cmake")\n'
        'find_package(Git REQUIRED)\n'
        f'include("{INSTALL_FILE.as_posix()}")\n'
    )
    build = tmp_path / "build"
    destination = tmp_path / "install"
    for command in (
        ["cmake", "-S", str(project), "-B", str(build),
         f"-DCMAKE_INSTALL_PREFIX={destination}"],
        ["cmake", "--install", str(build)],
    ):
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr

    notices = destination / "share/doc/serenedb/pdf"
    source_notice = (notices / "NOTICE.md").read_text()
    for dependency in ("podofo", "freetype"):
        revision = subprocess.check_output(
            ["git", "-C", str(source / "third_party" / dependency), "rev-parse", "HEAD"],
            text=True, timeout=10,
        ).strip()
        assert f"https://github.com/serenedb/{dependency}/tree/{revision}" in source_notice
    assert "@PDF_" not in source_notice
    for installed, original in (
        ("podofo/COPYING.MPL", "podofo/COPYING.MPL"),
        ("podofo/NOTICE", "podofo/NOTICE"),
        ("podofo/pdfium/LICENSE", "podofo/3rdparty/chromium/pdfium/LICENSE"),
        ("podofo/numerics/LICENSE", "podofo/3rdparty/chromium/numerics/LICENSE"),
        ("freetype/FTL.TXT", "freetype/docs/FTL.TXT"),
        ("freetype/LICENSE.TXT", "freetype/LICENSE.TXT"),
    ):
        assert (notices / installed).read_bytes() == (source / "third_party" / original).read_bytes()
    assert "Permission is hereby granted" in (notices / "podofo/THIRD_PARTY.txt").read_text()
