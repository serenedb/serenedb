from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[3]
INSTALL_FILE = ROOT / "cmake/SereneDBInstall.cmake"


def test_pdf_notices_in_binary_install(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "CMakeLists.txt").write_text(
        'cmake_minimum_required(VERSION 3.28)\n'
        'project(serenedb LANGUAGES NONE)\n'
        'set(CMAKE_INSTALL_LIBDIR lib)\n'
        f'set(CMAKE_SOURCE_DIR "{ROOT.as_posix()}")\n'
        f'set(PROJECT_SOURCE_DIR "{ROOT.as_posix()}")\n'
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
            ["git", "-C", str(ROOT / "third_party" / dependency), "rev-parse", "HEAD"],
            text=True, timeout=10,
        ).strip()
        assert f"https://github.com/serenedb/{dependency}/tree/{revision}" in source_notice
    assert "@PDF_" not in source_notice
    for installed, source in (
        ("podofo/COPYING.MPL", "podofo/COPYING.MPL"),
        ("podofo/NOTICE", "podofo/NOTICE"),
        ("podofo/pdfium/LICENSE", "podofo/3rdparty/chromium/pdfium/LICENSE"),
        ("podofo/numerics/LICENSE", "podofo/3rdparty/chromium/numerics/LICENSE"),
        ("freetype/FTL.TXT", "freetype/docs/FTL.TXT"),
        ("freetype/LICENSE.TXT", "freetype/LICENSE.TXT"),
    ):
        assert (notices / installed).read_bytes() == (ROOT / "third_party" / source).read_bytes()
    assert "Permission is hereby granted" in (notices / "podofo/THIRD_PARTY.txt").read_text()
